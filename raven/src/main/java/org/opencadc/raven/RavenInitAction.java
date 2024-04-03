/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
************************************************************************
*/

package org.opencadc.raven;

import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.rest.InitAction;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.RsaSignatureGenerator;
import ca.nrc.cadc.util.StringUtil;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.PreauthKeyPair;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.PreauthKeyPairDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.db.version.InitDatabaseSI;
import org.opencadc.inventory.transfer.StorageSiteAvailabilityCheck;
import org.opencadc.inventory.transfer.StorageSiteRule;
import org.springframework.dao.DataIntegrityViolationException;

/**
 *
 * @author adriand
 */
public class RavenInitAction extends InitAction {
    private static final Logger log = Logger.getLogger(RavenInitAction.class);

    static String KEY_PAIR_NAME = "raven-preauth-keys";
    
    // config keys
    private static final String RAVEN_KEY = "org.opencadc.raven";

    static final String JNDI_QUERY_DATASOURCE = "jdbc/query"; // context.xml
    static final String JNDI_ADMIN_DATASOURCE = "jdbc/inventory"; // context.xml

    static final String SCHEMA_KEY = RAVEN_KEY + ".inventory.schema";
    static final String PREVENT_NOT_FOUND_KEY = RAVEN_KEY + ".consistency.preventNotFound";
    static final String PREAUTH_KEY = RAVEN_KEY + ".keys.preauth";
    
    static final String READ_GRANTS_KEY = RAVEN_KEY + ".readGrantProvider";
    static final String WRITE_GRANTS_KEY = RAVEN_KEY + ".writeGrantProvider";
    static final String RESOLVER_ENTRY = "ca.nrc.cadc.net.StorageResolver";
    
    static final String DEV_AUTH_ONLY_KEY = RAVEN_KEY + ".authenticateOnly";

    // set init initConfig, used by subsequent init methods
    MultiValuedProperties props;
    
    private String jndiPreauthKeys;
    private String siteAvailabilitiesKey;
    private Thread availabilityCheck;

    public RavenInitAction() {
        super();
    }

    @Override
    public void doInit() {
        initConfig();
        initDatabase();
        initKeyPair();
        initQueryDAO();
        initGrantProviders();
        initStorageSiteRules();
        initAvailabilityCheck();
    }

    @Override
    public void doShutdown() {
        try {
            Context ctx = new InitialContext();
            ctx.unbind(jndiPreauthKeys);
        } catch (Exception oops) {
            log.error("unbind failed during destroy", oops);
        }
        
        terminateAvailabilityCheck();
    }
    
    void initConfig() {
        log.info("initConfig: START");
        this.props = getConfig();
        log.info("initConfig: OK");
    }
    
    private void initDatabase() {
        log.info("initDatabase: START");
        try {
            Map<String,Object> daoConfig = getDaoConfig(props, JNDI_ADMIN_DATASOURCE);
            String jndiDataSourceName = (String) daoConfig.get("jndiDataSourceName");
            String database = (String) daoConfig.get("database");
            String schema = (String) daoConfig.get("invSchema");
            DataSource ds = DBUtil.findJNDIDataSource(jndiDataSourceName);
            InitDatabaseSI init = new InitDatabaseSI(ds, database, schema);
            init.doInit();
            log.info("initDatabase: " + jndiDataSourceName + " " + schema + " OK");
        } catch (Exception ex) {
            throw new IllegalStateException("check/init database failed", ex);
        }
    }
    
    private void initKeyPair() {
        String enablePreauthKeys = props.getFirstPropertyValue(PREAUTH_KEY);
        if (enablePreauthKeys == null || !"true".equals(enablePreauthKeys)) {
            log.info("initKeyPair: " + PREAUTH_KEY + " == " + enablePreauthKeys + " - SKIP");
            return;
        }
        
        log.info("initKeyPair: START");
        jndiPreauthKeys = appName + "-" + PreauthKeyPair.class.getName();
        try {
            Map<String,Object> daoConfig = getDaoConfig(props, JNDI_ADMIN_DATASOURCE);
            PreauthKeyPairDAO dao = new PreauthKeyPairDAO();
            dao.setConfig(daoConfig);
            PreauthKeyPair keys = dao.get(KEY_PAIR_NAME);
            if (keys == null) {
                KeyPair kp = RsaSignatureGenerator.getKeyPair(4096);
                keys = new PreauthKeyPair(KEY_PAIR_NAME, kp.getPublic().getEncoded(), kp.getPrivate().getEncoded());
                try {
                    dao.put(keys);
                    log.info("initKeyPair: new keys created - OK");
                    
                } catch (DataIntegrityViolationException oops) {
                    log.warn("persist new " + PreauthKeyPair.class.getSimpleName() + " failed (" + oops + ") -- probably race condition");
                    keys = dao.get(KEY_PAIR_NAME);
                    if (keys != null) {
                        log.info("race condition confirmed: another instance created keys - OK");
                    } else {
                        throw new RuntimeException("check/init " + KEY_PAIR_NAME + " failed", oops);
                    }
                }
            } else {
                log.info("initKeyPair: re-use existing keys - OK");
            }
            Context ctx = new InitialContext();
            try {
                ctx.unbind(jndiPreauthKeys);
            } catch (NamingException ignore) {
                log.debug("unbind previous JNDI key (" + jndiPreauthKeys + ") failed... ignoring");
            }
            ctx.bind(jndiPreauthKeys, keys);
            log.info("initKeyPair: created JNDI key: " + jndiPreauthKeys);
        } catch (Exception ex) {
            throw new RuntimeException("check/init " + KEY_PAIR_NAME + " failed", ex);
        }
    }
    
    void initQueryDAO() {
        log.info("initDAO: START");
        Map<String,Object> dc = getDaoConfig(props, JNDI_QUERY_DATASOURCE);
        ArtifactDAO artifactDAO = new ArtifactDAO();
        artifactDAO.setConfig(dc); // connectivity tested
        log.info("initDAO: OK");
    }
    
    void initGrantProviders() {
        log.info("initGrantProviders: START");
        List<String> readGrants = props.getProperty(RavenInitAction.READ_GRANTS_KEY);
        if (readGrants != null) {
            for (String s : readGrants) {
                try {
                    URI u = new URI(s);
                    log.debug(RavenInitAction.READ_GRANTS_KEY + ": " + u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + RavenInitAction.READ_GRANTS_KEY + "=" + s + " must be a valid URI");
                }
            }
        }

        List<String> writeGrants = props.getProperty(RavenInitAction.WRITE_GRANTS_KEY);
        if (writeGrants != null) {
            for (String s : writeGrants) {
                try {
                    URI u = new URI(s);
                    log.debug(RavenInitAction.WRITE_GRANTS_KEY + ": " + u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + RavenInitAction.WRITE_GRANTS_KEY + "=" + s + " must be a valid URI");
                }
            }
        }
        log.info("initGrantProviders: OK");
    }

    void initStorageSiteRules() {
        log.info("initStorageSiteRules: START");
        getStorageSiteRules(getConfig());
        log.info("initStorageSiteRules: OK");
    }

    void initAvailabilityCheck() {
        StorageSiteDAO storageSiteDAO = new StorageSiteDAO();
        storageSiteDAO.setConfig(getDaoConfig(props, JNDI_QUERY_DATASOURCE));

        this.siteAvailabilitiesKey = appName + "-" + StorageSiteAvailabilityCheck.class.getName();
        terminateAvailabilityCheck();
        this.availabilityCheck = new Thread(new StorageSiteAvailabilityCheck(storageSiteDAO, siteAvailabilitiesKey));
        this.availabilityCheck.setDaemon(true);
        this.availabilityCheck.start();
    }

    private final void terminateAvailabilityCheck() {
        if (this.availabilityCheck != null) {
            try {
                log.info("terminating AvailabilityCheck Thread...");
                this.availabilityCheck.interrupt();
                this.availabilityCheck.join();
                log.info("terminating AvailabilityCheck Thread... [OK]");
            } catch (Throwable t) {
                log.info("failed to terminate AvailabilityCheck thread", t);
            } finally {
                this.availabilityCheck = null;
            }
        }
        try {
            InitialContext initialContext = new InitialContext();
            initialContext.unbind(this.siteAvailabilitiesKey);
        } catch (NamingException e) {
            log.debug(String.format("unable to unbind %s - %s", this.siteAvailabilitiesKey, e.getMessage()));
        }
    }

    /**
     * Read config file and verify that all required entries are present.
     *
     * @return MultiValuedProperties containing the application config
     */
    static MultiValuedProperties getConfig() {
        PropertiesReader r = new PropertiesReader("raven.properties");
        MultiValuedProperties mvp = r.getAllProperties();

        StringBuilder sb = new StringBuilder();
        sb.append("incomplete config: ");
        boolean ok = true;

        // validate required config here
        String schema = mvp.getFirstPropertyValue(RavenInitAction.SCHEMA_KEY);
        sb.append("\n\t").append(RavenInitAction.SCHEMA_KEY).append(": ");
        if (schema == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }

        String preventNotFound = mvp.getFirstPropertyValue(RavenInitAction.PREVENT_NOT_FOUND_KEY);
        sb.append("\n\t").append(RavenInitAction.PREVENT_NOT_FOUND_KEY).append(": ");
        if (preventNotFound == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }
        
        if (!ok) {
            throw new IllegalStateException(sb.toString());
        }

        return mvp;
    }

    static Map<String,Object> getDaoConfig(MultiValuedProperties props, String pool) {
        String cname = props.getFirstPropertyValue(SQLGenerator.class.getName());
        try {
            Map<String,Object> ret = new TreeMap<>();
            Class clz = Class.forName(cname);
            ret.put(SQLGenerator.class.getName(), clz);
            ret.put("jndiDataSourceName", pool);
            ret.put("invSchema", props.getFirstPropertyValue(RavenInitAction.SCHEMA_KEY));
            ret.put("genSchema", props.getFirstPropertyValue(RavenInitAction.SCHEMA_KEY));
            //config.put("database", null);
            return ret;
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("invalid config: failed to load SQLGenerator: " + cname);
        }
    }

    static Map<URI, StorageSiteRule> getStorageSiteRules(MultiValuedProperties props) {
        StringBuilder sb = new StringBuilder();
        Map<URI, StorageSiteRule> prefs = new HashMap<>();

        String propName;
        List<String> putPreferences = props.getProperty("org.opencadc.raven.putPreference");
        for (String putPreference : putPreferences) {

            propName = putPreference + ".resourceID";
            URI resourceID = null;
            List<String> property = props.getProperty(propName);
            if (property.isEmpty()) {
                sb.append(String.format("%s: missing or empty value\n", propName));
            } else if (property.size() > 1) {
                sb.append(String.format("%s: found multiple properties, expected one\n", propName));
            } else if (!StringUtil.hasText(property.get(0))) {
                sb.append(String.format("%s: property has no value\n", propName));
            } else {
                try {
                    resourceID = new URI(property.get(0));
                } catch (URISyntaxException e) {
                    sb.append(String.format("%s: invalid uri\n", propName));
                }
            }

            propName = putPreference + ".namespace";
            property = props.getProperty(propName);
            List<Namespace> namespaces = new ArrayList<>();
            if (property.isEmpty()) {
                sb.append(String.format("%s: missing or empty value\n", propName));
            } else {
                for (String namespace : property) {
                    if (namespace.isEmpty()) {
                        sb.append(String.format("%s: empty value\n", propName));
                    } else if (namespace.matches(".*[\\s].*")) {
                        sb.append(String.format("%s: invalid namespace, whitespace not allowed '%s'\n",
                                                propName, namespace));
                    } else {
                        try {
                            namespaces.add(new Namespace(namespace));
                        } catch (IllegalArgumentException e) {
                            sb.append(String.format("%s: invalid namespace syntax %s %s",
                                                    propName, namespace, e.getMessage()));
                        }
                    }
                }
                if (resourceID != null) {
                    prefs.put(resourceID, new StorageSiteRule(namespaces));
                }
            }
        }
        if (sb.length() > 0) {
            throw new IllegalStateException(String.format("invalid storage site preference rules:\n%s", sb));
        }
        return prefs;
    }
}

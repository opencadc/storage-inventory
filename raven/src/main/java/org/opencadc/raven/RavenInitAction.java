/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2021.                            (c) 2021.
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

import ca.nrc.cadc.rest.InitAction;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vosi.Availability;
import ca.nrc.cadc.vosi.AvailabilityClient;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.log4j.Logger;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageSiteDAO;

/**
 *
 * @author adriand
 */
public class RavenInitAction extends InitAction {
    private static final Logger log = Logger.getLogger(RavenInitAction.class);

    // config keys
    private static final String RAVEN_KEY = "org.opencadc.raven";

    static final String JNDI_DATASOURCE = "jdbc/inventory"; // context.xml
    static final String JNDI_AVAILABILITY_NAME = ".availabilities";

    static final String SCHEMA_KEY = RAVEN_KEY + ".inventory.schema";

    static final String PUBKEYFILE_KEY = RAVEN_KEY + ".publicKeyFile";
    static final String PRIVKEYFILE_KEY = RAVEN_KEY + ".privateKeyFile";
    static final String READ_GRANTS_KEY = RAVEN_KEY + ".readGrantProvider";
    static final String WRITE_GRANTS_KEY = RAVEN_KEY + ".writeGrantProvider";

    static final String DEV_AUTH_ONLY_KEY = RAVEN_KEY + ".authenticateOnly";

    static final int AVAILABILITY_CHECK_TIMEOUT = 30; //secs
    static final int AVAILABILITY_FULL_CHECK_TIMEOUT = 300; //secs

    // set init initConfig, used by subsequent init methods
    MultiValuedProperties props;
    private String siteAvailabilitiesKey;
    private Thread availabilityCheck;

    public RavenInitAction() {
        super();
    }

    @Override
    public void doInit() {
        initConfig();
        initDAO();
        initGrantProviders();
        initKeys();
        initStorageSiteRules();
        initAvailabilityCheck();
    }

    @Override
    public void doShutdown() {
        terminate();
    }
    
    void initConfig() {
        log.info("initConfig: START");
        this.props = getConfig();
        log.info("initConfig: OK");
    }
    
    void initDAO() {
        log.info("initDAO: START");
        Map<String,Object> dc = getDaoConfig(props);
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
    
    void initKeys() {
        log.info("initKeys: START");
        String pubkeyFileName = props.getFirstPropertyValue(RavenInitAction.PUBKEYFILE_KEY);
        String privkeyFileName = props.getFirstPropertyValue(RavenInitAction.PRIVKEYFILE_KEY);
        File publicKeyFile = new File(System.getProperty("user.home") + "/config/" + pubkeyFileName);
        File privateKeyFile = new File(System.getProperty("user.home") + "/config/" + privkeyFileName);
        if (!publicKeyFile.exists() || !privateKeyFile.exists()) {
            throw new IllegalStateException("invalid config: missing public/private key pair files -- " + publicKeyFile + " | " + privateKeyFile);
        }
        log.info("initKeys: OK");
    }

    void initStorageSiteRules() {
        log.info("initStorageSiteRules: START");
        getStorageSiteRules(getConfig());
        log.info("initStorageSiteRules: OK");
    }

    void initAvailabilityCheck() {
        StorageSiteDAO storageSiteDAO = new StorageSiteDAO();
        storageSiteDAO.setConfig(getDaoConfig(props));

        this.siteAvailabilitiesKey = this.appName + RavenInitAction.JNDI_AVAILABILITY_NAME;
        terminate();
        this.availabilityCheck = new Thread(new AvailabilityCheck(storageSiteDAO, this.siteAvailabilitiesKey));
        this.availabilityCheck.setDaemon(true);
        this.availabilityCheck.start();
    }

    private final void terminate() {
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

        String pub = mvp.getFirstPropertyValue(RavenInitAction.PUBKEYFILE_KEY);
        sb.append("\n\t").append(RavenInitAction.PUBKEYFILE_KEY).append(": ");
        if (pub == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }

        String priv = mvp.getFirstPropertyValue(RavenInitAction.PRIVKEYFILE_KEY);
        sb.append("\n\t").append(RavenInitAction.PRIVKEYFILE_KEY).append(": ");
        if (priv == null) {
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

    static Map<String,Object> getDaoConfig(MultiValuedProperties props) {
        String cname = props.getFirstPropertyValue(SQLGenerator.class.getName());
        try {
            Map<String,Object> ret = new TreeMap<>();
            Class clz = Class.forName(cname);
            ret.put(SQLGenerator.class.getName(), clz);
            ret.put("jndiDataSourceName", RavenInitAction.JNDI_DATASOURCE);
            ret.put("schema", props.getFirstPropertyValue(RavenInitAction.SCHEMA_KEY));
            //config.put("database", null);
            return ret;
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("invalid config: failed to load SQLGenerator: " + cname);
        }
    }

    static Map<URI, StorageSiteRule> getStorageSiteRules(MultiValuedProperties props) {
        StringBuilder sb = new StringBuilder();
        Map<URI, StorageSiteRule> prefs = new HashMap<>();

        List<String> putPreferences = props.getProperty("org.opencadc.raven.putPreference");
        for (String putPreference : putPreferences) {

            URI resourceID = null;
            List<String> resourceIDs = props.getProperty(putPreference + ".resourceID");
            if (resourceIDs.isEmpty()) {
                sb.append(String.format("%s.resourceID: MISSING\n", putPreference));
            } else if (resourceIDs.size() > 1) {
                sb.append(String.format("%s.resourceID: MULTIPLE ENTRIES\n", putPreference));
            } else if (!StringUtil.hasText(resourceIDs.get(0))) {
                sb.append(String.format("%s.resourceID: EMPTY VALUE\n", putPreference));
            } else {
                try {
                    resourceID = new URI(resourceIDs.get(0));
                } catch (URISyntaxException e) {
                    sb.append(String.format("%s.resourceID: INVALID URI\n", putPreference));
                }
            }

            List<String> namespaces = props.getProperty(putPreference + ".namespace");
            if (namespaces.isEmpty()) {
                sb.append(String.format("%s.namespace: MISSING\n", putPreference));
            } else {
                for (String namespace : namespaces) {
                    if (!StringUtil.hasText(namespace)) {
                        sb.append(String.format("%s.namespace: EMPTY VALUE\n", putPreference));
                    } else if (namespace.matches(".*[\\s].*")) {
                        sb.append(String.format("%s.namespace: MULTIPLE ENTRIES\n", putPreference));
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

    private static class AvailabilityCheck implements Runnable {
        private final StorageSiteDAO storageSiteDAO;
        private final Map<URI, SiteState> siteStates;
        private final Map<URI, Availability> siteAvailabilities;

        public AvailabilityCheck(StorageSiteDAO storageSiteDAO, String siteAvailabilitiesKey) {
            this.storageSiteDAO = storageSiteDAO;
            this.siteStates = new HashMap<URI, SiteState>();
            this.siteAvailabilities = new HashMap<URI, Availability>();

            try {
                Context initialContext = new InitialContext();
                // check if key already bound, if so unbind
                try {
                    initialContext.unbind(siteAvailabilitiesKey);
                } catch (NamingException ignore) {
                    // ignore
                }
                initialContext.bind(siteAvailabilitiesKey, this.siteAvailabilities);
            } catch (NamingException e) {
                throw new IllegalStateException(String.format("unable to bind %s to initial context: %s",
                                                              siteAvailabilitiesKey, e.getMessage()), e);
            }
        }

        @Override
        public void run() {
            int lastSiteQuerySecs = 0;
            Set<StorageSite> sites = storageSiteDAO.list();
            while (true) {
                if (lastSiteQuerySecs >= AVAILABILITY_FULL_CHECK_TIMEOUT) {
                    sites = storageSiteDAO.list();
                    lastSiteQuerySecs = 0;
                } else {
                    lastSiteQuerySecs += AVAILABILITY_CHECK_TIMEOUT;
                }

                for (StorageSite site: sites) {
                    URI resourceID = site.getResourceID();
                    log.debug("checking site: " + resourceID);
                    SiteState siteState = this.siteStates.get(resourceID);
                    if (siteState == null) {
                        siteState = new SiteState(true, 0);
                    }
                    boolean minDetail = siteState.isMinDetail();
                    Availability availability;
                    try {
                        availability = getAvailability(resourceID, minDetail);
                    } catch (Exception e) {
                        availability = new Availability(false, e.getMessage());
                        log.debug(String.format("availability check failed %s - %s", resourceID, e.getMessage()));
                    }
                    final boolean prev = siteState.available;
                    siteState.available = availability.isAvailable();
                    this.siteStates.put(resourceID, siteState);
                    this.siteAvailabilities.put(resourceID, availability);
                    String message = String.format("availability check %s %s - %s", minDetail ? "MIN" : "FULL",
                                                   resourceID, siteState.available ? "UP" : "DOWN");
                    if (!siteState.available) {
                        log.warn(message);
                    } else if (prev != siteState.available) {
                        log.info(message);
                    } else {
                        log.debug(message);
                    }
                }

                try {
                    log.debug(String.format("sleep availability checks for %d secs", AVAILABILITY_CHECK_TIMEOUT));
                    Thread.sleep(AVAILABILITY_CHECK_TIMEOUT * 1000);
                } catch (InterruptedException e) {
                    throw new IllegalStateException("AvailabilityCheck thread interrupted during sleep");
                }
            }
        }

        private Availability getAvailability(URI resourceID, boolean minDetail) {
            AvailabilityClient client = new AvailabilityClient(resourceID, minDetail);
            return client.getAvailability();
        }

        private class SiteState {

            public boolean available;
            public int lastFullCheckSecs;

            public SiteState(boolean available, int lastFullCheckSecs) {
                this.available = available;
                this.lastFullCheckSecs = lastFullCheckSecs;
            }

            public boolean isMinDetail() {
                log.debug(String.format("isMinDetail() available=%b, lastFullCheckSecs=%d",
                                        available, lastFullCheckSecs));
                if (this.available && this.lastFullCheckSecs < AVAILABILITY_FULL_CHECK_TIMEOUT) {
                    this.lastFullCheckSecs += AVAILABILITY_CHECK_TIMEOUT;
                    return true;
                }
                this.lastFullCheckSecs = 0;
                return false;
            }

        }

    }

}

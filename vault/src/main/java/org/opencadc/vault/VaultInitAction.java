/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

package org.opencadc.vault;

import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.rest.InitAction;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.RsaSignatureGenerator;
import ca.nrc.cadc.uws.server.impl.InitDatabaseUWS;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyPair;
import java.util.ArrayList;
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
import org.opencadc.inventory.db.HarvestStateDAO;
import org.opencadc.inventory.db.PreauthKeyPairDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.db.version.InitDatabaseSI;
import org.opencadc.inventory.transfer.StorageSiteAvailabilityCheck;
import org.opencadc.vault.metadata.DataNodeSizeSync;
import org.opencadc.vospace.db.InitDatabaseVOS;
import org.opencadc.vospace.server.NodePersistence;
import org.springframework.dao.DataIntegrityViolationException;

/**
 *
 * @author pdowler
 */
public class VaultInitAction extends InitAction {

    private static final Logger log = Logger.getLogger(VaultInitAction.class);

    static String KEY_PAIR_NAME = "vault-preauth-keys";
    
    static final String JNDI_VOS_DATASOURCE = "jdbc/nodes"; // context.xml
    static final String JNDI_INV_DATASOURCE = "jdbc/inventory"; // context.xml
    static final String JNDI_INV_ITER_DATASOURCE = "jdbc/inventory-iterator"; // context.xml
    static final String JNDI_UWS_DATASOURCE = "jdbc/uws"; // context.xml

    // config keys
    private static final String VAULT_KEY = "org.opencadc.vault";
    static final String RESOURCE_ID_KEY = VAULT_KEY + ".resourceID";
    static final String PREVENT_NOT_FOUND_KEY = VAULT_KEY + ".consistency.preventNotFound";
    static final String IS_STORAGE_SITE_KEY = VAULT_KEY + ".inventory.isStorageSite";
    static final String INVENTORY_SCHEMA_KEY = VAULT_KEY + ".inventory.schema";
    static final String VOSPACE_SCHEMA_KEY = VAULT_KEY + ".vospace.schema";
    static final String SINGLE_POOL_KEY = VAULT_KEY + ".singlePool";
    static final String ALLOCATION_PARENT = VAULT_KEY + ".allocationParent";
    static final String ROOT_OWNER = VAULT_KEY + ".root.owner";
    static final String STORAGE_NAMESPACE_KEY = VAULT_KEY + ".storage.namespace";
    static final String ENABLE_DATANODE_SIZE_WORKER_KEY = VAULT_KEY + ".enableDataNodeSizeWorker";

    MultiValuedProperties props;
    private URI resourceID;
    private Namespace storageNamespace;
    private Map<String, Object> vosDaoConfig;
    private Map<String, Object> invDaoConfig;

    private String jndiNodePersistence; // store in JNDI for cadc-vos-server lib
    private String jndiPreauthKeys;  // store pubkey in JNDI for download via GetKeyAction

    private String jndiSiteAvailabilities; // store in JNDI to share with ProtocolsGenerator
    private Thread availabilityCheck;

    private String jndiDataNodeSizeSync; // store in JNDI to support availability mode change
    private Thread dataNodeSizeSyncThread;

    public VaultInitAction() {
        super();
    }

    @Override
    public void doInit() {
        initConfig();
        initDatabaseVOS();
        initDatabaseINV();
        initDatabaseUWS();
        initNodePersistence();
        initKeyPair();
        initAvailabilityCheck();
        initBackgroundWorkers();
    }

    @Override
    public void doShutdown() {
        try {
            Context ctx = new InitialContext();
            ctx.unbind(jndiNodePersistence);
        } catch (Exception oops) {
            log.error("unbind failed during destroy", oops);
        }
        
        try {
            Context ctx = new InitialContext();
            ctx.unbind(jndiPreauthKeys);
        } catch (Exception oops) {
            log.error("unbind failed during destroy", oops);
        }
        
        terminateAvailabilityCheck();
        terminateBackgroundWorkers();
    }
    
    /**
     * Read config file and verify that all required entries are present.
     *
     * @return MultiValuedProperties containing the application config
     * @throws IllegalStateException if required config items are missing
     */
    static MultiValuedProperties getConfig() {
        PropertiesReader r = new PropertiesReader("vault.properties");
        MultiValuedProperties mvp = r.getAllProperties();

        StringBuilder sb = new StringBuilder();
        sb.append("incomplete config: ");
        boolean ok = true;

        String rid = mvp.getFirstPropertyValue(RESOURCE_ID_KEY);
        sb.append("\n\t" + RESOURCE_ID_KEY + ": ");
        if (rid == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }
        
        String iss = mvp.getFirstPropertyValue(IS_STORAGE_SITE_KEY);
        sb.append("\n\t" + IS_STORAGE_SITE_KEY + ": ");
        if (iss == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }
        
        String pnf = mvp.getFirstPropertyValue(PREVENT_NOT_FOUND_KEY);
        sb.append("\n\t" + PREVENT_NOT_FOUND_KEY + ": ");
        if (pnf == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }

        String invSchema = mvp.getFirstPropertyValue(INVENTORY_SCHEMA_KEY);
        sb.append("\n\t").append(INVENTORY_SCHEMA_KEY).append(": ");
        if (invSchema == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }

        String vosSchema = mvp.getFirstPropertyValue(VOSPACE_SCHEMA_KEY);
        sb.append("\n\t").append(VOSPACE_SCHEMA_KEY).append(": ");
        if (vosSchema == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }
        
        String sp = mvp.getFirstPropertyValue(SINGLE_POOL_KEY);
        sb.append("\n\t").append(SINGLE_POOL_KEY).append(": ");
        if (sp == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }

        String ns = mvp.getFirstPropertyValue(STORAGE_NAMESPACE_KEY);
        sb.append("\n\t").append(STORAGE_NAMESPACE_KEY).append(": ");
        if (ns == null) {
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

    static List<String> getAllocationParents(MultiValuedProperties props) {
        List<String> ret = new ArrayList<>();
        for (String sap : props.getProperty(ALLOCATION_PARENT)) {
            String ap = sap;
            if (ap.charAt(0) == '/') {
                ap = ap.substring(1);
            }
            if (ap.length() > 0 && ap.charAt(ap.length() - 1) == '/') {
                ap = ap.substring(0, ap.length() - 1);
            }
            if (ap.indexOf('/') >= 0) {
                throw new InvalidConfigException("invalid " + ALLOCATION_PARENT + ": " + sap
                    + " reason: must be a top-level container node name");
            }
            // empty string means root, otherwise child of root
            ret.add(ap);
        }
        return ret;
    }

    static Map<String, Object> getDaoConfig(MultiValuedProperties props) {
        Map<String, Object> ret = new TreeMap<>();
        ret.put(SQLGenerator.class.getName(), SQLGenerator.class); // not configurable right now
        ret.put("jndiDataSourceName", VaultInitAction.JNDI_VOS_DATASOURCE);
        ret.put("invSchema", props.getFirstPropertyValue(INVENTORY_SCHEMA_KEY));
        ret.put("genSchema", props.getFirstPropertyValue(VOSPACE_SCHEMA_KEY)); // for complete init
        ret.put("vosSchema", props.getFirstPropertyValue(VOSPACE_SCHEMA_KEY));
        return ret;
    }
    
    static Map<String, Object> getInvConfig(MultiValuedProperties props) {
        boolean usp = Boolean.parseBoolean(props.getFirstPropertyValue(SINGLE_POOL_KEY));
        if (usp) {
            return getDaoConfig(props);
        }
        Map<String, Object> ret = new TreeMap<>();
        ret.put(SQLGenerator.class.getName(), SQLGenerator.class); // not configurable right now
        ret.put("jndiDataSourceName", JNDI_INV_DATASOURCE);
        ret.put("invSchema", props.getFirstPropertyValue(INVENTORY_SCHEMA_KEY));
        ret.put("genSchema", props.getFirstPropertyValue(INVENTORY_SCHEMA_KEY)); // for complete init
        return ret;
    }
    
    static Map<String, Object> getIteratorConfig(MultiValuedProperties props) {
        Map<String, Object> ret = new TreeMap<>();
        ret.put(SQLGenerator.class.getName(), SQLGenerator.class); // not configurable right now
        ret.put("jndiDataSourceName", JNDI_INV_ITER_DATASOURCE);
        ret.put("invSchema", props.getFirstPropertyValue(INVENTORY_SCHEMA_KEY));
        ret.put("genSchema", props.getFirstPropertyValue(INVENTORY_SCHEMA_KEY)); // for complete init
        return ret;
    }
    
    static Map<String, Object> getKeyPairConfig(MultiValuedProperties props) {
        return getDaoConfig(props);
    }
    
    private void initConfig() {
        log.info("initConfig: START");
        this.props = getConfig();
        String rid = props.getFirstPropertyValue(RESOURCE_ID_KEY);
        String ns = props.getFirstPropertyValue(STORAGE_NAMESPACE_KEY);
        try {
            this.resourceID = new URI(rid);
            this.storageNamespace = new Namespace(ns);
            this.vosDaoConfig = getDaoConfig(props);
            this.invDaoConfig = getInvConfig(props);
            log.info("initConfig: OK");
        } catch (URISyntaxException ex) {
            throw new IllegalStateException("invalid config: " + RESOURCE_ID_KEY + " must be a valid URI");
        }
    }

    private void initDatabaseVOS() {
        try {
            String dsname = (String) vosDaoConfig.get("jndiDataSourceName");
            String schema = (String) vosDaoConfig.get("vosSchema");
            log.info("initDatabase: " + dsname + " " + schema + " START");
            DataSource ds = DBUtil.findJNDIDataSource(dsname);
            InitDatabaseVOS init = new InitDatabaseVOS(ds, null, schema);
            init.doInit();
            log.info("initDatabase: " + dsname + " " + schema + " OK");
        } catch (Exception ex) {
            throw new IllegalStateException("check/init vospace database failed", ex);
        }
    }
    
    private void initDatabaseINV() {
        try {
            String dsname = (String) invDaoConfig.get("jndiDataSourceName");
            String schema = (String) invDaoConfig.get("invSchema");
            log.info("initDatabase: " + dsname + " " + schema + " START");
            DataSource ds = DBUtil.findJNDIDataSource(dsname);
            InitDatabaseSI init = new InitDatabaseSI(ds, null, schema);
            init.doInit();
            log.info("initDatabase: " + dsname + " " + schema + " OK");
        } catch (Exception ex) {
            throw new IllegalStateException("check/init inventory database failed", ex);
        }
    }

    private void initDatabaseUWS() {
        try {
            log.info("initDatabase: " + JNDI_UWS_DATASOURCE + " uws START");
            DataSource uws = DBUtil.findJNDIDataSource(JNDI_UWS_DATASOURCE);
            InitDatabaseUWS uwsi = new InitDatabaseUWS(uws, null, "uws");
            uwsi.doInit();
            log.info("initDatabase: " + JNDI_UWS_DATASOURCE + " uws OK");
        } catch (Exception ex) {
            throw new RuntimeException("check/init uws database failed", ex);
        }
    }

    private void initNodePersistence() {
        log.info("initNodePersistence: START");
        jndiNodePersistence = appName + "-" + NodePersistence.class.getName();
        try {
            Context ctx = new InitialContext();
            try {
                ctx.unbind(jndiNodePersistence);
            } catch (NamingException ignore) {
                log.debug("unbind previous JNDI key (" + jndiNodePersistence + ") failed... ignoring");
            }
            NodePersistence npi = new NodePersistenceImpl(resourceID, appName);
            ctx.bind(jndiNodePersistence, npi);

            log.info("initNodePersistence: created JNDI key: " + jndiNodePersistence + " impl: " + npi.getClass().getName());
        } catch (Exception ex) {
            log.error("Failed to create JNDI Key " + jndiNodePersistence, ex);
        }
    }
    
    private void initKeyPair() {
        log.info("initKeyPair: START");
        jndiPreauthKeys = appName + "-" + PreauthKeyPair.class.getName();
        try {
            PreauthKeyPairDAO dao = new PreauthKeyPairDAO();
            dao.setConfig(getKeyPairConfig(props));
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
            
            Object o = ctx.lookup(jndiPreauthKeys);
            log.info("checking... found: " + jndiPreauthKeys + " = " + o + " in " + ctx);
        } catch (Exception ex) {
            throw new RuntimeException("check/init " + KEY_PAIR_NAME + " failed", ex);
        }
    }
    
    private void initAvailabilityCheck() {
        StorageSiteDAO storageSiteDAO = new StorageSiteDAO();
        storageSiteDAO.setConfig(getInvConfig(props));

        this.jndiSiteAvailabilities = appName + "-" + StorageSiteAvailabilityCheck.class.getName();
        terminateAvailabilityCheck();
        this.availabilityCheck = new Thread(new StorageSiteAvailabilityCheck(storageSiteDAO, this.jndiSiteAvailabilities));
        this.availabilityCheck.setDaemon(true);
        this.availabilityCheck.start();
    }

    private void terminateAvailabilityCheck() {
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
        
        // ugh: bind() is inside StorageSiteAvailabilityCheck but unbind() is here
        try {
            InitialContext initialContext = new InitialContext();
            initialContext.unbind(this.jndiSiteAvailabilities);
        } catch (NamingException e) {
            log.debug(String.format("unable to unbind %s - %s", this.jndiSiteAvailabilities, e.getMessage()));
        }
    }
    
    private void initBackgroundWorkers() {
        String str = props.getFirstPropertyValue(ENABLE_DATANODE_SIZE_WORKER_KEY);
        if (str != null) {
            boolean enableDataNodeSizeWorker = Boolean.valueOf(str);
            if (!enableDataNodeSizeWorker) {
                log.info("initBackgroundWorkers: DataNodeSizeWorker disabled");
                return;
            }
        }
        try {
            HarvestStateDAO hsDAO = new HarvestStateDAO();
            hsDAO.setConfig(vosDaoConfig);

            ArtifactDAO artifactDAO = new ArtifactDAO();
            Map<String,Object> iterprops = getIteratorConfig(props);
            log.debug("iterator pool: " + iterprops.get("jndiDataSourceName"));
            artifactDAO.setConfig(iterprops);
            
            // determine startup mode
            boolean offline = false; // normal
            String key = appName + RestAction.STATE_MODE_KEY;
            String ret = System.getProperty(key);
            if (ret != null 
                    && (RestAction.STATE_READ_ONLY.equals(ret) || RestAction.STATE_OFFLINE.equals(ret))) {
                offline = true;
            }

            boolean isStorageSite = Boolean.parseBoolean(props.getFirstPropertyValue(IS_STORAGE_SITE_KEY));
            
            terminateBackgroundWorkers();
            DataNodeSizeSync async = new DataNodeSizeSync(hsDAO, artifactDAO, storageNamespace, isStorageSite);
            async.setOffline(offline);
            this.dataNodeSizeSyncThread = new Thread(async);
            dataNodeSizeSyncThread.setDaemon(true);
            dataNodeSizeSyncThread.start();

            // store in JNDI so availability can set offline
            this.jndiDataNodeSizeSync = appName + "-" + DataNodeSizeSync.class.getName();
            InitialContext ctx = new InitialContext();
            try {
                ctx.unbind(jndiDataNodeSizeSync);
            } catch (NamingException ignore) {
                log.debug("unbind previous JNDI key (" + jndiPreauthKeys + ") failed... ignoring");
            }
            ctx.bind(jndiDataNodeSizeSync, async);
            log.info("initBackgroundWorkers: created JNDI key: " + jndiDataNodeSizeSync);
        } catch (Exception ex) {
            throw new RuntimeException("check/init ArtifactSync failed", ex);
        }
    }
    
    private void terminateBackgroundWorkers() {
        if (this.dataNodeSizeSyncThread != null) {
            try {
                log.info("terminating " + DataNodeSizeSync.class.getSimpleName()  + " Thread...");
                this.dataNodeSizeSyncThread.interrupt();
                this.dataNodeSizeSyncThread.join();
                log.info("terminating " + DataNodeSizeSync.class.getSimpleName()  + " Thread... [OK]");
            } catch (Throwable t) {
                log.info("failed to terminate " + DataNodeSizeSync.class.getSimpleName()  + " thread", t);
            } finally {
                this.dataNodeSizeSyncThread = null;
            }
            
            try {
                InitialContext initialContext = new InitialContext();
                initialContext.unbind(this.jndiDataNodeSizeSync);
            } catch (NamingException e) {
                log.debug(String.format("unable to unbind %s - %s", this.jndiDataNodeSizeSync, e.getMessage()));
            }
        }
    }
}

/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020.                            (c) 2020.
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

package org.opencadc.minoc;

import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.rest.InitAction;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.db.version.InitDatabase;
import org.opencadc.inventory.storage.StorageAdapter;

/**
 *
 * @author pdowler
 */
public class MinocInitAction extends InitAction {
    private static final Logger log = Logger.getLogger(MinocInitAction.class);
    
    static final String JNDI_DATASOURCE = "jdbc/inventory"; // context.xml
    
    // config keys
    private static final String MINOC_KEY = "org.opencadc.minoc";
    static final String RESOURCE_ID_KEY = MINOC_KEY + ".resourceID";
    
    static final String SQLGEN_KEY = SQLGenerator.class.getName();
    
    static final String SCHEMA_KEY = MINOC_KEY + ".inventory.schema";
    
    static final String SA_KEY = StorageAdapter.class.getName();
    
    static final String PUBKEYFILE_KEY = MINOC_KEY + ".publicKeyFile";
    static final String READ_GRANTS_KEY = MINOC_KEY + ".readGrantProvider";
    static final String WRITE_GRANTS_KEY = MINOC_KEY + ".writeGrantProvider";
    
    static final String DEV_AUTH_ONLY_KEY = MINOC_KEY + ".authenticateOnly";
    
    // set init initConfig, used by subsequent init methods
    
    MultiValuedProperties props;
    private URI resourceID;
    private Map<String,Object> daoConfig;

    public MinocInitAction() { 
        super();
    }

    @Override
    public void doInit() {
        initConfig();
        initDatabase();
        initStorageSite();
        initStorageAdapter();
    }
    
    /**
     * Read config file and verify that all required entries are present.
     * 
     * @return MultiValuedProperties containing the application config
     * @throws IllegalStateException if required config items are missing
     */
    static MultiValuedProperties getConfig() {
        PropertiesReader r = new PropertiesReader("minoc.properties");
        MultiValuedProperties mvp = r.getAllProperties();

        StringBuilder sb = new StringBuilder();
        sb.append("incomplete config: ");
        boolean ok = true;

        String rid = mvp.getFirstPropertyValue(RESOURCE_ID_KEY);
        sb.append("\n\t" + MinocInitAction.RESOURCE_ID_KEY + ": ");
        if (rid == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }

        String sac = mvp.getFirstPropertyValue(SA_KEY);
        sb.append("\n\t").append(MinocInitAction.SA_KEY).append(": ");
        if (sac == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }

        String sqlgen = mvp.getFirstPropertyValue(SQLGEN_KEY);
        Class sqlGenClass = null;
        sb.append("\n\t").append(MinocInitAction.SQLGEN_KEY).append(": ");
        if (sqlgen == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            try {
                sqlGenClass = Class.forName(sqlgen);
                sb.append("OK");
            } catch (ClassNotFoundException ex) {
                sb.append("class not found: " + sqlgen);
                ok = false;
            }
        }

        String schema = mvp.getFirstPropertyValue(SCHEMA_KEY);
        sb.append("\n\t").append(MinocInitAction.SCHEMA_KEY).append(": ");
        if (schema == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }
        
        String pubkeyFileName = mvp.getFirstPropertyValue(PUBKEYFILE_KEY);
        sb.append("\n\t").append(MinocInitAction.PUBKEYFILE_KEY).append(": ");
        if (pubkeyFileName == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            File pk = new File(System.getProperty("user.home") + "/config/" + pubkeyFileName);
            if (!pk.exists()) {
                sb.append(" NOT FOUND " + pk.getAbsolutePath());
                ok = false;
            } else {
                sb.append("OK");
            }
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
            ret.put("jndiDataSourceName", MinocInitAction.JNDI_DATASOURCE);
            ret.put("schema", props.getFirstPropertyValue(MinocInitAction.SCHEMA_KEY));
            //config.put("database", null);
            return ret;
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("invalid config: failed to load SQLGenerator: " + cname);
        }
    }
    
    private void initConfig() {
        log.info("initConfig: START");
        this.props = getConfig();
        String rid = props.getFirstPropertyValue(RESOURCE_ID_KEY);
        
        try {
            this.resourceID = new URI(rid);
            this.daoConfig = getDaoConfig(props);
            log.info("initConfig: OK");
        } catch (URISyntaxException ex) {
            throw new IllegalStateException("invalid config: " + RESOURCE_ID_KEY + " must be a valid URI");
        }
    }
    
    private void initDatabase() {
        log.info("initDatabase: START");
        try {
            DataSource ds = DBUtil.findJNDIDataSource(JNDI_DATASOURCE);
            String database = (String) daoConfig.get("database");
            String schema = (String) daoConfig.get("schema");
            InitDatabase init = new InitDatabase(ds, database, schema);
            init.doInit();
            log.info("initDatabase: " + JNDI_DATASOURCE + " " + schema + " OK");
        } catch (Exception ex) {
            throw new IllegalStateException("check/init database failed", ex);
        }
    }
    
    private void initStorageSite() {
        log.info("initStorageSite: START");
        StorageSiteDAO ssdao = new StorageSiteDAO();
        ssdao.setConfig(daoConfig);
        
        Set<StorageSite> curlist = ssdao.list();
        if (curlist.size() > 1) {
            throw new IllegalStateException("found: " + curlist.size() + " StorageSite(s) in database; expected 0 or 1");
        }
        // TODO: get display name from config
        // use path from resourceID as default
        String name = resourceID.getPath();
        if (name.charAt(0) == '/') {
            name = name.substring(1);
        }

        if (curlist.isEmpty()) {
            boolean allowRead = !props.getProperty(READ_GRANTS_KEY).isEmpty();
            boolean allowWrite = !props.getProperty(WRITE_GRANTS_KEY).isEmpty();
            StorageSite self = new StorageSite(resourceID, name, allowRead, allowWrite);
            ssdao.put(self);
        } else if (curlist.size() == 1) {
            StorageSite cur = curlist.iterator().next();
            boolean allowRead = !props.getProperty(READ_GRANTS_KEY).isEmpty();
            boolean allowWrite = !props.getProperty(WRITE_GRANTS_KEY).isEmpty();
            cur.setResourceID(resourceID);
            cur.setName(name);
            cur.setAllowRead(allowRead);
            cur.setAllowWrite(allowWrite);
            ssdao.put(cur);
        } else {
            throw new IllegalStateException("BUG: found " + curlist.size() + " StorageSite entries");
        }
        log.info("initStorageSite: " + resourceID + " OK");
    }
    
    private void initStorageAdapter() {
        log.info("initStorageAdapter: START");
        StorageAdapter storageAdapter = InventoryUtil.loadPlugin(props.getFirstPropertyValue(MinocInitAction.SA_KEY));
        log.info("initStorageAdapter: " + storageAdapter.getClass().getName() + " OK");
    }
}

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

package org.opencadc.minoc;

import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.rest.InitAction;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.db.version.InitDatabaseSI;
import org.opencadc.inventory.storage.StorageAdapter;

/**
 *
 * @author pdowler
 */
public class MinocInitAction extends InitAction {
    private static final Logger log = Logger.getLogger(MinocInitAction.class);
    
    // set init initConfig, used by subsequent init methods
    private MinocConfig config;
    private String jndiConfigKey;
    private URI resourceID;

    public MinocInitAction() { 
        super();
    }

    @Override
    public void doInit() {
        try {
            initConfig();
            initDatabase();
            initStorageSite();
            initStorageAdapter();
        } catch (Throwable ex) {
            log.error("init fail", ex);
        }
    }

    @Override
    public void doShutdown() {
        super.doShutdown();
        try {
            Context ctx = new InitialContext();
            ctx.unbind(jndiConfigKey);
        } catch (NamingException ex) {
            log.debug("failed to remove config from JNDI", ex);
        }
    }
    
    // get config from JNDI
    static MinocConfig getConfig(String appName) {
        String key = appName + "-" + MinocConfig.class.getName();
        try {
            Context ctx = new InitialContext();
            MinocConfig ret = (MinocConfig) ctx.lookup(key);
            return ret;
        } catch (NamingException ex) {
            throw new RuntimeException("BUG: failed to get config from JNDI", ex);
        }
    }
    
    private void initConfig() {
        log.info("initConfig: START");
        this.config = new MinocConfig();
        MultiValuedProperties mvp = config.getProperties();
        String rid = mvp.getFirstPropertyValue(MinocConfig.RESOURCE_ID_KEY);
        jndiConfigKey = appName + "-" + MinocConfig.class.getName();
        try {
            Context ctx = new InitialContext();
            try {
                ctx.unbind(jndiConfigKey);
            } catch (NamingException ignore) {
                log.debug("unbind previous JNDI key (" + jndiConfigKey + ") failed... ignoring");
            }
            ctx.bind(jndiConfigKey, config);

            log.info("created JNDI key: " + jndiConfigKey + " object: " + config.getClass().getName());
        } catch (Exception ex) {
            log.error("Failed to create JNDI Key " + jndiConfigKey, ex);
        }
        
        try {
            this.resourceID = new URI(rid);
            log.info("initConfig: OK");
        } catch (URISyntaxException ex) {
            throw new IllegalStateException("invalid config: " + MinocConfig.RESOURCE_ID_KEY + " must be a valid URI");
        }
    }
    
    private void initDatabase() {
        log.info("initDatabase: START");
        try {
            Map<String,Object> daoConfig = config.getDaoConfig();
            DataSource ds = DBUtil.findJNDIDataSource(MinocConfig.JNDI_DATASOURCE);
            String database = (String) daoConfig.get("database");
            String schema = (String) daoConfig.get("invSchema");
            InitDatabaseSI init = new InitDatabaseSI(ds, database, schema);
            init.doInit();
            log.info("initDatabase: " + MinocConfig.JNDI_DATASOURCE + " " + schema + " OK");
        } catch (Exception ex) {
            throw new IllegalStateException("check/init database failed", ex);
        }
    }
    
    private void initStorageSite() {
        log.info("initStorageSite: START");
        Map<String,Object> daoConfig = config.getDaoConfig();
        StorageSiteDAO ssdao = new StorageSiteDAO();
        ssdao.setConfig(daoConfig);
        
        Set<StorageSite> curlist = ssdao.list();

        // TODO: get display name from config
        // use path from resourceID as default
        String name = resourceID.getPath();
        if (name.charAt(0) == '/') {
            name = name.substring(1);
        }

        boolean allowRead = config.isReadable();
        boolean allowWrite = config.isWritable();
            
        StorageSite self = null;
        if (curlist.isEmpty()) {
            self = new StorageSite(resourceID, name, allowRead, allowWrite);
            ssdao.put(self);
        } else if (curlist.size() == 1) {
            self = curlist.iterator().next();
            self.setResourceID(resourceID);
            self.setName(name);
            self.setAllowRead(allowRead);
            self.setAllowWrite(allowWrite);
        } else {
            throw new IllegalStateException("BUG: found " + curlist.size() + " StorageSite entries; expected 0 or 1");
        }
        
        ssdao.put(self);
        log.info("initStorageSite: " + self + " OK");
    }
    
    private void initStorageAdapter() {
        log.info("initStorageAdapter: START");
        StorageAdapter sa = config.getStorageAdapter();
        log.info("initStorageAdapter: " + sa.getClass().getName() + " OK");
    }
}

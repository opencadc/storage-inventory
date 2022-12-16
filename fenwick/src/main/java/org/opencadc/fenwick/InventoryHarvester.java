/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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

package org.opencadc.fenwick;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfigException;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.client.RegistryClient;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.db.version.InitDatabase;
import org.opencadc.inventory.util.ArtifactSelector;

/**
 * @author pdowler
 */
public class InventoryHarvester implements Runnable {
    private static final Logger log = Logger.getLogger(InventoryHarvester.class);
    
    private static final int WAIT_FOR_SITE_SLEEP = 6; // seconds
    private static final int MONITOR_SLEEP = 120;     // seconds
    private static final int SYNC_SLEEP = 30;         // seconds
    
    private final StorageSiteDAO storageSiteDAO;
    private final ArtifactDAO artifactDAO;
    private final ArtifactDAO sleArtifactDAO;
    private final ArtifactDAO daeArtifactDAO;
    private final ArtifactDAO dsleArtifactDAO;
    
    private final URI resourceID;
    private final ArtifactSelector selector;
    private final boolean trackSiteLocations;
    private final int maxRetryInterval;
    
    /**
     * Constructor.
     *
     * @param daoConfig          config map to pass to cadc-inventory-db DAO classes
     * @param connectionConfig                 database connection info for creating DataSource(s)
     * @param resourceID         identifier for the remote query service
     * @param selector           selector implementation
     * @param trackSiteLocations Whether to track the remote storage site and add it to the Artifact being processed.
     * @param maxRetryInterval   max interval in seconds to sleep after an error during processing.
     */
    public InventoryHarvester(Map<String, Object> daoConfig, ConnectionConfig connectionConfig,
            URI resourceID, ArtifactSelector selector, boolean trackSiteLocations, int maxRetryInterval) {
        InventoryUtil.assertNotNull(InventoryHarvester.class, "daoConfig", daoConfig);
        InventoryUtil.assertNotNull(InventoryHarvester.class, "connectionConfig", connectionConfig);
        InventoryUtil.assertNotNull(InventoryHarvester.class, "resourceID", resourceID);
        InventoryUtil.assertNotNull(InventoryHarvester.class, "selector", selector);
        this.resourceID = resourceID;
        this.selector = selector;
        this.trackSiteLocations = trackSiteLocations;
        this.maxRetryInterval = maxRetryInterval;
        
        try {
            RegistryClient rc = new RegistryClient();
            URL capURL = rc.getAccessURL(resourceID);
            if (capURL == null) {
                throw new IllegalArgumentException("invalid config: query service not found: " + resourceID);
            }
        } catch (ResourceNotFoundException ex) {
            throw new IllegalArgumentException("invalid config: query service not found: " + resourceID);
        } catch (IOException ex) {
            throw new IllegalArgumentException("invalid config", ex);
        }
        
        try {
            // connection 1: storage-site + artifact-sync
            Map<String,Object> dconf = new TreeMap<>();
            String dsname = "jdbc/inventory";
            DBUtil.createJNDIDataSource(dsname, connectionConfig);
            dconf.putAll(daoConfig);
            dconf.put("jndiDataSourceName", dsname);
            this.storageSiteDAO = new StorageSiteDAO(false);
            storageSiteDAO.setConfig(dconf);
            this.artifactDAO = new ArtifactDAO(storageSiteDAO);
            
            String database = (String) dconf.get("database");
            String schema = (String) dconf.get("schema");
            DataSource ds = DBUtil.findJNDIDataSource(dsname);
            InitDatabase init = new InitDatabase(ds, database, schema);
            init.doInit();
            log.info("initDatabase: " + schema + " OK");
            
            // connection 2: StorageLocationEventSync
            dconf = new TreeMap<>();
            dsname = "jdbc/StorageLocationEventSync";
            DBUtil.createJNDIDataSource(dsname, connectionConfig);
            dconf.putAll(daoConfig);
            dconf.put("jndiDataSourceName", dsname);
            this.sleArtifactDAO = new ArtifactDAO(false);
            sleArtifactDAO.setConfig(dconf);
            
            // connection 2: DeletedArtifactEventSync
            dconf = new TreeMap<>();
            dsname = "jdbc/DeletedArtifactEventSync";
            DBUtil.createJNDIDataSource(dsname, connectionConfig);
            dconf.putAll(daoConfig);
            dconf.put("jndiDataSourceName", dsname);
            this.daeArtifactDAO = new ArtifactDAO(false);
            daeArtifactDAO.setConfig(dconf);
            
            // connection 3: DeletedStorageLocationEventSync
            dconf = new TreeMap<>();
            dsname = "jdbc/DeletedStorageLocationEventSync";
            DBUtil.createJNDIDataSource(dsname, connectionConfig);
            dconf.putAll(daoConfig);
            dconf.put("jndiDataSourceName", dsname);
            this.dsleArtifactDAO = new ArtifactDAO(false);
            dsleArtifactDAO.setConfig(dconf);
        } catch (DBConfigException | NamingException ex) {
            throw new IllegalStateException("Unable to access database: " + connectionConfig.getURL(), ex);
        }
    }

    @Override
    public void run() {
        List<Thread> threads = new ArrayList<>();
        List<AbstractSync> tasks = new ArrayList<>();
        try {
            
            StorageSite storageSite = null;
            if (trackSiteLocations) {
                StorageSiteSync siteSync = new StorageSiteSync(storageSiteDAO, resourceID, 4 * SYNC_SLEEP, maxRetryInterval);
                tasks.add(siteSync);
                threads.add(createThread("site-thread", siteSync));
                while (storageSite == null) {
                    log.info("waiting for StorageSiteSync...");
                    Thread.sleep(WAIT_FOR_SITE_SLEEP * 1000L);
                    storageSite = siteSync.getCurrentStorageSite();
                }
            
                AbstractSync r0 = new DeletedStorageLocationEventSync(dsleArtifactDAO, resourceID, SYNC_SLEEP, maxRetryInterval, storageSite);
                tasks.add(r0);
                threads.add(createThread("dsle-thread", r0));
                
                AbstractSync r3 = new StorageLocationEventSync(sleArtifactDAO, resourceID, SYNC_SLEEP, maxRetryInterval, storageSite);
                tasks.add(r3);
                threads.add(createThread("sle-thread", r3));
            }
            
            AbstractSync r1 = new DeletedArtifactEventSync(daeArtifactDAO, resourceID, trackSiteLocations, SYNC_SLEEP, maxRetryInterval);
            tasks.add(r1);
            threads.add(createThread("dae-thread", r1));
            
            AbstractSync r2 = new ArtifactSync(artifactDAO, resourceID, SYNC_SLEEP, maxRetryInterval, selector, storageSite);
            tasks.add(r2);
            threads.add(createThread("artifact-thread", r2));

            while (true) {
                Thread.sleep(MONITOR_SLEEP * 1000L);
                int alive = 0;
                for (Thread t : threads) {
                    if (t.isAlive()) {
                        alive++;
                        log.debug("InventoryHarvester.Thread name=" + t.getName() + " alive=true");
                    } else {
                        log.warn("InventoryHarvester.Thread name=" + t.getName() + " alive=false");
                    }
                }
                Exception fail = null;
                int ok = 0;
                for (AbstractSync s : tasks) {
                    if (s.getFail() != null) {
                        fail = s.getFail();
                        log.error("InventoryHarvester.status name=" + s.getClass().getSimpleName() + " status=FAILED", s.getFail());
                    } else {
                        log.debug("InventoryHarvester.status name=" + s.getClass().getSimpleName() + " status=OK");
                        ok++;
                    }
                }
                log.info("InventoryHarvester.status tasks=" + threads.size() + " alive=" + alive + " ok=" + ok);
                if (fail != null) {
                    throw new RuntimeException("fatal error - terminating", fail);
                }
                if (alive != threads.size()) {
                    throw new RuntimeException("fatal error - terminating because one or more threads died");
                }
            }
        } catch (InterruptedException ex) {
            log.warn("interrupted", ex);
        } finally {
            for (Thread t : threads) {
                if (t.isAlive()) {
                    t.interrupt();
                }
            }
        }
    }
    
    private Thread createThread(String name, Runnable r) {
        Thread ret = new Thread(r);
        ret.setName(name);
        ret.setDaemon(true);
        ret.start();
        return ret;
    }
}

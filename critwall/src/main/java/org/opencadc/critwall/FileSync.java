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

package org.opencadc.critwall;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;

import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.vosi.AvailabilityClient;
import ca.nrc.cadc.vosi.AvailabilityStatus;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import java.util.concurrent.LinkedBlockingQueue;
import javax.naming.NamingException;
import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.util.BucketSelector;


public class FileSync implements Runnable {
    private static final Logger log = Logger.getLogger(FileSync.class);

    private static final int MAX_THREADS = 16;

    private final ArtifactDAO artifactDAO;
    private final ArtifactDAO jobArtifactDAO;
    private final URI locatorService;
    private final BucketSelector selector;
    private final int nthreads;
    private final StorageAdapter storageAdapter;
    private final ThreadPool threadPool;
    private final LinkedBlockingQueue<Runnable> jobQueue;
    
    // test usage only
    boolean testRunOnce = false;

    /**
     * Constructor.
     *
     * @param daoConfig        config map to pass to cadc-inventory-db DAO classes
     * @param connectionConfig ConnectionConfig object to use for creating jndiDataSource
     * @param localStorage     adapter to put to local storage
     * @param locatorServiceID identifier for the remote query service (locator)
     * @param selector         selector implementation
     * @param nthreads         number of threads in download thread pool
     */
    public FileSync(Map<String, Object> daoConfig, ConnectionConfig connectionConfig, StorageAdapter
        localStorage, URI locatorServiceID, BucketSelector selector, int nthreads) {

        InventoryUtil.assertNotNull(FileSync.class, "daoConfig", daoConfig);
        InventoryUtil.assertNotNull(FileSync.class, "connectionConfig", connectionConfig);
        InventoryUtil.assertNotNull(FileSync.class, "localStorage", localStorage);
        InventoryUtil.assertNotNull(FileSync.class, "locatorServiceID", locatorServiceID);
        InventoryUtil.assertNotNull(FileSync.class, "selector", selector);

        if (nthreads <= 0 || nthreads > MAX_THREADS) {
            throw new IllegalArgumentException("invalid config: nthreads must be in [1," + MAX_THREADS + "], found: " + nthreads);
        }

        this.locatorService = locatorServiceID;
        this.selector = selector;
        this.nthreads = nthreads;

        // For managing the artifact iterator FileSync loops over
        try {
            // Make FileSync ArtifactDAO instance
            String jndiSourceName = "jdbc/fileSync";
            daoConfig.put("jndiDataSourceName", jndiSourceName);
            DBUtil.createJNDIDataSource(jndiSourceName, connectionConfig);

            this.artifactDAO = new ArtifactDAO();
            this.artifactDAO.setConfig(daoConfig);

            // Make FileSyncJob ArtifactDAO instance
            jndiSourceName = "jdbc/fileSyncJob";
            daoConfig.put("jndiDataSourceName", jndiSourceName);
            DBUtil.createJNDIDataSource(jndiSourceName, connectionConfig);

            // For passing to FileSyncJob
            this.jobArtifactDAO = new ArtifactDAO();
            this.jobArtifactDAO.setConfig(daoConfig);

        } catch (NamingException ne) {
            throw new IllegalStateException("unable to access database: " + daoConfig.get("database"), ne);
        }

        // jobQueue is the queue used in this producer/consumer implementation.
        // producer: FileSync uses jobQueue.put(); blocks if queue is full
        // consumer: ThreadPool uses jobQueue.take(); blocks if queue is empty

        this.jobQueue = new LinkedBlockingQueue<>(this.nthreads * 2);
        this.threadPool = new ThreadPool(this.jobQueue, this.nthreads);

        this.storageAdapter = localStorage;

        log.debug("FileSync ctor done");
    }

    public void run() {
        Iterator<String> bucketSelector = selector.getBucketIterator();
        long poll = 30 * 1000L; // 30 sec
        long recheck = 120 * 1000L; // 120 sec
        if (testRunOnce) {
            poll = 100L;
        }
        boolean ok = true;
        while (ok) {
            try {
                AvailabilityClient loc = new AvailabilityClient(locatorService);
                AvailabilityStatus status = loc.getAvailability();
                while (!status.isAvailable()) {
                    log.warn("FileSync.LOCATOR_STATUS available=false msg=" + status.getNote());
                    Thread.sleep(10 * poll);
                    status = loc.getAvailability();
                }
                
                long startQ = System.currentTimeMillis();
                long num = 0L;
                log.info("FileSync.QUERY START");
                while (bucketSelector.hasNext()) {
                    String bucket = bucketSelector.next();
                    
                    
                    log.debug("FileSync.QUERY bucket=" + bucket);
                    try (final ResourceIterator<Artifact> unstoredArtifacts = artifactDAO.unstoredIterator(bucket)) {
                        while (unstoredArtifacts.hasNext()) {
                            // TODO:  handle errors from this more sanely after they
                            // are available from the cadc-inventory-db API
                            Artifact curArtifact = unstoredArtifacts.next();
                            log.debug("create job: " + curArtifact.getURI());
                            FileSyncJob fsj = new FileSyncJob(curArtifact.getURI(), this.locatorService,
                                                              this.storageAdapter, this.jobArtifactDAO);
                            final Subject currentUser = AuthenticationUtil.getCurrentSubject();
                            fsj.setOwner(currentUser);

                            jobQueue.put(fsj); // blocks when queue capacity is reached
                            log.info("FileSync.CREATE: " + curArtifact.getURI());
                            num++;
                        }
                    } catch (Exception qex) {
                        // TODO:  handle errors from this more sanely after they
                        // are available from the cadc-inventory-db API
                        throw qex;
                    }
                    
                }
                long dtQ = System.currentTimeMillis() - startQ;
                log.info("FileSync.QUERY END dt=" + dtQ + " num=" + num);
                
                boolean waiting = true;
                while (waiting) {
                    if (jobQueue.isEmpty()) {
                        // look more closely at state of thread pool
                        if (threadPool.getAllThreadsIdle()) {
                            log.info("queue empty; jobs complete");
                            waiting = false;
                        } else {
                            log.info("queue empty; jobs running...");
                            Thread.sleep(poll);
                        }
                    } else {
                        log.info("queue contains jobs...");
                        Thread.sleep(poll);
                    }
                    
                }
                if (testRunOnce) {
                    log.warn("TEST MODE: testRunOnce=true... terminating!");
                    ok = false;
                }
            //} catch (TransientException ex) {
            //    log.error("transient error - continuing", ex);
            } catch (Exception e) {
                log.error("fatal error - terminating", e);
                ok = false;
            }
            if (ok) {
                try {
                    log.info("FileSync.SLEEP dt=" + recheck);
                    Thread.sleep(recheck);
                } catch (InterruptedException ex) {
                    ok = false;
                }
            }
        }
        this.threadPool.terminate();
    }
}

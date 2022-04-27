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

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ExpectationFailedException;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.RemoteServiceException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.client.RegistryClient;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.inventory.db.HarvestStateDAO;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.db.version.InitDatabase;
import org.opencadc.inventory.util.ArtifactSelector;
import org.opencadc.tap.RowMapException;
import org.opencadc.tap.TapClient;


/**
 * @author pdowler
 */
public class InventoryHarvester implements Runnable {

    private static final Logger log = Logger.getLogger(InventoryHarvester.class);
    public static final String CERTIFICATE_FILE_LOCATION = System.getProperty("user.home") + "/.ssl/cadcproxy.pem";
    private static final long LOOKBACK_TIME = 1 * 60 * 1000L;   // one minute, unit: ms
    private static final int INITIAL_SLEEP_TIMEOUT = (int) (LOOKBACK_TIME / 2000); // half of lookback, unit: sec
    
    private final ArtifactDAO artifactDAO;
    private final URI resourceID;
    private final ArtifactSelector selector;
    private final boolean trackSiteLocations;
    private final HarvestStateDAO harvestStateDAO;
    private final int maxRetryInterval;
    
    /**
     * Constructor.
     *
     * @param daoConfig          config map to pass to cadc-inventory-db DAO classes
     * @param resourceID         identifier for the remote query service
     * @param selector           selector implementation
     * @param trackSiteLocations Whether to track the remote storage site and add it to the Artifact being processed.
     * @param maxRetryInterval   max interval in seconds to sleep after an error during processing.
     */
    public InventoryHarvester(Map<String, Object> daoConfig, ConnectionConfig cc,
            URI resourceID, ArtifactSelector selector,
            boolean trackSiteLocations, int maxRetryInterval) {
        InventoryUtil.assertNotNull(InventoryHarvester.class, "daoConfig", daoConfig);
        InventoryUtil.assertNotNull(InventoryHarvester.class, "resourceID", resourceID);
        InventoryUtil.assertNotNull(InventoryHarvester.class, "selector", selector);
        this.artifactDAO = new ArtifactDAO(false);
        artifactDAO.setConfig(daoConfig);
        this.resourceID = resourceID;
        this.selector = selector;
        this.trackSiteLocations = trackSiteLocations;
        this.maxRetryInterval = maxRetryInterval;
        this.harvestStateDAO = new HarvestStateDAO(artifactDAO);
        
        
        // create connection pool for event streams
        try {
            int nthreads = 1; // streams in parallel
            String dsName = "jdbc/inventory";
            daoConfig.put("jndiDataSourceName", dsName);
            int poolSize = nthreads;
            org.opencadc.inventory.util.DBUtil.PoolConfig pc = new org.opencadc.inventory.util.DBUtil.PoolConfig(cc, poolSize, 20000L, "select 123");
            org.opencadc.inventory.util.DBUtil.createJNDIDataSource(dsName, pc);
        } catch (NamingException ne) {
            throw new IllegalStateException(String.format("Unable to access database: %s", cc.getURL()), ne);
        }
            
        
        try {
            String jndiDataSourceName = (String) daoConfig.get("jndiDataSourceName");
            String database = (String) daoConfig.get("database");
            String schema = (String) daoConfig.get("schema");
            DataSource ds = DBUtil.findJNDIDataSource(jndiDataSourceName);
            InitDatabase init = new InitDatabase(ds, database, schema);
            init.doInit();
            log.info("initDatabase: " + jndiDataSourceName + " " + schema + " OK");
        } catch (Exception ex) {
            throw new IllegalStateException("check/init database failed", ex);
        }

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
    }

    // general behaviour is that this process runs continually and manages it's own schedule
    // - harvest everything up to *now*
    // - go idle for a dynamically determined amount of time
    // - repeat until fail/killed

    /**
     *
     */
    @Override
    public void run() {
        boolean retry;
        int retryCount = 1;
        int sleepSeconds = INITIAL_SLEEP_TIMEOUT;

        while (true) {
            try {
                final Subject subject = SSLUtil.createSubject(new File(CERTIFICATE_FILE_LOCATION));
                int retries = retryCount;
                int timeout = sleepSeconds;

                retry = Subject.doAs(subject, (PrivilegedAction<Boolean>) () -> {
                    boolean isRetry = true;
                    try {
                        doit();
                        isRetry = false;
                        // catch exceptions resulting in a retry
                    } catch (RowMapException | ResourceNotFoundException | PreconditionFailedException | ExpectationFailedException
                             | RemoteServiceException | TransientException | IOException | NotAuthenticatedException
                             | AccessControlException | IllegalArgumentException | IllegalStateException
                             | IndexOutOfBoundsException ex) {
                        logRetry(retries, timeout, ex.getMessage());
                    } catch (InterruptedException ex) {
                        // Thread interrupted, fail.
                        throw new RuntimeException(ex.getMessage(), ex);
                    }
                    return isRetry;
                });
            } catch (RuntimeException ex) {
                logExit(ex.getMessage());
                throw ex;
            }

            // TODO: dynamic depending on how rapidly the remote content is changing
            // ... this value and the reprocess-last-N-seconds should be related
            if (retry && numEvents == 0) {
                // action failed before processing any events: delay
                retryCount++;
                sleepSeconds *= 2;
                if (sleepSeconds > this.maxRetryInterval) {
                    sleepSeconds = this.maxRetryInterval;
                }
            } else {
                // successful run or failuren after processing some events, reset retry values
                retryCount = 1;
                sleepSeconds = INITIAL_SLEEP_TIMEOUT;
            }

            try {
                log.info("InventoryHarvester.sleep duration=" + sleepSeconds);
                Thread.sleep(sleepSeconds * 1000L);
            } catch (InterruptedException ex) {
                logExit(ex.getMessage());
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
    }

    // use TapClient to get remote StorageSite record and store it locally
    // - only global inventory needs to track remote StorageSite(s); should be exactly one (minoc) record at a storage site
    // - keep the StorageSite UUID for creating SiteLocation objects below

    // get tracked progress (latest timestamp seen from any iterator)
    // - the head of an iterator is volatile: events can be inserted that are slightly out of monotonic time sequence
    // - actions taken while processing are idempotent
    // - therefore: it should be OK to re-process the last N seconds (eg startTime = curLastModified - 120 seconds)

    // use TapClient to open multiple iterator streams: Artifact, DeletedArtifactEvent, DeletedStorageLocationEvent
    // - incremental: use latest timestamp from above for all iterators
    // - selective: use ArtifactSelector to modify the query for Iterator<Artifact>

    // process multiple iterators in timestamp order
    // - taking appropriate action for each track progress
    // - stop iterating when reaching a timestamp that exceeds the timestamp when query executed

    /**
     * Perform a sync for Artifacts that match the include constraints in this harvester's selector.
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    void doit() throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                       InterruptedException {

        final Date end = new Date();
        final Date lookBack = new Date(end.getTime() - LOOKBACK_TIME);
        
        final StorageSite storageSite;
        if (trackSiteLocations) {
            
            final StorageSiteDAO storageSiteDAO = new StorageSiteDAO(this.artifactDAO);
            final StorageSiteSync storageSiteSync = new StorageSiteSync(new TapClient<>(this.resourceID), storageSiteDAO);
            storageSite = storageSiteSync.doit();
            
            try {
                syncDeletedStorageLocationEvents(new TapClient<>(this.resourceID), lookBack, end, storageSite);
            } finally {
                logSummary(DeletedStorageLocationEvent.class, true);
            }
            
        } else {
            storageSite = null;
        }

        try {
            syncDeletedArtifactEvents(new TapClient<>(this.resourceID), lookBack, end);
        } finally {
            logSummary(DeletedArtifactEvent.class, true);
        }
        
        try {
            syncArtifacts(new TapClient<>(this.resourceID), lookBack, end, storageSite);
        } finally {
            logSummary(Artifact.class, true);
        }
    }

    private long summaryInterval = 5 * 60 * 1000L; // 5 min
    private long lastSummaryTime = 0L;
    private long numEvents = 0L;
    private long numEventsTotal = 0L;
    
    private void logSummary(Class c) {
        logSummary(c, false);
    }
    
    private void logSummary(Class c, boolean doFinal) {
        if (!doFinal) {
            numEvents++;
            numEventsTotal++;
        }
        if (lastSummaryTime == 0L) {
            // first event in query result
            lastSummaryTime = System.currentTimeMillis();
            return;
        }
        if (numEvents > 0L) {
            long t2 = System.currentTimeMillis();
            long dt = t2 - lastSummaryTime;
            if (dt >= summaryInterval || doFinal) {
                double minutes = ((double) dt) / (60L * 1000L);
                long epm = Math.round(numEvents / minutes); 
                String msg = "InventoryHarvester.summary%s numTotal=%d num=%d events-per-minute=%d eob=%b";
                log.info(String.format(msg, c.getSimpleName(), numEventsTotal, numEvents, epm, doFinal));
                this.lastSummaryTime = t2;
                this.numEvents = 0L;
            }
        }
        if (doFinal) {
            this.lastSummaryTime = 0L;
            this.numEvents = 0L;
            this.numEventsTotal = 0L;
        }
    }
    
    /**
     * Perform a sync for deleted storage locations.  This will be used by Global sites to sync from storage sites to
     * indicate that an Artifact at the given storage site is no longer available.
     * @param tapClient                  A TAP client to issue a Luskan request.
     * @param lookBack                   Conservative lower bound of the Luskan query.
     * @param endDate                    The upper bound of the Luskan query.
     * @param storageSite                The storage site obtained from the Storage Site sync.  Cannot be null.
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    private void syncDeletedStorageLocationEvents(final TapClient<DeletedStorageLocationEvent> tapClient, 
            final Date lookBack, final Date endDate, final StorageSite storageSite)
        throws ResourceNotFoundException, IOException, IllegalStateException, InterruptedException, 
            TransientException {

        HarvestState hs = harvestStateDAO.get(DeletedStorageLocationEvent.class.getName(), resourceID);
        if (hs.curLastModified == null) { 
            // first harvest: ignore old deleted events?
            HarvestState artifactHS = harvestStateDAO.get(Artifact.class.getName(), resourceID);
            if (artifactHS.curLastModified == null) {
                // never artifacts harvested: ignore old deleted events
                hs.curLastModified = new Date();
                harvestStateDAO.put(hs);
                hs = harvestStateDAO.get(hs.getID());
            }
        }
        final HarvestState harvestState = hs;
        
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

        SSLUtil.renewSubject(AuthenticationUtil.getCurrentSubject(), new File(CERTIFICATE_FILE_LOCATION));

        final DeletedStorageLocationEventSync deletedStorageLocationEventSync =
                new DeletedStorageLocationEventSync(tapClient);
        deletedStorageLocationEventSync.startTime = getQueryLowerBound(lookBack, harvestState.curLastModified);
        deletedStorageLocationEventSync.endTime = endDate;

        final TransactionManager transactionManager = artifactDAO.getTransactionManager();
        
        String start = null;
        if (deletedStorageLocationEventSync.startTime != null) {
            start = df.format(deletedStorageLocationEventSync.startTime);
        }
        String end = null;
        if (deletedStorageLocationEventSync.endTime != null) {
            end = df.format(deletedStorageLocationEventSync.endTime);
        }
        log.info("DeletedStorageLocationEvent.QUERY start=" + start + " end=" + end);

        boolean first = true;
        long t1 = System.currentTimeMillis();
        try (final ResourceIterator<DeletedStorageLocationEvent> deletedStorageLocationEventResourceIterator =
                     deletedStorageLocationEventSync.getEvents()) {

            while (deletedStorageLocationEventResourceIterator.hasNext()) {
                final DeletedStorageLocationEvent deletedStorageLocationEvent =
                        deletedStorageLocationEventResourceIterator.next();
                if (first) {
                    long dt = System.currentTimeMillis() - t1;
                    log.info("DeletedStorageLocationEvent.QUERY start=" + start + " end=" + end + " duration=" + dt);
                    first = false;
                    
                    if (deletedStorageLocationEvent.getID().equals(harvestState.curID)
                        && deletedStorageLocationEvent.getLastModified().equals(harvestState.curLastModified)) {
                        log.debug("SKIP: previously processed: " + deletedStorageLocationEvent.getID());
                        // ugh but the skip is comprehensible: have to do this inside the loop when using
                        // try-with-resources
                        continue;
                    }
                }

                try {
                    transactionManager.startTransaction();
                    Artifact cur = artifactDAO.lock(deletedStorageLocationEvent.getID());
                    if (cur != null) {
                        final SiteLocation siteLocation = new SiteLocation(storageSite.getID());
                        // TODO: this message could also log the artifact and site that was removed
                        log.info("InventoryHarvester.removeSiteLocation id=" + deletedStorageLocationEvent.getID()
                                + " uri=" + cur.getURI()
                                + " lastModified=" + df.format(deletedStorageLocationEvent.getLastModified())
                                + " reason=DeletedStorageLocationEvent");
                        artifactDAO.removeSiteLocation(cur, siteLocation);
                    } else {
                        log.debug("InventoryHarvester.removeSiteLocation SKIP id=" + deletedStorageLocationEvent.getID()
                            + " reason=no-matching-artifact");
                    }
                    harvestState.curLastModified = deletedStorageLocationEvent.getLastModified();
                    harvestState.curID = deletedStorageLocationEvent.getID();
                    harvestStateDAO.put(harvestState);
                    transactionManager.commitTransaction();
                } catch (Exception exception) {
                    if (transactionManager.isOpen()) {
                        log.error("Exception in transaction.  Rolling back...");
                        transactionManager.rollbackTransaction();
                        log.error("Rollback: OK");
                    }

                    throw exception;
                } finally {
                    if (transactionManager.isOpen()) {
                        log.error("BUG: transaction open in finally. Rolling back...");
                        transactionManager.rollbackTransaction();
                        log.error("Rollback: OK");
                        throw new RuntimeException("BUG: transaction open in finally");
                    }
                }
                logSummary(DeletedStorageLocationEvent.class);
            }
        } 
    }

    /**
     * Perform a sync of the remote deleted artifact events.
     * @param tapClient                  A TAP client to issue a Luskan request.
     * @param lookBack                   Conservative lower bound of the Luskan query.
     * @param endDate                    The upper bound of the Luskan query.
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    private void syncDeletedArtifactEvents(final TapClient<DeletedArtifactEvent> tapClient, 
            final Date lookBack, final Date endDate)
        throws ResourceNotFoundException, IOException, IllegalStateException, InterruptedException, 
            TransientException {
        
        HarvestState hs = harvestStateDAO.get(DeletedArtifactEvent.class.getName(), resourceID);
        if (hs.curLastModified == null) { 
            // first harvest: ignore old deleted events?
            HarvestState artifactHS = harvestStateDAO.get(Artifact.class.getName(), resourceID);
            if (artifactHS.curLastModified == null) {
                // never artifacts harvested: ignore old deleted events
                hs.curLastModified = new Date();
                harvestStateDAO.put(hs);
                hs = harvestStateDAO.get(hs.getID());
            }
        }
        final HarvestState harvestState = hs;
        
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

        SSLUtil.renewSubject(AuthenticationUtil.getCurrentSubject(), new File(CERTIFICATE_FILE_LOCATION));

        final DeletedArtifactEventSync deletedArtifactEventSync = new DeletedArtifactEventSync(tapClient);
        final DeletedArtifactEventDAO deletedArtifactEventDeletedEventDAO = new DeletedArtifactEventDAO(this.artifactDAO);

        deletedArtifactEventSync.startTime = getQueryLowerBound(lookBack, harvestState.curLastModified);
        deletedArtifactEventSync.endTime = endDate;

        final TransactionManager transactionManager = artifactDAO.getTransactionManager();

        String start = null;
        if (deletedArtifactEventSync.startTime != null) {
            start = df.format(deletedArtifactEventSync.startTime);
        }
        String end = null;
        if (deletedArtifactEventSync.endTime != null) {
            end = df.format(deletedArtifactEventSync.endTime);
        }
        log.info("DeletedArtifactEvent.QUERY start=" + start + " end=" + end);
        boolean first = true;
        long t1 = System.currentTimeMillis();
        try (final ResourceIterator<DeletedArtifactEvent> deletedArtifactEventResourceIterator
                     = deletedArtifactEventSync.getEvents()) {

            while (deletedArtifactEventResourceIterator.hasNext()) {
                final DeletedArtifactEvent deletedArtifactEvent = deletedArtifactEventResourceIterator.next();
                if (first) {
                    long dt = System.currentTimeMillis() - t1;
                    log.info("DeletedArtifactEvent.QUERY start=" + start + " end=" + end + " duration=" + dt);
                    first = false;
                    if (deletedArtifactEvent.getID().equals(harvestState.curID)
                        && deletedArtifactEvent.getLastModified().equals(harvestState.curLastModified)) {
                        log.debug("SKIP: previously processed: " + deletedArtifactEvent.getID());
                        // ugh but the skip is comprehensible: have to do this inside the loop when using
                        // try-with-resources
                        continue;
                    }
                }
                
                try {
                    transactionManager.startTransaction();
                    Artifact cur = artifactDAO.lock(deletedArtifactEvent.getID());
                    
                    String logURI = "";
                    if (cur != null) {
                        logURI = " uri=" + cur.getURI();
                        log.info("InventoryHarvester.deleteArtifact id=" + cur.getID()
                            + logURI
                            + " lastModified=" + df.format(deletedArtifactEvent.getLastModified())
                            + " reason=DeletedArtifactEvent");
                        artifactDAO.delete(deletedArtifactEvent.getID());
                    }
                    log.info("InventoryHarvester.putDeletedArtifactEvent id=" + deletedArtifactEvent.getID()
                            + logURI
                            + " lastModified=" + df.format(deletedArtifactEvent.getLastModified()));
                    deletedArtifactEventDeletedEventDAO.put(deletedArtifactEvent);
                    
                    harvestState.curLastModified = deletedArtifactEvent.getLastModified();
                    harvestState.curID = deletedArtifactEvent.getID();
                    harvestStateDAO.put(harvestState);
                    transactionManager.commitTransaction();
                } catch (Exception exception) {
                    if (transactionManager.isOpen()) {
                        log.error("Exception in transaction.  Rolling back...");
                        transactionManager.rollbackTransaction();
                        log.error("Rollback: OK");
                    }

                    throw exception;
                } finally {
                    if (transactionManager.isOpen()) {
                        log.error("BUG: transaction open in finally. Rolling back...");
                        transactionManager.rollbackTransaction();
                        log.error("Rollback: OK");
                        throw new RuntimeException("BUG: transaction open in finally");
                    }
                }
                logSummary(DeletedArtifactEvent.class);
            }
        } 
    }

    /**
     * Synchronize the artifacts found by the TAP (Luskan) query.
     *
     * @param tapClient                  A TAP client to issue a Luskan request.
     * @param lookBack                   Conservative lower bound of the Luskan query.
     * @param endDate                    The upper bound of the Luskan query.
     * @param storageSite                The storage site obtained from the Storage Site sync.  Optional.
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    private void syncArtifacts(final TapClient<Artifact> tapClient, 
            final Date lookBack, final Date endDate, final StorageSite storageSite)
        throws ResourceNotFoundException, IOException, IllegalStateException, InterruptedException, 
            TransientException {
        final MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("BUG: failed to get instance of MD5", e);
        }

        final SiteLocation remoteSiteLocation = (storageSite == null ? null : new SiteLocation(storageSite.getID()));
        
        SSLUtil.renewSubject(AuthenticationUtil.getCurrentSubject(), new File(CERTIFICATE_FILE_LOCATION));

        final HarvestState harvestState = this.harvestStateDAO.get(Artifact.class.getName(), this.resourceID);

        final ArtifactSync artifactSync = new ArtifactSync(tapClient);
        artifactSync.includeClause = this.selector.getConstraint();
        artifactSync.startTime = getQueryLowerBound(lookBack, harvestState.curLastModified);
        artifactSync.endTime = endDate;
        
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        if (lookBack != null && harvestState.curLastModified != null) {
            log.debug("lookBack=" + df.format(lookBack) + " curLastModified=" + df.format(harvestState.curLastModified) 
                + " -> " + df.format(artifactSync.startTime));
        }
        String start = null;
        if (artifactSync.startTime != null) {
            start = df.format(artifactSync.startTime);
        }
        String end = null;
        if (artifactSync.endTime != null) {
            end = df.format(artifactSync.endTime);
        }
        log.info("Artifact.QUERY start=" + start + " end=" + end);
        
        final TransactionManager transactionManager = artifactDAO.getTransactionManager();
        final DeletedArtifactEventDAO daeDAO = new DeletedArtifactEventDAO(artifactDAO);
        
        boolean first = true;
        long t1 = System.currentTimeMillis();
        try (final ResourceIterator<Artifact> artifactResourceIterator = artifactSync.iterator()) {
            while (artifactResourceIterator.hasNext()) {
                final Artifact artifact = artifactResourceIterator.next();
                if (first) {
                    long dt = System.currentTimeMillis() - t1;
                    log.info("Artifact.QUERY start=" + start + " end=" + end + " duration=" + dt);
                    first = false;
                    if (artifact.getID().equals(harvestState.curID)
                        && artifact.getLastModified().equals(harvestState.curLastModified)) {
                        log.debug("SKIP PUT: previously processed: " + artifact.getID() + " " + artifact.getURI());
                        // ugh but the skip is comprehensible: have to do this inside the loop when using
                        // try-with-resources
                        continue;
                    }
                }
                

                log.debug("START: Process Artifact " + artifact.getID() + " " + artifact.getURI());

                final URI computedChecksum = artifact.computeMetaChecksum(messageDigest);
                if (!artifact.getMetaChecksum().equals(computedChecksum)) {
                    throw new IllegalStateException("checksum mismatch: " + artifact.getID() + " " + artifact.getURI()
                            + " provided=" + artifact.getMetaChecksum() + " actual=" + computedChecksum);
                }

                // check if the artifact already deleted (sync from stale site)
                DeletedArtifactEvent ev = daeDAO.get(artifact.getID());
                if (ev != null) {
                    log.info("InventoryHarvester.skipArtifact id=" + artifact.getID() 
                            + " uri=" + artifact.getURI() + " lastModified=" + df.format(artifact.getLastModified())
                            + " reason=DeletedArtifactEvent");
                    continue;
                }
                        
                Artifact collidingArtifact = artifactDAO.get(artifact.getURI());
                if (collidingArtifact != null && collidingArtifact.getID().equals(artifact.getID())) {
                    // same ID: not a collision
                    collidingArtifact = null;
                }

                try {
                    transactionManager.startTransaction();

                    // since Artifact.id and Artifact.uri are both unique keys, there is only ever one "current artifact"
                    // it normally has the same ID but may be the colliding artifact
                    Artifact currentArtifact = null;
                    if (collidingArtifact == null) {
                        currentArtifact = artifactDAO.lock(artifact);
                    } else {
                        currentArtifact = artifactDAO.lock(collidingArtifact);
                    }

                    boolean continueWithPut = true;
                    if (collidingArtifact != null && currentArtifact != null) {
                        // resolve collision
                        if (isRemoteWinner(currentArtifact, artifact, (remoteSiteLocation != null))) {
                            DeletedArtifactEvent dae = new DeletedArtifactEvent(currentArtifact.getID());
                            log.info("InventoryHarvester.createDeletedArtifactEvent id=" + dae.getID()
                                    + " uri=" + currentArtifact.getURI()
                                    + " reason=resolve-collision");
                            daeDAO.put(new DeletedArtifactEvent(currentArtifact.getID()));
                            log.info("InventoryHarvester.deleteArtifact id=" + currentArtifact.getID()
                                    + " uri=" + currentArtifact.getURI()
                                    + " contentLastModified=" + df.format(currentArtifact.getContentLastModified())
                                    + " reason=resolve-collision");
                            artifactDAO.delete(currentArtifact.getID());
                        } else {
                            log.info("InventoryHarvester.skipArtifact id=" + artifact.getID() 
                                    + " uri=" + artifact.getURI() 
                                    + " contentLastModified=" + df.format(currentArtifact.getContentLastModified())
                                    + " reason=uri-collision");
                            continueWithPut = false;
                        }
                    }

                    // addSiteLocation may modify lastModified so capture real value here
                    final Date harvestedLastModified = artifact.getLastModified();
                    
                    if (continueWithPut) {
                        // merge existing non-entity state
                        if (currentArtifact != null) {
                            if (remoteSiteLocation != null) {
                                // trackSiteLocations: keep SiteLocation(s)
                                artifact.siteLocations.addAll(currentArtifact.siteLocations);
                            } else {
                                // storage site: keep StorageLocation
                                artifact.storageLocation = currentArtifact.storageLocation;
                            }
                        }

                        log.info("InventoryHarvester.putArtifact id=" + artifact.getID() 
                                + " uri=" + artifact.getURI() 
                                + " lastModified=" + df.format(artifact.getLastModified()));
                        artifactDAO.put(artifact);
                        if (remoteSiteLocation != null) {
                            // explicit so addSiteLocation can force lastModified update in global
                            artifactDAO.addSiteLocation(artifact, remoteSiteLocation);
                        }
                    }
                    
                    harvestState.curLastModified = harvestedLastModified;
                    harvestState.curID = artifact.getID();
                    harvestStateDAO.put(harvestState);
                    transactionManager.commitTransaction();
                    log.debug("END: Process Artifact " + artifact.getID() + " " + artifact.getURI());
                } catch (Exception exception) {
                    if (transactionManager.isOpen()) {
                        log.error("Exception in transaction.  Rolling back...");
                        transactionManager.rollbackTransaction();
                        log.error("Rollback: OK");
                    }

                    throw exception;
                } finally {
                    if (transactionManager.isOpen()) {
                        log.error("BUG: transaction open in finally. Rolling back...");
                        transactionManager.rollbackTransaction();
                        log.error("Rollback: OK");
                        throw new RuntimeException("BUG: transaction open in finally");
                    }
                }
                logSummary(Artifact.class);
            }
        }
    }
    
    // true if remote is the winner, false if local is the winner
    // same logic in ratik ArtifactValidator
    boolean isRemoteWinner(Artifact local, Artifact remote, boolean remoteStorageSite) {
        Boolean rem = InventoryUtil.isRemoteWinner(local, remote);
        if (rem != null) {
            return rem;
        }
        
        // equal timestamps and different instances:
        // likely caused by poorly behaved storage site that ingests duplicates 
        // from external system
        if (remoteStorageSite) {
            // declare the artifact in global as the winner
            return false;
        }
        // this is storage site: remote is global
        return true;
    }

    // incremental mode: look back in time a little because head of sequence is not stable
    private Date getQueryLowerBound(Date lookBack, Date lastModified) {
        if (lookBack == null) {
            // feature not enabled
            return lastModified;
        }
        if (lastModified == null) {
            // first harvest
            return null;
        }
        if (lookBack.before(lastModified)) {
            return lookBack;
        }
        return lastModified;
        
    }
    
    private void logRetry(int retries, int timeout, String message) {
        log.error(String.format("retry[%s] timeout %ss - reason: %s", retries, timeout, message));
    }

    private void logExit(String message) {
        log.error(String.format("Exiting, reason - %s", message));
    }
}

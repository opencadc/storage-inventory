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

package org.opencadc.fenwick;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;

import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedEventDAO;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.inventory.db.HarvestStateDAO;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.db.version.InitDatabase;
import org.opencadc.tap.TapClient;


/**
 * @author pdowler
 */
public class InventoryHarvester implements Runnable {

    private static final Logger log = Logger.getLogger(InventoryHarvester.class);

    private final ArtifactDAO artifactDAO;
    private final URI resourceID;
    private final Capability inventoryTAP;
    private final ArtifactSelector selector;
    private final boolean trackSiteLocations;
    private final HarvestStateDAO harvestStateDAO;
    private int errorCount = 0;

    /**
     * Constructor.
     *
     * @param daoConfig          config map to pass to cadc-inventory-db DAO classes
     * @param resourceID         identifier for the remote query service
     * @param selector           selector implementation
     * @param trackSiteLocations Whether to track the remote storage site and add it to the Artifact being processed.
     */
    public InventoryHarvester(Map<String, Object> daoConfig, URI resourceID, ArtifactSelector selector,
                              boolean trackSiteLocations) {
        InventoryUtil.assertNotNull(InventoryHarvester.class, "daoConfig", daoConfig);
        InventoryUtil.assertNotNull(InventoryHarvester.class, "resourceID", resourceID);
        InventoryUtil.assertNotNull(InventoryHarvester.class, "selector", selector);
        this.artifactDAO = new ArtifactDAO(false);
        artifactDAO.setConfig(daoConfig);
        this.resourceID = resourceID;
        this.selector = selector;
        this.trackSiteLocations = trackSiteLocations;
        this.harvestStateDAO = new HarvestStateDAO(artifactDAO);
        
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
            Capabilities caps = rc.getCapabilities(resourceID);
            // above call throws IllegalArgumentException... should be ResourceNotFoundException but out of scope to fix
            this.inventoryTAP = caps.findCapability(Standards.TAP_10);
            if (inventoryTAP == null) {
                throw new IllegalArgumentException(
                        "invalid config: remote query service " + resourceID + " does not implement "
                        + Standards.TAP_10);
            }
        } catch (ResourceNotFoundException ex) {
            throw new IllegalArgumentException("query service not found: " + resourceID, ex);
        } catch (IOException ex) {
            throw new IllegalArgumentException("invalid config", ex);
        }
    }

    // general behaviour is that this process runs continually and manages it's own schedule
    // - harvest everything up to *now*
    // - go idle for a dynamically determined amount of time
    // - repeat until fail/killed
    @Override
    public void run() {
        while (true) {
            try {
                doit();

                // TODO: dynamic depending on how rapidly the remote content is changing
                // ... this value and the reprocess-last-N-seconds should be related
                long dt = 60 * 1000L;
                Thread.sleep(dt);
            } catch (IllegalArgumentException ex) {
                // Be careful here.  This IllegalArgumentException is being caught to work around a mysterious
                // case where TCP connections are simply dropped and the incoming stream of data is invalid.
                // This catch will allow Fenwick to restart its processing.
                // jenkinsd 2020.09.25
                final String message = ex.getMessage().trim();
                if (!message.startsWith("wrong number of columns")) {
                    log.error("\n\n*******\n");
                    log.error("Caught IllegalArgumentException - " + message + " (" + ++errorCount + ")");
                    log.error("Ignoring error as presumed to be a dropped connection before fully reading the stream.");
                    log.error("\n*******\n");
                } else if (!message.startsWith("invalid checksum URI:")) {
                    log.error("\n\n*******\n");
                    log.error("Caught IllegalArgumentException - " + message + " (" + ++errorCount + ")");
                    log.error("Ignoring error as presumed to be a dropped connection before fully reading the stream.");
                    log.error("CAUTION! - This could be an actual mad MD5 checksum! Logging this to provide an audit.");
                    log.error("\n*******\n");
                } else {
                    throw ex;
                }
            } catch (InterruptedException ex) {
                throw new RuntimeException("interrupted", ex);
            } catch (Exception ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
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
     * @throws NoSuchAlgorithmException  If the MessageDigest for synchronizing Artifacts cannot be used.
     */
    void doit() throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                       InterruptedException, NoSuchAlgorithmException {
        final StorageSite storageSite;

        if (trackSiteLocations) {
            final TapClient<StorageSite> storageSiteTapClient = new TapClient<>(this.resourceID);
            final StorageSiteDAO storageSiteDAO = new StorageSiteDAO(this.artifactDAO);
            final StorageSiteSync storageSiteSync = new StorageSiteSync(storageSiteTapClient, storageSiteDAO);
            storageSite = storageSiteSync.doit();
            syncDeletedStorageLocationEvents(storageSite);
        } else {
            storageSite = null;
        }

        syncDeletedArtifactEvents();
        syncArtifacts(storageSite);
    }

    /**
     * Perform a sync for deleted storage locations.  This will be used by Global sites to sync from storage sites to
     * indicate that an Artifact at the given storage site is no longer available.
     * @param storageSite                The storage site obtained from the Storage Site sync.  Cannot be null.
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    private void syncDeletedStorageLocationEvents(final StorageSite storageSite)
            throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                   InterruptedException {
        final HarvestState existingHarvestState = this.harvestStateDAO.get(DeletedStorageLocationEvent.class.getName(),
                                                                           this.resourceID);
        final HarvestState harvestState = (existingHarvestState == null)
                                          ? new HarvestState(DeletedStorageLocationEvent.class.getName(),
                                                             this.resourceID)
                                          : existingHarvestState;

        final TapClient<DeletedStorageLocationEvent> deletedStorageLocationEventTapClient =
                new TapClient<>(this.resourceID);
        final DeletedStorageLocationEventSync deletedStorageLocationEventSync =
                new DeletedStorageLocationEventSync(deletedStorageLocationEventTapClient);
        deletedStorageLocationEventSync.startTime = harvestState.curLastModified;

        final TransactionManager transactionManager = artifactDAO.getTransactionManager();
        try (final ResourceIterator<DeletedStorageLocationEvent> deletedStorageLocationEventResourceIterator =
                     deletedStorageLocationEventSync.getEvents()) {

            while (deletedStorageLocationEventResourceIterator.hasNext()) {
                final DeletedStorageLocationEvent deletedStorageLocationEvent =
                        deletedStorageLocationEventResourceIterator.next();

                transactionManager.startTransaction();
                final Artifact artifact = this.artifactDAO.get(deletedStorageLocationEvent.getID());
                if (artifact != null) {
                    final SiteLocation siteLocation = new SiteLocation(storageSite.getID());
                    artifactDAO.removeSiteLocation(artifact, siteLocation);
                    harvestState.curLastModified = deletedStorageLocationEvent.getLastModified();
                    harvestStateDAO.put(harvestState);
                } else {
                    log.warn("Artifact " + deletedStorageLocationEvent.getID() + " does not exist locally.");
                }
                transactionManager.commitTransaction();
            }
        } catch (Exception exception) {
            if (transactionManager.isOpen()) {
                log.error("Exception in transaction.  Rolling back...");
                transactionManager.rollbackTransaction();
                log.error("Rollback: OK");
            } else {
                log.warn("No transaction started.");
            }

            throw exception;
        }
    }

    /**
     * Perform a sync of the remote deleted artifact events.
     *
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    private void syncDeletedArtifactEvents() throws ResourceNotFoundException, IOException, IllegalStateException,
                                                    TransientException, InterruptedException {
        final HarvestState existingHarvestState = this.harvestStateDAO.get(DeletedArtifactEvent.class.getName(),
                                                                           this.resourceID);
        final HarvestState harvestState = (existingHarvestState == null)
                                                  ? new HarvestState(DeletedArtifactEvent.class.getName(),
                                                                     this.resourceID)
                                                  : existingHarvestState;

        final TapClient<DeletedArtifactEvent> deletedArtifactEventTapClientTapClient = new TapClient<>(this.resourceID);
        final DeletedArtifactEventSync deletedArtifactEventSync =
                new DeletedArtifactEventSync(deletedArtifactEventTapClientTapClient);
        final DeletedEventDAO<DeletedArtifactEvent> deletedArtifactEventDeletedEventDAO =
                new DeletedEventDAO<>(this.artifactDAO);
        deletedArtifactEventSync.startTime = harvestState.curLastModified;

        final TransactionManager transactionManager = artifactDAO.getTransactionManager();

        try (final ResourceIterator<DeletedArtifactEvent> deletedArtifactEventResourceIterator
                     = deletedArtifactEventSync.getEvents()) {

            while (deletedArtifactEventResourceIterator.hasNext()) {
                final DeletedArtifactEvent deletedArtifactEvent = deletedArtifactEventResourceIterator.next();
                transactionManager.startTransaction();
                deletedArtifactEventDeletedEventDAO.put(deletedArtifactEvent);
                artifactDAO.delete(deletedArtifactEvent.getID());
                harvestState.curLastModified = deletedArtifactEvent.getLastModified();
                harvestStateDAO.put(harvestState);
                transactionManager.commitTransaction();
            }
        } catch (Exception exception) {
            if (transactionManager.isOpen()) {
                log.error("Exception in transaction.  Rolling back...");
                transactionManager.rollbackTransaction();
                log.error("Rollback: OK");
            } else {
                log.warn("No transaction started.");
            }

            throw exception;
        }
    }

    /**
     * Synchronize the artifacts found by the TAP (Luskan) query.
     *
     * @param storageSite                The storage site obtained from the Storage Site sync.  Optional.
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws NoSuchAlgorithmException  If the MessageDigest for synchronizing Artifacts cannot be used.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    private void syncArtifacts(final StorageSite storageSite) throws ResourceNotFoundException, IOException,
                                                                     IllegalStateException, NoSuchAlgorithmException,
                                                                     InterruptedException, TransientException {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

        final TapClient<Artifact> artifactTapClient = new TapClient<>(this.resourceID);

        final HarvestState existingHarvestState = this.harvestStateDAO.get(Artifact.class.getName(), this.resourceID);
        final HarvestState harvestState = (existingHarvestState == null)
                                          ? new HarvestState(Artifact.class.getName(), this.resourceID)
                                          : existingHarvestState;

        final ArtifactSync artifactSync = new ArtifactSync(artifactTapClient, harvestState.curLastModified);
        final List<String> artifactIncludeConstraints = this.selector.getConstraints();
        final Iterator<String> artifactIncludeConstraintIterator = artifactIncludeConstraints.iterator();

        final TransactionManager transactionManager = artifactDAO.getTransactionManager();

        try {
            boolean keepRunning = true;

            // This loop will always run at least once.  This will allow a list of constraints of one or more clauses,
            // or include the case where no constraints are issued.
            while (keepRunning) {

                if (artifactIncludeConstraintIterator.hasNext()) {
                    artifactSync.includeClause = artifactIncludeConstraintIterator.next();
                }

                try (final ResourceIterator<Artifact> artifactResourceIterator = artifactSync.iterator()) {
                    while (artifactResourceIterator.hasNext()) {
                        final Artifact artifact = artifactResourceIterator.next();
                        log.debug("START: Process Artifact " + artifact.getURI());

                        final URI computedChecksum = artifact.computeMetaChecksum(messageDigest);
                        if (!artifact.getMetaChecksum().equals(computedChecksum)) {
                            throw new IllegalStateException("checksum mismatch: " + artifact.getID() + " " + artifact.getURI()
                                    + " provided=" + artifact.getMetaChecksum() + " actual=" + computedChecksum);
                        }

                        // TODO: acquire update lock on current artifact and get it again, move startTransaction
                        Artifact currentArtifact = artifactDAO.get(artifact.getID());
                        if (storageSite != null) {
                            // trackSiteLocations: merge SiteLocation(s)
                            artifact.siteLocations.add(new SiteLocation(storageSite.getID()));
                            if (currentArtifact != null) {
                                artifact.siteLocations.addAll(currentArtifact.siteLocations);
                            }
                        } else {
                            // storage site: keep StorageLocation
                            if (currentArtifact != null) {
                                artifact.storageLocation = currentArtifact.storageLocation;
                            }
                        }

                        transactionManager.startTransaction();

                        log.info("PUT: " + artifact.getID() + " " + artifact.getURI() + " "
                                 + df.format(artifact.getLastModified()));
                        artifactDAO.put(artifact);
                        harvestState.curLastModified = artifact.getLastModified();
                        harvestStateDAO.put(harvestState);
                        transactionManager.commitTransaction();
                        log.debug("END: Process Artifact " + artifact.getURI() + ".");
                    }
                }

                keepRunning = artifactIncludeConstraintIterator.hasNext();
            }
        } catch (Exception exception) {
            if (transactionManager.isOpen()) {
                log.error("Exception in transaction.  Rolling back...");
                transactionManager.rollbackTransaction();
                log.error("Rollback: OK");
            } else {
                log.warn("No transaction started.");
            }

            throw exception;
        }
    }
}

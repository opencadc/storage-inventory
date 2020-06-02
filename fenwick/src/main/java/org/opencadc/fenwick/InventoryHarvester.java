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
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.inventory.db.HarvestStateDAO;
import org.opencadc.inventory.db.ObsoleteStorageLocationDAO;
import org.opencadc.inventory.db.StorageSiteDAO;
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
        this.artifactDAO = new ArtifactDAO();
        artifactDAO.setConfig(daoConfig);
        this.resourceID = resourceID;
        this.selector = selector;
        this.trackSiteLocations = trackSiteLocations;
        this.harvestStateDAO = new HarvestStateDAO(artifactDAO);

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
            } catch (InterruptedException ex) {
                throw new RuntimeException("interrupted", ex);
            } catch (Exception ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            }
        }
    }

    // use TapClient to get remote StorageSite record and store it locally
    // - only global inventory needs to track remote StorageSite(s); should be exactly one (minoc) record at a
    // storage site
    // - could be a harmless simplification to ignore the fact that storage sites don't need to sync other
    // StorageSite records from global
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
    private void doit() throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                               InterruptedException, NoSuchAlgorithmException {
        final HarvestState existingHarvestState = this.harvestStateDAO.get(Artifact.class.getName(), this.resourceID);
        final HarvestState harvestState = (existingHarvestState == null)
                                          ? new HarvestState(Artifact.class.getName(), this.resourceID)
                                          : existingHarvestState;

        syncDeletedStorageLocationEvents(harvestState.getLastModified());
        syncDeletedArtifactEvents(harvestState.getLastModified());
        syncArtifacts(harvestState);
    }

    private void syncDeletedStorageLocationEvents(final Date lastModified)
            throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                   InterruptedException {
        final TapClient<DeletedStorageLocationEvent> deletedStorageLocationEventTapClient =
                new TapClient<>(this.resourceID);
        final DeletedStorageLocationEventSync deletedStorageLocationEventSync =
                new DeletedStorageLocationEventSync(deletedStorageLocationEventTapClient);
        deletedStorageLocationEventSync.startTime = lastModified;

        try (final ResourceIterator<DeletedStorageLocationEvent> deletedStorageLocationEventResourceIterator =
                     deletedStorageLocationEventSync.getEvents()) {
            deletedStorageLocationEventResourceIterator.forEachRemaining(deletedStorageLocationEvent -> {
                final Artifact artifact = this.artifactDAO.get(deletedStorageLocationEvent.getID());
                artifact.storageLocation = null;
                artifactDAO.put(artifact);
            });
        }
    }

    private void syncDeletedArtifactEvents(final Date lastModified)
            throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                   InterruptedException {
        final TapClient<DeletedArtifactEvent> deletedArtifactEventTapClientTapClient = new TapClient<>(this.resourceID);
        final DeletedArtifactEventSync deletedArtifactEventSync =
                new DeletedArtifactEventSync(deletedArtifactEventTapClientTapClient);
        deletedArtifactEventSync.startTime = lastModified;

        deletedArtifactEventSync.getEvents().forEachRemaining(deletedArtifactEvent -> {
            artifactDAO.delete(deletedArtifactEvent.getID());
        });
    }

    /**
     * Synchronize the artifacts found by the TAP (Luskan) query.  This method WILL modify the HarvestState and store it
     * for the next run.
     *
     * @param harvestState      The HarvestState object w
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     * @throws NoSuchAlgorithmException  If the MessageDigest for synchronizing Artifacts cannot be used.
     */
    private void syncArtifacts(final HarvestState harvestState)
            throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                   InterruptedException, NoSuchAlgorithmException {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        final StorageSiteDAO storageSiteDAO = new StorageSiteDAO(this.artifactDAO);
        final TapClient<StorageSite> storageSiteTapClient = new TapClient<>(this.resourceID);

        final StorageSite storageSite;
        if (trackSiteLocations) {
            final StorageSiteSync storageSiteSync = new StorageSiteSync(storageSiteTapClient, storageSiteDAO);
            storageSite = storageSiteSync.doit();
        } else {
            storageSite = null;
        }

        final TapClient<Artifact> artifactTapClient = new TapClient<>(this.resourceID);
        final ArtifactSync artifactSync = new ArtifactSync(artifactTapClient, harvestState.getLastModified());
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
                    artifactResourceIterator.forEachRemaining(artifact -> {
                        transactionManager.startTransaction();

                        final URI computedChecksum = artifact.computeMetaChecksum(messageDigest);

                        if (!artifact.getMetaChecksum().equals(computedChecksum)) {
                            throw new IllegalStateException(
                                    String.format("Checksums for Artifact %s do not match.  "
                                                  + "\nProvided: %s\nComputed: %s.",
                                                  artifact.getURI(), artifact.getMetaChecksum(), computedChecksum));
                        }

                        Artifact currentArtifact = artifactDAO.get(artifact.getID());

                        if (currentArtifact != null) {
                            // trackSiteLocations is false if storageSite is null.
                            if (storageSite != null) {
                                currentArtifact.siteLocations.add(new SiteLocation(storageSite.getID()));
                            }

                            currentArtifact.contentEncoding = artifact.contentEncoding;
                            currentArtifact.contentType = artifact.contentType;
                            currentArtifact.storageLocation = artifact.storageLocation;
                        } else {
                            currentArtifact = artifact;
                        }

                        artifactDAO.put(currentArtifact);
                        harvestState.curLastModified = artifact.getLastModified();
                        harvestStateDAO.put(harvestState);
                        transactionManager.commitTransaction();
                    });
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

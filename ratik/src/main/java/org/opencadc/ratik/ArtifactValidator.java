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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.ratik;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.DeletedStorageLocationEventDAO;
import org.opencadc.tap.TapClient;
import org.opencadc.tap.TapRowMapper;

/**
 * Class that compares and validates two Artifacts, repairing the local Artifact to
 * resolve any discrepancies.
 */
public class ArtifactValidator {
    private static final Logger log = Logger.getLogger(ArtifactValidator.class);

    private final ArtifactDAO artifactDAO;
    private final URI resourceID;
    private final boolean trackSiteLocations;

    private final DeletedArtifactEventDAO deletedArtifactEventDAO;
    private final DeletedStorageLocationEventDAO deletedStorageLocationEventDAO;
    private final TransactionManager transactionManager;
    private final DateFormat dateFormat;

    /**
     * Constructor
     *
     * @param artifactDAO local inventory database.
     * @param resourceID remote service resourceID.
     * @param trackSiteLocations true if the local service is a global site, false otherwise.
     */
    public ArtifactValidator(ArtifactDAO artifactDAO, URI resourceID, boolean trackSiteLocations) {
        this.artifactDAO = artifactDAO;
        this.resourceID = resourceID;
        this.trackSiteLocations = trackSiteLocations;

        this.transactionManager = this.artifactDAO.getTransactionManager();
        this.deletedArtifactEventDAO = new DeletedArtifactEventDAO(this.artifactDAO);
        this.deletedStorageLocationEventDAO = new DeletedStorageLocationEventDAO(this.artifactDAO);
        this.dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    }

    /**
     * Validate the local and remote Artifacts.
     *
     * @param local the local Artifact.
     * @param remote the remote Artifact.
     * @throws InterruptedException         Thread interrupted.
     * @throws IOException                  For unreadable configuration files.
     * @throws ResourceNotFoundException    For any missing required configuration that is missing.
     * @throws TransientException           Temporary failure of TAP service: same call could work in future.
     */
    public void validate(Artifact local, Artifact remote)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        if (local == null && remote == null) {
            throw new IllegalArgumentException("local and remote Artifact can not both be null");
        } else if (remote == null) {
            validateLocal(local);
        } else if (local == null) {
            validateRemote(remote);
        } else {
            validateLocalAndRemote(local, remote);
        }
    }

    /**
     * discrepancy: artifact in L && artifact not in R
     *
     * <p>explanation0: filter policy at L changed to exclude artifact in R
     * evidence: Artifact in R without filter
     * action: delete Artifact, if (L==storage) create DeletedStorageLocationEvent
     *
     * <p>explanation1: deleted from R, pending/missed DeletedArtifactEvent in L
     * evidence: DeletedArtifactEvent in R
     * action: put DAE, delete artifact
     *
     * <p>explanation2: L==global, deleted from R, pending/missed DeletedStorageLocationEvent in L
     * evidence: DeletedStorageLocationEvent in R
     * action: remove siteID from Artifact.storageLocations (see below)
     *
     * <p>explanation3: L==global, new Artifact in L, pending/missed Artifact or sync in R
     * evidence: ?
     * action: remove siteID from Artifact.storageLocations (see below)
     *
     * <p>explanation4: L==storage, new Artifact in L, pending/missed new Artifact event in R
     * evidence: ?
     * action: none
     *
     * <p>explanation6: deleted from R, lost DeletedArtifactEvent
     * evidence: ?
     * action: assume explanation3
     *
     * <p>explanation7: L==global, lost DeletedStorageLocationEvent
     * evidence: ?
     * action: assume explanation3
     *
     * <p>note: when removing siteID from Artifact.storageLocations in global, if the Artifact.siteLocations becomes empty
     * the artifact should be deleted (metadata-sync needs to also do this in response to a DeletedStorageLocationEvent)
     * TBD: must this also create a DeletedArtifactEvent?
     *
     * @param local                         The local Artifact.
     * @throws InterruptedException         Thread interrupted.
     * @throws IOException                  For unreadable configuration files.
     * @throws ResourceNotFoundException    For any missing required configuration that is missing.
     * @throws TransientException           Temporary failure of TAP service: same call could work in future.
     */
    protected void validateLocal(Artifact local)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {

        // explanation0: filter policy at L changed to exclude artifact in R
        // evidence: Artifact in R without filter
        // action: delete Artifact, if (L==storage) create DeletedStorageLocationEvent
        Artifact remote = getRemoteArtifact(local.getURI());
        if (remote != null) {
            log.info(String.format("Artifact in L && not in R - Explanation0: Artifact in R without filter\n"
                                       + "action: delete Artifact, if (L==storage) create DeletedStorageLocationEvent\n"
                                       + " local - %s\nremote - %s", local, remote));
            return;
        }

        // explanation1: deleted from R, pending/missed DeletedArtifactEvent in L
        // evidence: DeletedArtifactEvent in R
        // action: put DAE, delete artifact
        DeletedArtifactEvent remoteDeletedArtifactEvent = getRemoteDeletedArtifactEvent(local.getID());
        if (remoteDeletedArtifactEvent != null) {
            log.info(String.format("Artifact in L && not in R - Explanation1: DeletedArtifactEvent in R\n"
                                       + "action: put DeletedArtifactEvent, delete Artifact\n"
                                       + " local Artifact: %s\nremote DeletedArtifactEvent: %s",
                                   local, remoteDeletedArtifactEvent));
            return;
        }

        // 2. DeletedStorageLocationEvent in R
        // remove siteID from Artifact.storageLocations
        DeletedStorageLocationEvent remoteDeletedStorageLocationEvent =
            getRemoteDeletedStorageLocationEvent(local.getID());
        if (remoteDeletedStorageLocationEvent != null) {
            log.info(String.format("Artifact in L && not in R - Explanation2: DeletedStorageLocationEvent in R\n"
                                       + "action: remove siteID from Artifact.storageLocations\n"
                                       + " local Artifact: %s\nremote DeletedStorageLocationEvent: %s",
                                   local, remoteDeletedStorageLocationEvent));
            return;
        }

        // explanation3: L==global, new Artifact in L, pending/missed Artifact or sync in R
        // explanation4: L==storage, new Artifact in L, pending/missed new Artifact event in R
        // explanation6: deleted from R, lost DeletedArtifactEvent
        // explanation7: L==global, lost DeletedStorageLocationEvent
        // evidence: ?
        // action: remove siteID from Artifact.storageLocations
        log.info(String.format("Artifact in L && not in R - Explanation3,4,6,7\n"
                                   + "remove siteID from Artifact.storageLocations\nlocal Artifact: %s", local));
    }

    /**
     * discrepancy: artifact not in L && artifact in R
     *
     * <p>explanation0: filter policy at L changed to include artifact in R
     * evidence: ?
     * action: equivalent to missed Artifact event (explanation3 below)
     *
     * <p>explanation1: deleted from L, pending/missed DeletedArtifactEvent in R
     * evidence: DeletedArtifactEvent in L
     * action: none
     *
     * <p>explanation2: L==storage, deleted from L, pending/missed DeletedStorageLocationEvent in R
     * evidence: DeletedStorageLocationEvent in L
     * action: none
     *
     * <p>explanation3: L==storage, new Artifact in R, pending/missed new Artifact event in L
     * evidence: ?
     * action: insert Artifact
     *
     * <p>explanation4: L==global, new Artifact in R, pending/missed changed Artifact event in L
     * evidence: Artifact in local db but siteLocations does not include remote siteID
     * action: add siteID to Artifact.siteLocations
     *
     * <p>explanation6: deleted from L, lost DeletedArtifactEvent
     * evidence: ?
     * action: assume explanation3
     *
     * <p>explanation7: L==storage, deleted from L, lost DeletedStorageLocationEvent
     * evidence: ?
     * action: assume explanation3
c     *
     * @param remote                        The remote Artifact.
     * @throws InterruptedException         Thread interrupted.
     * @throws IOException                  For unreadable configuration files.
     * @throws ResourceNotFoundException    For any missing required configuration that is missing.
     * @throws TransientException           Temporary failure of TAP service: same call could work in future.
     */
    protected void validateRemote(Artifact remote)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        // explanation1: deleted from L, pending/missed DeletedArtifactEvent in R
        // evidence: DeletedArtifactEvent in L
        // action: none
        DeletedArtifactEvent localDeletedArtifactEvent = this.deletedArtifactEventDAO.get(remote.getID());
        if (localDeletedArtifactEvent != null) {
            log.info(String.format("Artifact not in L && in R - Explanation1: DeletedArtifactEvent in L\n"
                                       + "action: no action necessary\n"
                                       + "remote - %s\nlocal DeletedArtifactEvent - %s",
                                   remote, localDeletedArtifactEvent));
            return;
        }

        // explanation2: L==storage, deleted from L, pending/missed DeletedStorageLocationEvent in R
        // evidence: DeletedStorageLocationEvent in L
        // action: none
        DeletedStorageLocationEvent localDeletedStorageLocationEvent =
            this.deletedStorageLocationEventDAO.get(remote.getID());
        if (localDeletedStorageLocationEvent != null) {
            log.info(String.format("Artifact not in L && in R - Explanation2: DeletedStorageLocationEvent in L\n"
                                       + "action: no action necessary"
                                       + "\nremote - %s\nlocal DeletedStorageLocationEvent - %s",
                                   remote, localDeletedStorageLocationEvent));
            return;
        }

        // explanation4: L==global, new Artifact in R, pending/missed changed Artifact event in L
        // evidence: Artifact in local db but siteLocations does not include remote siteID
        // action: add siteID to Artifact.siteLocations
        if (this.trackSiteLocations) {
            Artifact local = this.artifactDAO.get(remote.getID());
            if (local != null) {
                StorageSite remoteStorageSite = getRemoteStorageSite();
                SiteLocation remoteSiteLocation = new SiteLocation(remoteStorageSite.getID());
                if (!local.siteLocations.contains(remoteSiteLocation)) {
                    log.info(String.format("Artifact not in L && in R - Explanation4: Artifact in local db but "
                                               + "siteLocations does not include remote siteID\n"
                                               + "action: add siteID to Artifact.siteLocations\n"
                                               + "remote - %s", remote));
                    return;
                }
            }
        }

        // explanation3: L==storage, new Artifact in R, pending/missed new Artifact event in L
        // evidence: ?
        // action: insert Artifact
        log.info(String.format("Artifact not in L && in R - Explanation3,0,6,7\naction: insert Artifact\n"
                                   + "remote - %s", remote));
    }

    /**
     * discrepancy: artifact.uri in both && artifact.id mismatch (collision)
     *
     * <p>explantion1: same ID collision due to race condition that metadata-sync has to handle
     * evidence: no more evidence needed
     * action: pick winner, create DeletedArtifactEvent for loser, delete loser if it is in L, insert winner if winner was in R
     *
     * <p>discrepancy: artifact in both && valid metaChecksum mismatch
     *
     * <p>explanation1: pending/missed artifact update in L
     * evidence: ??
     * action: put Artifact
     *
     * <p>explanation2: pending/missed artifact update in R
     * evidence: ??
     * action: do nothing
     *
     * @param local     The local Artifact.
     * @param remote    The remote Artifact.
     */
    protected void validateLocalAndRemote(Artifact local, Artifact remote) {
        if (local.getID().equals(remote.getID())) {
            log.info(String.format("ID's equal: - %s", local));
        } else if (!local.getID().equals(remote.getID())) {
            log.info(String.format("ID mismatch:\npick winner\nuri - %s\n local - %s %s\nremote - %s %s",
                                   local.getURI(),
                                   local.getID(), dateFormat.format(local.getLastModified()),
                                   remote.getID(), dateFormat.format(remote.getLastModified())));
        } else if (!local.getMetaChecksum().equals(remote.getMetaChecksum())) {
            log.info(String.format("MetaChecksum mismatch:\n uri - %s %s\n local - %s %s\nremote - %s %s",
                                   local.getURI(), local.getID(),
                                   local.getMetaChecksum(), dateFormat.format(local.getLastModified()),
                                   remote.getMetaChecksum(), dateFormat.format(remote.getLastModified())));
        }
    }


    /**
     * Get a remote Artifact
     */
    Artifact getRemoteArtifact(URI uri)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final TapClient<Artifact> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("SELECT id, uri, contentChecksum, contentLastModified, contentLength, "
                                               + "contentType, contentEncoding, lastModified, metaChecksum "
                                               + "FROM inventory.Artifact WHERE uri='%s'", uri.toASCIIString());
        log.debug("\nExecuting query '" + query + "'\n");
        Artifact returned = null;
        ResourceIterator<Artifact> results = tapClient.execute(query, new ArtifactRowMapper());
        if (results.hasNext()) {
            returned = results.next();
        }
        return returned;
    }

    /**
     * Get a remote DeletedArtifactEvent
     */
    DeletedArtifactEvent getRemoteDeletedArtifactEvent(UUID id)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final TapClient<DeletedArtifactEvent> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("SELECT id, lastModified, metaChecksum FROM inventory.DeletedArtifactEvent "
                                               + "WHERE id=%s", id);
        log.debug("\nExecuting query '" + query + "'\n");
        DeletedArtifactEvent returned = null;
        ResourceIterator<DeletedArtifactEvent> results =
            tapClient.execute(query, new DeletedArtifactEventRowMapper());
        if (results.hasNext()) {
            returned = results.next();
        }
        return returned;
    }

    /**
     * Get a remote DeletedStorageLocalEvent
     */
    DeletedStorageLocationEvent getRemoteDeletedStorageLocationEvent(UUID id)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final TapClient<DeletedStorageLocationEvent> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("SELECT id, lastModified, metaChecksum "
                                               + "FROM inventory.DeletedStorageLocationEvent WHERE id=%s", id);
        log.debug("\nExecuting query '" + query + "'\n");
        DeletedStorageLocationEvent returned = null;
        ResourceIterator<DeletedStorageLocationEvent> results =
            tapClient.execute(query, new DeletedStorageLocationEventRowMapper());
        if (results.hasNext()) {
            returned = results.next();
        }
        return returned;
    }

    /**
     * Get the StorageSite for the remote instance resourceID;
     */
    StorageSite getRemoteStorageSite()
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final TapClient<StorageSite> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("SELECT id, resourceID, name, allowRead, allowWrite, lastModified, "
                                               + "metaChecksum FROM inventory.StorageSite where resourceID = %s",
                                           this.resourceID);
        log.debug("\nExecuting query '" + query + "'\n");
        StorageSite returned = null;
        ResourceIterator<StorageSite> results =
            tapClient.execute(query, new StorageSiteRowMapper());
        if (results.hasNext()) {
            returned = results.next();
        }
        return returned;
    }

    /**
     * Class to map the query results to an Artifact.
     */
    static class ArtifactRowMapper implements TapRowMapper<Artifact> {

        @Override
        public Artifact mapRow(final List<Object> row) {
            int index = 0;
            final UUID id = (UUID) row.get(index++);
            final URI uri = (URI) row.get(index++);
            final URI contentChecksum = (URI) row.get(index++);
            final Date contentLastModified = (Date) row.get(index++);
            final Long contentLength = (Long) row.get(index++);

            final Artifact artifact = new Artifact(id, uri, contentChecksum, contentLastModified, contentLength);
            artifact.contentType = (String) row.get(index++);
            artifact.contentEncoding = (String) row.get(index++);
            InventoryUtil.assignLastModified(artifact, (Date) row.get(index++));
            InventoryUtil.assignMetaChecksum(artifact, (URI) row.get(index));
            return artifact;
        }
    }

    /**
     * Class to map the query results to a DeletedArtifactEvent.
     */
    static class DeletedArtifactEventRowMapper implements TapRowMapper<DeletedArtifactEvent> {

        @Override
        public DeletedArtifactEvent mapRow(List<Object> row) {
            int index = 0;
            final UUID id = (UUID) row.get(index++);

            final DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(id);
            InventoryUtil.assignLastModified(deletedArtifactEvent, (Date) row.get(index++));
            InventoryUtil.assignMetaChecksum(deletedArtifactEvent, (URI) row.get(index));
            return deletedArtifactEvent;
        }
    }

    /**
     * Class to map the query results to a DeletedStorageLocationEvent.
     */
    static class DeletedStorageLocationEventRowMapper implements TapRowMapper<DeletedStorageLocationEvent> {

        @Override
        public DeletedStorageLocationEvent mapRow(List<Object> row) {
            int index = 0;
            final UUID id = (UUID) row.get(index++);

            final DeletedStorageLocationEvent deletedStorageLocationEvent = new DeletedStorageLocationEvent(id);
            InventoryUtil.assignLastModified(deletedStorageLocationEvent, (Date) row.get(index++));
            InventoryUtil.assignMetaChecksum(deletedStorageLocationEvent, (URI) row.get(index));
            return deletedStorageLocationEvent;
        }
    }

    /**
     * Class to map the query results to a StorageSite.
     */
    static class StorageSiteRowMapper implements TapRowMapper<StorageSite> {

        @Override
        public StorageSite mapRow(List<Object> row) {
            int index = 0;
            final UUID id = (UUID) row.get(index++);
            final URI resourceID = (URI) row.get(index++);
            final String name = (String) row.get(index++);
            final boolean allowRead = (Boolean) row.get(index++);
            final boolean allowWrite = (Boolean) row.get(index);

            final StorageSite storageSite = new StorageSite(id, resourceID, name, allowRead, allowWrite);
            InventoryUtil.assignLastModified(storageSite, (Date) row.get(index++));
            InventoryUtil.assignMetaChecksum(storageSite, (URI) row.get(index));
            return storageSite;
        }
    }

}


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
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.DeletedStorageLocationEventDAO;
import org.opencadc.inventory.query.ArtifactQuery;
import org.opencadc.inventory.query.DeletedArtifactEventQuery;
import org.opencadc.inventory.query.DeletedStorageLocationEventQuery;
import org.opencadc.inventory.query.StorageSiteQuery;

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
     * discrepancy: artifact in L AND artifact not in R
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
        log.debug("checking explanation 0");
        Artifact remote = getRemoteArtifact(local.getURI());
        if (remote != null) {
            log.info(String.format("Artifact in L && not in R - Explanation0: Artifact in R without filter\n"
                                       + "action: delete Artifact, if (L==storage) create DeletedStorageLocationEvent\n"
                                       + " local - %s\nremote - %s", local, remote));
            //TODO do action
            return;
        }

        // explanation1: deleted from R, pending/missed DeletedArtifactEvent in L
        // evidence: DeletedArtifactEvent in R
        // action: put DAE, delete artifact
        log.debug("checking explanation 1");
        DeletedArtifactEvent remoteDeletedArtifactEvent = getRemoteDeletedArtifactEvent(local.getID());
        if (remoteDeletedArtifactEvent != null) {
            log.info(String.format("Artifact in L && not in R - Explanation1: DeletedArtifactEvent in R\n"
                                       + "action: put DeletedArtifactEvent, delete Artifact\n"
                                       + " local Artifact: %s\nremote DeletedArtifactEvent: %s",
                                   local, remoteDeletedArtifactEvent));
            //TODO do action
            return;
        }

        // 2. DeletedStorageLocationEvent in R
        // remove siteID from Artifact.storageLocations
        log.debug("checking explanation 2");
        if (this.trackSiteLocations) {
            DeletedStorageLocationEvent remoteDeletedStorageLocationEvent =
                getRemoteDeletedStorageLocationEvent(local.getID());
            if (remoteDeletedStorageLocationEvent != null) {
                log.info(String.format("Artifact in L && not in R - Explanation2: DeletedStorageLocationEvent in R\n"
                                           + "action: remove siteID from Artifact.storageLocations\n"
                                           + " local Artifact: %s\nremote DeletedStorageLocationEvent: %s",
                                       local, remoteDeletedStorageLocationEvent));
                //TODO do action
                return;
            }
        }

        // explanation3: L==global, new Artifact in L, pending/missed Artifact or sync in R
        // also
        // explanation4: L==storage, new Artifact in L, pending/missed new Artifact event in R
        // explanation6: deleted from R, lost DeletedArtifactEvent
        // explanation7: L==global, lost DeletedStorageLocationEvent
        // evidence: ?
        // action: remove siteID from Artifact.storageLocations
        log.debug("explanation 3");
        log.info(String.format("Artifact in L && not in R - Explanation3,4,6,7\n"
                                   + "remove siteID from Artifact.storageLocations\nlocal Artifact: %s", local));
        //TODO do action
    }

    /**
     * discrepancy: artifact not in L AND artifact in R
     *
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
        log.debug("checking explanation 1");
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
        log.debug("checking explanation 2");
        if (!this.trackSiteLocations) {
            DeletedStorageLocationEvent localDeletedStorageLocationEvent = this.deletedStorageLocationEventDAO.get(remote.getID());
            if (localDeletedStorageLocationEvent != null) {
                log.info(String.format("Artifact not in L && in R - Explanation2: DeletedStorageLocationEvent in L\n"
                                           + "action: no action necessary"
                                           + "\nremote - %s\nlocal DeletedStorageLocationEvent - %s",
                                       remote, localDeletedStorageLocationEvent));
                return;
            }
        }

        // explanation3: L==storage, new Artifact in R, pending/missed new Artifact event in L
        // also
        // explanation0: filter policy at L changed to include artifact in R
        // explanation6: deleted from L, lost DeletedArtifactEvent
        // explanation7: L==storage, deleted from L, lost DeletedStorageLocationEvent
        // evidence: ?
        // action: insert Artifact
        log.debug("checking explanation 3");
        if (!this.trackSiteLocations) {
            Artifact local = this.artifactDAO.get(remote.getID());
            if (local == null) {
                log.info(String.format("Artifact not in L && in R - Explanation0,3,6,7\naction: insert Artifact\n"
                                            + "remote - %s",
                                        remote));
                //TODO do action
                return;
            }
        }

        // explanation4: L==global, new Artifact in R, pending/missed changed Artifact event in L
        // evidence: Artifact in local db but siteLocations does not include remote siteID
        // action: add siteID to Artifact.siteLocations
        log.debug("checking explanation 4");
        if (this.trackSiteLocations) {
            Artifact local = this.artifactDAO.get(remote.getID());
            if (local != null) {
                StorageSite remoteStorageSite = getRemoteStorageSite();
                SiteLocation remoteSiteLocation = new SiteLocation(remoteStorageSite.getID());
                if (!local.siteLocations.contains(remoteSiteLocation)) {
                    log.info(String.format("Artifact not in L && in R - Explanation4: Artifact in local db but "
                                               + "siteLocations does not include remote siteID\n"
                                               + "action: add siteID to Artifact.siteLocations\n"
                                               + "remote - %s\nremote StorageSite - %s", remote, remoteStorageSite));
                    //TODO do action
                    return;
                }
            }
        }

    }

    /**
     * Artifact in local AND remote
     *
     * @param local     The local Artifact.
     * @param remote    The remote Artifact.
     */
    protected void validateLocalAndRemote(Artifact local, Artifact remote) {
        // discrepancy: artifact.uri in both && artifact.id mismatch (collision)
        // explanation1: same ID collision due to race condition that metadata-sync has to handle
        // evidence: no more evidence needed
        // action: pick winner, create DeletedArtifactEvent for loser, delete loser if it is in L, insert winner if winner was in R
        log.debug("checking artifact.id mismatch");
        if (!local.getID().equals(remote.getID())) {
            log.info(String.format("ID mismatch:\npick winner\nuri - %s\n local - %s %s\nremote - %s %s",
                                   local.getURI(),
                                   local.getID(), dateFormat.format(local.getLastModified()),
                                   remote.getID(), dateFormat.format(remote.getLastModified())));
            if (local.getContentLastModified().before(remote.getContentLastModified())) {
                //TODO do action
            }
            return;
        }

        // discrepancy: artifact in both && valid metaChecksum mismatch
        log.debug("checking valid metaChecksum mismatch");
        if (!local.getMetaChecksum().equals(remote.getMetaChecksum())) {
            log.info(String.format("MetaChecksum mismatch:\n uri - %s %s\n local - %s %s\nremote - %s %s",
                                   local.getURI(), local.getID(), local.getMetaChecksum(),
                                   dateFormat.format(local.getLastModified()), remote.getMetaChecksum(),
                                   dateFormat.format(remote.getLastModified())));
            // explanation1: pending/missed artifact update in L
            // evidence: ??
            // action: put Artifact
            //TODO determine action

            // explanation2: pending/missed artifact update in R
            // evidence: ??
            // action: do nothing
            //TODO determine action
            return;
        }

        // Local & remote Artifact's have the same ID.
        if (local.getID().equals(remote.getID())) {
            log.info(String.format("Valid: - %s", local));
        }
    }

    /**
     * Get a remote Artifact
     */
    Artifact getRemoteArtifact(URI uri)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final String where = String.format("WHERE uri = '%s'", uri.toASCIIString());
        ArtifactQuery artifactQuery = new ArtifactQuery();
        ResourceIterator<Artifact> results = artifactQuery.execute(where, this.resourceID);
        if (results.hasNext()) {
            return results.next();
        }
        return null;
    }

    /**
     * Get a remote DeletedArtifactEvent
     */
    DeletedArtifactEvent getRemoteDeletedArtifactEvent(UUID id)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final String where = String.format("WHERE id = '%s'", id);
        DeletedArtifactEventQuery deletedArtifactEventQuery = new DeletedArtifactEventQuery();
        ResourceIterator<DeletedArtifactEvent> results = deletedArtifactEventQuery.execute(where, this.resourceID);
        if (results.hasNext()) {
            return results.next();
        }
        return null;
    }

    /**
     * Get a remote DeletedStorageLocalEvent
     */
    DeletedStorageLocationEvent getRemoteDeletedStorageLocationEvent(UUID id)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final String where = String.format("WHERE id = '%s'", id);
        DeletedStorageLocationEventQuery deletedStorageLocationEventQuery = new DeletedStorageLocationEventQuery();
        ResourceIterator<DeletedStorageLocationEvent> results =
            deletedStorageLocationEventQuery.execute(where, this.resourceID);
        if (results.hasNext()) {
            return results.next();
        }
        return null;
    }

    /**
     * Get the StorageSite for the remote instance resourceID;
     */
    StorageSite getRemoteStorageSite()
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final String where = String.format("WHERE resourceID = '%s'", this.resourceID);
        StorageSiteQuery storageSiteQuery = new StorageSiteQuery();
        ResourceIterator<StorageSite> results = storageSiteQuery.execute(where, this.resourceID);
        if (results.hasNext()) {
            return results.next();
        }
        return null;
    }

}


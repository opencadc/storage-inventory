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

import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import java.io.IOException;
import java.net.URI;
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
import org.opencadc.inventory.db.EntityNotFoundException;
import org.opencadc.inventory.query.ArtifactRowMapper;
import org.opencadc.inventory.query.DeletedArtifactEventRowMapper;
import org.opencadc.inventory.query.DeletedStorageLocationEventRowMapper;
import org.opencadc.tap.TapClient;

/**
 * Class that compares and validates two Artifacts, repairing the local Artifact to
 * resolve any discrepancies.
 */
public class ArtifactValidator {
    private static final Logger log = Logger.getLogger(ArtifactValidator.class);

    private final ArtifactDAO artifactDAO;
    private final URI resourceID;
    private final StorageSite remoteSite;

    private final DeletedArtifactEventDAO deletedArtifactEventDAO;
    private final DeletedStorageLocationEventDAO deletedStorageLocationEventDAO;
    private final TransactionManager transactionManager;

    /**
     * Constructor
     *
     * @param artifactDAO   local inventory database.
     * @param resourceID    identifier for the remote query service
     * @param remoteSite    identifier for remote file service, null when local is a storage site
     */
    public ArtifactValidator(ArtifactDAO artifactDAO, URI resourceID, StorageSite remoteSite) {
        this.artifactDAO = artifactDAO;
        this.resourceID = resourceID;
        this.remoteSite = remoteSite;

        this.transactionManager = this.artifactDAO.getTransactionManager();
        this.deletedArtifactEventDAO = new DeletedArtifactEventDAO(this.artifactDAO);
        this.deletedStorageLocationEventDAO = new DeletedStorageLocationEventDAO(this.artifactDAO);
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
            try {
                log.debug("starting transaction");
                this.transactionManager.startTransaction();
                log.debug("start txn: OK");

                this.artifactDAO.lock(local);
                log.info(String.format("delete: %s %s reason: local filter policy change",
                                       local.getID(), local.getURI()));
                this.artifactDAO.delete(local.getID());

                if (this.remoteSite == null) {
                    DeletedStorageLocationEvent deletedStorageLocationEvent =
                        new DeletedStorageLocationEvent(local.getID());
                    log.info(String.format("put %s reason: filter policy change excludes remote",
                                           deletedStorageLocationEvent));
                    this.deletedStorageLocationEventDAO.put(deletedStorageLocationEvent);
                }

                log.debug("committing transaction");
                this.transactionManager.commitTransaction();
                log.debug("commit txn: OK");
            } catch (EntityNotFoundException e) {
                log.debug(String.format("skip: %s %s reason: stale local Artifact",
                                           local.getID(), local.getURI()));
                this.transactionManager.rollbackTransaction();
            } catch (Exception e) {
                log.error(String.format("failed to delete %s %s", local.getID(), local.getURI()), e);
                this.transactionManager.rollbackTransaction();
                log.debug("rollback txn: OK");
            } finally {
                if (this.transactionManager.isOpen()) {
                    log.error("BUG - open transaction in finally");
                    this.transactionManager.rollbackTransaction();
                    log.error("rollback txn: OK");
                }
            }
            return;
        }

        // explanation1: deleted from R, pending/missed DeletedArtifactEvent in L
        // evidence: DeletedArtifactEvent in R
        // action: put DAE, delete artifact
        log.debug("checking explanation 1");
        DeletedArtifactEvent remoteDeletedArtifactEvent = getRemoteDeletedArtifactEvent(local.getID());
        if (remoteDeletedArtifactEvent != null) {
            try {
                log.debug("starting transaction");
                this.transactionManager.startTransaction();
                log.debug("start txn: OK");

                this.artifactDAO.lock(local);
                log.info(String.format("delete: %s %s reason: found remote DeletedArtifactEvent",
                                       local.getID(), local.getURI()));
                this.deletedArtifactEventDAO.put(remoteDeletedArtifactEvent);
                this.artifactDAO.delete(local.getID());

                log.debug("committing transaction");
                this.transactionManager.commitTransaction();
                log.debug("commit txn: OK");
            } catch (EntityNotFoundException e) {
                log.debug(String.format("skip: %s %s reason: stale local Artifact",
                                        local.getID(), local.getURI()));
                this.transactionManager.rollbackTransaction();
            } catch (Exception e) {
                log.error(String.format("failed to delete %s %s", local.getID(), local.getURI()), e);
                this.transactionManager.rollbackTransaction();
                log.debug("rollback txn: OK");
            } finally {
                if (this.transactionManager.isOpen()) {
                    log.error("BUG - open transaction in finally");
                    this.transactionManager.rollbackTransaction();
                    log.error("rollback txn: OK");
                }
            }
            return;
        }

        // explanation2: L==global, deleted from R, pending/missed DeletedStorageLocationEvent in L
        // evidence: DeletedStorageLocationEvent in R
        // action: remove siteID from Artifact.storageLocations
        log.debug("checking explanation 2");
        if (this.remoteSite != null) {
            DeletedStorageLocationEvent remoteDeletedStorageLocationEvent =
                getRemoteDeletedStorageLocationEvent(local.getID());
            if (remoteDeletedStorageLocationEvent != null) {
                SiteLocation remoteSiteLocation = new SiteLocation(this.remoteSite.getID());
                try {
                    log.debug("starting transaction");
                    this.transactionManager.startTransaction();
                    log.debug("start txn: OK");

                    this.artifactDAO.lock(local);
                    Artifact current = this.artifactDAO.get(local.getID());
                    if (current.siteLocations.contains(remoteSiteLocation)) {
                        // if siteLocations becomes empty removing the remote siteLocation, delete the artifact
                        if (current.siteLocations.size() == 1) {
                            log.info(String.format("delete: %s %s reason: empty SiteLocations",
                                                   current.getID(), current.getURI()));
                            this.artifactDAO.delete(current.getID());
                        } else {
                            log.info(String.format("remove %s for %s %s reason: found remote %s",
                                                   remoteSiteLocation, current.getID(), current.getURI(),
                                                   remoteDeletedStorageLocationEvent));
                            this.artifactDAO.removeSiteLocation(current, remoteSiteLocation);
                        }
                    }

                    log.debug("committing transaction");
                    this.transactionManager.commitTransaction();
                    log.debug("commit txn: OK");
                } catch (EntityNotFoundException e) {
                    log.debug(String.format("skip: %s %s reason: stale local Artifact",
                                            local.getID(), local.getURI()));
                    this.transactionManager.rollbackTransaction();
                } catch (Exception e) {
                    log.error(String.format("failed to delete %s in Artifact %s %s",
                                            remoteSiteLocation, local.getID(), local.getURI()), e);
                    this.transactionManager.rollbackTransaction();
                    log.debug("rollback txn: OK");
                } finally {
                    if (this.transactionManager.isOpen()) {
                        log.error("BUG - open transaction in finally");
                        this.transactionManager.rollbackTransaction();
                        log.error("rollback txn: OK");
                    }
                }
                return;
            }
        }

        if (this.remoteSite != null) {
            // explanation3: L==global, new Artifact in L, pending/missed Artifact or sync in R
            // also
            // explanation6: deleted from R, lost DeletedArtifactEvent
            // explanation7: L==global, lost DeletedStorageLocationEvent
            // evidence: ?
            // action: remove siteID from Artifact.storageLocations
            log.debug("explanation 3");
            SiteLocation remoteSiteLocation = new SiteLocation(this.remoteSite.getID());
            try {
                log.debug("starting transaction");
                this.transactionManager.startTransaction();
                log.debug("start txn: OK");

                this.artifactDAO.lock(local);
                Artifact current = this.artifactDAO.get(local.getID());
                if (current.siteLocations.contains(remoteSiteLocation)) {
                    // if siteLocation's becomes empty removing the siteLocation, delete the artifact
                    if (current.siteLocations.size() == 1) {
                        log.info(String.format("delete: %s %s reason: empty SiteLocations",
                                               current.getID(), current.getURI()));
                        this.artifactDAO.delete(current.getID());
                    } else {
                        log.info(String.format("remove SiteLocation: %s %s reason: multiple",
                                               current.getID(), current.getURI()));
                        this.artifactDAO.removeSiteLocation(current, remoteSiteLocation);
                    }
                }

                log.debug("committing transaction");
                this.transactionManager.commitTransaction();
                log.debug("commit txn: OK");
            } catch (EntityNotFoundException e) {
                log.debug(String.format("skip: %s %s reason: stale local Artifact",
                                        local.getID(), local.getURI()));
                this.transactionManager.rollbackTransaction();
            } catch (Exception e) {
                log.error(String.format("failed to delete %s in Artifact %s %s",
                                        remoteSiteLocation, local.getID(), local.getURI()), e);
                this.transactionManager.rollbackTransaction();
                log.debug("rollback txn: OK");
            } finally {
                if (this.transactionManager.isOpen()) {
                    log.error("BUG - open transaction in finally");
                    this.transactionManager.rollbackTransaction();
                    log.error("rollback txn: OK");
                }
            }
        } else {
            // explanation4: L==storage, new Artifact in L, pending/missed new Artifact event in R
            // action: none
            log.debug("explanation 4");
            logNoAction(local,"pending/missed new Artifact event in remote");
        }
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
            logNoAction(remote, "found local DeletedArtifactEvent");
            return;
        }

        // explanation2: L==storage, deleted from L, pending/missed DeletedStorageLocationEvent in R
        // evidence: DeletedStorageLocationEvent in L
        // action: none
        log.debug("checking explanation 2");
        if (this.remoteSite == null) {
            DeletedStorageLocationEvent localDeletedStorageLocationEvent
                = this.deletedStorageLocationEventDAO.get(remote.getID());
            if (localDeletedStorageLocationEvent != null) {
                logNoAction(remote, "found local DeletedStorageLocationEvent");
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
        if (this.remoteSite == null) {
            try {
                //log.debug("starting transaction");
                //this.transactionManager.startTransaction();
                //log.debug("start txn: OK");

                log.info(String.format("put: %s %s reason: pending/missed Artifact", remote.getID(), remote.getURI()));
                this.artifactDAO.put(remote);

                //log.debug("committing transaction");
                //this.transactionManager.commitTransaction();
                //log.debug("commit txn: OK");
            } catch (Exception e) {
                log.error(String.format("failed to put %s %s", remote.getID(), remote.getURI()), e);
                this.transactionManager.rollbackTransaction();
                log.debug("rollback txn: OK");
            } finally {
                if (this.transactionManager.isOpen()) {
                    log.error("BUG - open transaction in finally");
                    this.transactionManager.rollbackTransaction();
                    log.error("rollback txn: OK");
                }
            }
            return;
        }

        // explanation4: L==global, new Artifact in R, pending/missed changed Artifact event in L
        // also
        // explanation0: filter policy at L changed to include artifact in R
        // explanation6: deleted from L, lost DeletedArtifactEvent
        // evidence: ?
        // action: insert Artifact or add siteID to Artifact.siteLocations
        log.debug("checking explanation 4");
        if (this.remoteSite != null) {
            SiteLocation remoteSiteLocation = new SiteLocation(this.remoteSite.getID());
            Artifact local = this.artifactDAO.get(remote.getID());
            if (local != null) {
                
                try {
                    log.debug("starting transaction");
                    this.transactionManager.startTransaction();
                    log.debug("start txn: OK");

                    this.artifactDAO.lock(local);
                    Artifact current = this.artifactDAO.get(local.getID());
                    if (!current.siteLocations.contains(remoteSiteLocation)) {
                        log.info(String.format("add: %s to %s %s reason: remote SiteLocation missing",
                                               remoteSiteLocation, current.getID(), current.getURI()));
                        this.artifactDAO.addSiteLocation(current, remoteSiteLocation);
                    }

                    log.debug("committing transaction");
                    this.transactionManager.commitTransaction();
                    log.debug("commit txn: OK");
                } catch (EntityNotFoundException e) {
                    log.debug(String.format("skip: %s %s reason: stale local Artifact",
                                            local.getID(), local.getURI()));
                    this.transactionManager.rollbackTransaction();
                } catch (Exception e) {
                    log.error(String.format("failed to put %s %s", local.getID(), local.getURI()), e);
                    this.transactionManager.rollbackTransaction();
                    log.debug("rollback txn: OK");
                } finally {
                    if (this.transactionManager.isOpen()) {
                        log.error("BUG - open transaction in finally");
                        this.transactionManager.rollbackTransaction();
                        log.error("rollback txn: OK");
                    }
                }
            } else {
                //log.debug("starting transaction");
                //this.transactionManager.startTransaction();
                //log.debug("start txn: OK");

                log.info(String.format("put: %s %s reason: pending/missed Artifact", remote.getID(), remote.getURI()));
                remote.siteLocations.add(remoteSiteLocation);
                this.artifactDAO.put(remote);

                //log.debug("committing transaction");
                //this.transactionManager.commitTransaction();
                //log.debug("commit txn: OK");
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
        // action: pick winner, create DeletedArtifactEvent for loser, delete loser if it is in L,
        //         insert winner if winner was in R
        //         winner in local:  DeletedArtifactEvent for remote
        //         winner in remote: DeletedArtifactEvent for local, delete local, insert remote
        log.debug("checking artifact.id mismatch");
        if (!local.getID().equals(remote.getID())) {
            try {
                log.debug("starting transaction");
                this.transactionManager.startTransaction();
                log.debug("start txn: OK");

                this.artifactDAO.lock(local);
                if (local.getContentLastModified().before(remote.getContentLastModified())) {
                    DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(local.getID());
                    log.info(String.format(
                        "resolve Artifact.id collision: delete local %s %s, put deletedArtifactEvent, put remote %s %s "
                            + "reason: local contentLastModified older than remote",
                        local.getID(), local.getURI(), remote.getID(), remote.getURI()));
                    this.deletedArtifactEventDAO.put(deletedArtifactEvent);
                    this.artifactDAO.delete(local.getID());
                    this.artifactDAO.put(remote);
                } else {
                    log.info(String.format(
                        "resolve Artifact.id collision: put DeletedArtifactEvent for remote %s %s "
                            + "reason: local contentLastModified newer than remote",
                        remote.getID(), remote.getURI()));
                    DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(remote.getID());
                    this.deletedArtifactEventDAO.put(deletedArtifactEvent);
                }

                log.debug("committing transaction");
                this.transactionManager.commitTransaction();
                log.debug("commit txn: OK");
            } catch (EntityNotFoundException e) {
                log.debug(String.format("skip: %s %s reason: stale local Artifact",
                                        local.getID(), local.getURI()));
                this.transactionManager.rollbackTransaction();
            } catch (Exception e) {
                log.error(String.format("failed to resolve Artifact.id collision for local %s %s, remote %s %s",
                                        local.getID(), local.getURI(), remote.getID(), remote.getURI()), e);
                this.transactionManager.rollbackTransaction();
                log.debug("rollback txn: OK");
            } finally {
                if (this.transactionManager.isOpen()) {
                    log.error("BUG - open transaction in finally");
                    this.transactionManager.rollbackTransaction();
                    log.error("rollback txn: OK");
                }
            }
            return;
        }

        // discrepancy: artifact in both && valid metaChecksum mismatch
        log.debug("checking valid metaChecksum mismatch");
        if (!local.getMetaChecksum().equals(remote.getMetaChecksum())) {
            if (local.getLastModified().before(remote.getLastModified())) {
                // explanation1: pending/missed artifact update in L
                // evidence: local artifact has older Entity.lastModified indicating an update to
                //           optional metadata at remote
                // action: put Artifact
                try {
                    log.debug("starting transaction");
                    this.transactionManager.startTransaction();
                    log.debug("start txn: OK");
                    this.artifactDAO.lock(local);
                    Artifact current = this.artifactDAO.get(local.getID());
                    if (!current.getMetaChecksum().equals(remote.getMetaChecksum())) {
                        if (current.getLastModified().before(remote.getLastModified())) {
                            log.info(String.format("resolve Artifact.metaChecksum mismatch: put remote %s %s "
                                                       + "reason: remote lastModified newer than local",
                                                   remote.getID(), remote.getURI()));
                            if (this.remoteSite == null) {
                                // storage site: keep StorageLocation
                                remote.storageLocation = local.storageLocation;
                            } else {
                                // global site: merge SiteLocation(s)
                                remote.siteLocations.add(new SiteLocation(this.remoteSite.getID()));
                                remote.siteLocations.addAll(local.siteLocations);
                            }
                            this.artifactDAO.put(remote);
                        } else {
                            // same as explanation2 below
                            log.info("resolve Artifact.metaChecksum mismatch: no action, "
                                         + "reason: local lastModified newer than remote");
                        }
                    }

                    log.debug("committing transaction");
                    this.transactionManager.commitTransaction();
                    log.debug("commit txn: OK");
                } catch (EntityNotFoundException e) {
                    log.debug(String.format("skip: %s %s reason: stale local Artifact",
                                            local.getID(), local.getURI()));
                    this.transactionManager.rollbackTransaction();
                } catch (Exception e) {
                    log.error(String.format("failed to put Artifact %s %s", local.getID(), local.getURI()), e);
                    this.transactionManager.rollbackTransaction();
                    log.debug("rollback txn: OK");
                } finally {
                    if (this.transactionManager.isOpen()) {
                        log.error("BUG - open transaction in finally");
                        this.transactionManager.rollbackTransaction();
                        log.error("rollback txn: OK");
                    }
                }
            } else {
                // explanation2: pending/missed artifact update in R
                // evidence: local artifact has newer Entity.lastModified indicating the update happened locally
                // action: do nothing
                log.info("resolve Artifact.metaChecksum mismatch: no action, "
                             + "reason: local lastModified newer than remote");
            }
        }

    }

    /**
     * Get a remote Artifact
     */
    Artifact getRemoteArtifact(URI uri)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {

        final TapClient<Artifact> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("%s WHERE uri = '%s'", ArtifactRowMapper.BASE_QUERY, uri.toASCIIString());
        log.debug("\nExecuting query '" + query + "'\n");
        ResourceIterator<Artifact> results = tapClient.execute(query, new ArtifactRowMapper());
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

        final TapClient<DeletedArtifactEvent> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("%s WHERE id = '%s'", DeletedArtifactEventRowMapper.BASE_QUERY, id);
        log.debug("\nExecuting query '" + query + "'\n");
        ResourceIterator<DeletedArtifactEvent> results = tapClient.execute(query, new DeletedArtifactEventRowMapper());
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

        final TapClient<DeletedStorageLocationEvent> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("%s WHERE id = '%s'", DeletedStorageLocationEventRowMapper.BASE_QUERY, id);
        log.debug("\nExecuting query '" + query + "'\n");
        ResourceIterator<DeletedStorageLocationEvent> results =
            tapClient.execute(query, new DeletedStorageLocationEventRowMapper());
        if (results.hasNext()) {
            return results.next();
        }
        return null;
    }

    private void logNoAction(Artifact artifact, String message) {
        log.info(String.format("no action %s %s, reason: %s", artifact.getID(), artifact.getURI(), message));
    }

}


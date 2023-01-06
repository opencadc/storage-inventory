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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.ratik;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.TransactionManager;
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
import org.opencadc.inventory.query.ArtifactRowMapper;
import org.opencadc.inventory.query.DeletedArtifactEventRowMapper;
import org.opencadc.inventory.util.ArtifactSelector;
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
    private final StorageSite remoteSite;
    private final ArtifactSelector artifactSelector;
    private Date raceConditionStart;

    private final DeletedArtifactEventDAO deletedArtifactEventDAO;
    private final DeletedStorageLocationEventDAO deletedStorageLocationEventDAO;
    private final TransactionManager transactionManager;
    
    private final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

    /**
     * Constructor
     *
     * @param artifactDAO   local inventory database.
     * @param resourceID    identifier for the remote query service
     * @param remoteSite    identifier for remote file service, null when local is a storage site
     * @param artifactSelector selection policy implementation
     */
    public ArtifactValidator(ArtifactDAO artifactDAO, URI resourceID, StorageSite remoteSite,
                             ArtifactSelector artifactSelector) {
        this.artifactDAO = artifactDAO;
        this.resourceID = resourceID;
        this.remoteSite = remoteSite;
        this.artifactSelector = artifactSelector;

        this.transactionManager = this.artifactDAO.getTransactionManager();
        this.deletedArtifactEventDAO = new DeletedArtifactEventDAO(this.artifactDAO);
        this.deletedStorageLocationEventDAO = new DeletedStorageLocationEventDAO(this.artifactDAO);
    }

    /**
     * Must be called before validate(...).
     * 
     * @param raceConditionStart events from queries after this date subject to race conditions 
     */
    public void setRaceConditionStart(Date raceConditionStart) {
        this.raceConditionStart = raceConditionStart;
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

        // explanation0: filter policy at L excludes artifact in R
        // evidence: R uses a filter policy AND Artifact in R without filter AND remoteArtifact.lastModified < remoteQueryStart
        // if (L==global) delete Artifact, if (L==storage) delete Artifact only if
        // remote has multiple copies and create DeletedStorageLocationEvent
        log.debug("checking explanation 0");
        if (this.artifactSelector.getConstraint() != null) {
            ArtifactQueryResult queryResult = getRemoteArtifactQueryResult(local.getID());
            if (queryResult != null && queryResult.artifact != null) {
                Artifact remote = queryResult.artifact;
                int numCopies = 0;
                // if L == storage, get the Artifact count from global
                if (this.remoteSite == null) {
                    if (queryResult.numCopies != null) {
                        numCopies = queryResult.numCopies;
                    }
                }
                //log.warn("raceConditionStart=" + df.format(raceConditionStart)
                //        + " remote.lastModified=" + df.format(remote.getLastModified()));
                if (raceConditionStart.before(queryResult.artifact.getLastModified())) {
                    log.info(String.format("ArtifactValidator.deleteArtifact-delayed Artifact.id=%s Artifact.uri=%s"
                        + " reason=local-filter-policy-exclude-race-condition", local.getID(), local.getURI()));
                    
                } else {
                    try {
                        log.debug("starting transaction");
                        this.transactionManager.startTransaction();
                        log.debug("start txn: OK");

                        Artifact current = this.artifactDAO.lock(local);
                        if (current != null) {
                            if (this.remoteSite != null) {
                                // if L==global, delete Artifact, do not create a DeletedStorageLocationEvent
                                log.info(String.format(
                                    "ArtifactValidator.deleteArtifact Artifact.id=%s Artifact.uri=%s"
                                        + " reason=local-filter-policy-change",
                                    local.getID(), local.getURI()));
                                this.artifactDAO.delete(local.getID());
                            } else if (current.storageLocation == null || numCopies > 1) { 
                                // if L==storage, delete Artifact only if multiple copies in global, create a DeletedStorageLocationEvent
                                // TODO: could be that the limit above is increased above 1 for reasons
                                log.info(String.format(
                                    "ArtifactValidator.deleteArtifact Artifact.id=%s Artifact.uri=%s"
                                        + " reason=local-filter-policy-exclude numCopies=" + numCopies,
                                    local.getID(), local.getURI()));
                                this.artifactDAO.delete(local.getID());

                                if (current.storageLocation != null) {
                                    DeletedStorageLocationEvent deletedStorageLocationEvent = new DeletedStorageLocationEvent(local.getID());
                                    log.info(String.format(
                                        "ArtifactValidator.createDeletedStorageLocationEvent id=%s uri=%s"
                                            + " reason=local-filter-policy-exclude numCopies=" + numCopies,
                                        deletedStorageLocationEvent.getID(), current.getURI()));
                                    this.deletedStorageLocationEventDAO.put(deletedStorageLocationEvent);
                                }
                            } else {
                                log.info(String.format("ArtifactValidator.deleteArtifact-delayed Artifact.id=%s Artifact.uri=%s"
                                        + " reason=local-filter-policy-exclude numCopies=" + numCopies, local.getID(), local.getURI()));
                            }

                            this.transactionManager.commitTransaction();
                        } else {
                            log.debug(String.format("ArtifactValidator.skip: Artifact.id=%s Artifact.uri=%s reason=stale-local-artifact", 
                                    local.getID(), local.getURI()));
                            this.transactionManager.rollbackTransaction();

                        }
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
                }
                // there was a matching remote without filter so this is resolved or delayed
                return;
            }
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

                Artifact current = this.artifactDAO.lock(local);
                if (current != null) {
                    log.info(String.format("ArtifactValidator.deleteArtifact id=%s uri=%s reason=DeletedArtifactEvent",
                                       local.getID(), local.getURI()));
                    this.deletedArtifactEventDAO.put(remoteDeletedArtifactEvent);
                    this.artifactDAO.delete(local.getID());

                    log.debug("committing transaction");
                    this.transactionManager.commitTransaction();
                    log.debug("commit txn: OK");
                } else {
                    log.debug(String.format("ArtifactValidator.skip: id=%s uri=%s reason=stale-local-artifact",
                            local.getID(), local.getURI()));
                    this.transactionManager.rollbackTransaction();
                }
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
        
        // note: action is the same as the default (explanation3) below
        // so this more expensive check is disabled... it only provides
        // a more concrete reason in the log
        /*
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

                    Artifact current = this.artifactDAO.lock(local);
                    if (current == null) {
                        throw new EntityNotFoundException(); // HACK: goto catch below
                    }
                    if (current.siteLocations.contains(remoteSiteLocation)) {
                        log.info(String.format("ArtifactValidator.removeSiteLocation id=%s uri=%s" 
                                    + " site=%s reason=no-remote-artifact", 
                                    current.getID(), current.getURI(), remoteSiteLocation)); 
                        this.artifactDAO.removeSiteLocation(current, remoteSiteLocation);
                    }

                    log.debug("committing transaction");
                    this.transactionManager.commitTransaction();
                    log.debug("commit txn: OK");
                } catch (EntityNotFoundException e) {
                    log.debug(String.format("ArtifactValidator.skip: Artifact.id=%s Artifact.uri=%s reason=stale-local-artifact",
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
        */

        if (this.remoteSite != null) {
            // explanation3: L==global, new Artifact in L, pending/missed Artifact or sync in R
            // also
            // explantion 2: L==global, missed DeletedStorageLocationEvent
            // explanation5: deleted from R, lost DeletedArtifactEvent
            // explanation6: L==global, lost DeletedStorageLocationEvent
            // evidence: ?
            // action: remove siteID from Artifact.storageLocations
            log.debug("explanation 2/3/5/6");
            SiteLocation remoteSiteLocation = new SiteLocation(this.remoteSite.getID());
            try {
                log.debug("starting transaction");
                this.transactionManager.startTransaction();
                log.debug("start txn: OK");

                Artifact current = this.artifactDAO.lock(local);
                if (current != null) {
                    if (current.siteLocations.contains(remoteSiteLocation)) {
                        log.info(String.format("ArtifactValidator.removeSiteLocation id=%s uri=%s" 
                                    + " site=%s reason=no-remote-artifact", 
                                    current.getID(), current.getURI(), remoteSiteLocation)); 
                        this.artifactDAO.removeSiteLocation(current, remoteSiteLocation);
                    }

                    log.debug("committing transaction");
                    this.transactionManager.commitTransaction();
                    log.debug("commit txn: OK");
                } else {
                    log.debug(String.format("skip: %s %s reason: stale local Artifact",
                            local.getID(), local.getURI()));
                    this.transactionManager.rollbackTransaction();
                }
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
        
        
        // explanation4: L==storage, new Artifact in L, pending/missed new Artifact event in R
        // action: none
        log.debug("explanation 4");
        logNoAction(local, "pending/missed new Artifact event in remote");
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
                log.debug("starting transaction");
                this.transactionManager.startTransaction();
                log.debug("start txn: OK");
                
                Artifact current = artifactDAO.lock(remote);
                if (current != null) {
                    // artifact appeared since query start: preserve existing storageLocation
                    remote.storageLocation = current.storageLocation;
                }
                log.info(String.format("ArtifactValidator.putArtifact id=%s uri=%s %s", 
                        remote.getID(), remote.getURI(), df.format(remote.getLastModified())));
                this.artifactDAO.put(remote);
                log.debug("committing transaction");
                this.transactionManager.commitTransaction();
                log.debug("commit txn: OK");
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

        // explantion4: L==global, stale Artifact in R, pending/missed DeletedArtifactEvent in R
        // evidence: artifact in L without siteLocation constraint
        // action: resolve as ID collision
        log.debug("checking explanation 4");
        if (this.remoteSite != null) {
            Artifact local = this.artifactDAO.get(remote.getURI());
            if (local != null) {
                validateLocalAndRemote(local, remote);
                return;
            }
        }
        
        // explanation5: L==global, new Artifact in R, pending/missed changed Artifact event in L
        // also
        // explanation0: filter policy at L changed to include artifact in R
        // explanation6: deleted from L, lost DeletedArtifactEvent
        // evidence: ?
        // action: insert Artifact and/or add siteID to Artifact.siteLocations
        log.debug("checking explanation 5");
        if (this.remoteSite != null) {
            SiteLocation remoteSiteLocation = new SiteLocation(this.remoteSite.getID());
            try {
                log.debug("starting transaction");
                this.transactionManager.startTransaction();
                log.debug("start txn: OK");

                Artifact current = this.artifactDAO.lock(remote);
                if (current != null) {
                    log.info(String.format("ArtifactValidator.addSiteLocation Artifact.id=%s Artifact.uri=%s",
                                       current.getID(), current.getURI()));
                    this.artifactDAO.addSiteLocation(current, remoteSiteLocation);
                } else {
                    log.info(String.format("ArtifactValidator.putArtifact Artifact.id=%s Artifact.uri=%s %s", 
                        remote.getID(), remote.getURI(), df.format(remote.getLastModified())));
                    this.artifactDAO.put(remote);
                    // explicit addSiteLocations like fenwick to propagate event
                    this.artifactDAO.addSiteLocation(remote, remoteSiteLocation);
                }

                log.debug("committing transaction");
                this.transactionManager.commitTransaction();
                log.debug("commit txn: OK");

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

                Artifact current = this.artifactDAO.lock(local);
                if (current != null) {
                    if (isRemoteWinner(local, remote)) {
                        log.debug(String.format(
                            "resolve Artifact.id collision: put DeletedArtifactEvent for local %s %s "
                                + "reason: remote contentLastModified newer than local",
                                remote.getID(), remote.getURI()));
                        DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(local.getID());
                        log.info(String.format("ArtifactValidator.createDeletedArtifactEvent id=%s uri=%s reason=resolve-collision",
                                deletedArtifactEvent.getID(), local.getURI()));
                        this.deletedArtifactEventDAO.put(deletedArtifactEvent);

                        log.info(String.format("ArtifactValidator.deletedArtifact id=%s uri=%s reason=resolve-collision",
                                local.getID(), local.getURI()));
                        this.artifactDAO.delete(local.getID());

                        log.info(String.format("ArtifactValidator.putArtifact id=%s uri=%s reason=resolve-collision", 
                                remote.getID(), remote.getURI()));
                        this.artifactDAO.put(remote);
                        if (remoteSite != null) {
                            artifactDAO.addSiteLocation(remote, new SiteLocation(remoteSite.getID()));
                        }
                    } else {
                        log.debug(String.format(
                            "resolve Artifact.id collision: put DeletedArtifactEvent for remote %s %s "
                                + "reason: local contentLastModified newer than remote",
                                remote.getID(), remote.getURI()));
                        DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(remote.getID());
                        log.info(String.format("ArtifactValidator.createDeletedArtifactEvent id=%s uri=%s reason=resolve-collision", 
                                deletedArtifactEvent.getID(), remote.getURI()));
                        this.deletedArtifactEventDAO.put(deletedArtifactEvent);
                    }
                    
                    log.debug("committing transaction");
                    this.transactionManager.commitTransaction();
                    log.debug("commit txn: OK");
                } else {
                    log.debug(String.format("skip: id=%s uri=%s reason: stale local Artifact", local.getID(), local.getURI()));
                    this.transactionManager.rollbackTransaction();
                }
            } catch (Exception e) {
                log.error(String.format("failed to resolve Artifact.id collision for local id=%s uri=%s, remote id=%s uri=%s",
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
            // subsequent discrepancies irrelevant now 
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
                    Artifact current = this.artifactDAO.lock(local);
                    if (current != null) {
                        if (!current.getMetaChecksum().equals(remote.getMetaChecksum())) {
                            if (current.getLastModified().before(remote.getLastModified())) {
                                if (this.remoteSite == null) {
                                    // storage site: keep StorageLocation
                                    remote.storageLocation = current.storageLocation;
                                } else {
                                    // global site: keep SiteLocation(s)
                                    remote.siteLocations.addAll(current.siteLocations);
                                }
                                
                                log.info(String.format("ArtifactValidator.putArtifact id=%s uri=%s %s reason=missed-update",
                                        remote.getID(), remote.getURI(), df.format(remote.getLastModified())));
                                this.artifactDAO.put(remote);
                                if (remoteSite != null) {
                                    // explicit so addSiteLocation can force lastModified update in global
                                    artifactDAO.addSiteLocation(remote, new SiteLocation(remoteSite.getID()));
                                }
                            } else {
                                // same as explanation2 below
                                log.info(String.format("ArtifactValidator.noAction id=%s uri=%s reason=metaChecksum-mismatch",
                                        local.getID(), local.getURI()));
                            }
                        }
                        
                        log.debug("committing transaction");
                        this.transactionManager.commitTransaction();
                        log.debug("commit txn: OK");
                    } else {
                        log.debug(String.format("skip: %s %s reason: stale local Artifact", local.getID(), local.getURI()));
                        this.transactionManager.rollbackTransaction();
                    }
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
                log.info(String.format("ArtifactValidator.noAction Artifact.id=%s Artifact.uri=%s reason=metaChecksum-mismatch",
                        local.getID(), local.getURI()));
            }
            // no return here: could also have to deal with subsequent discrepancies
        }

        // artifact in both && L==global && siteLocations does not include remote
        if (this.remoteSite != null) {
            SiteLocation remoteSiteLocation = new SiteLocation(this.remoteSite.getID());
            if (!local.siteLocations.contains(remoteSiteLocation)) {
                try {
                    log.debug("starting transaction");
                    this.transactionManager.startTransaction();
                    log.debug("start txn: OK");

                    Artifact current = this.artifactDAO.lock(local);
                    if (current != null) {
                        if (!current.siteLocations.contains(remoteSiteLocation)) {
                            log.info(String.format("ArtifactValidator.addSiteLocation Artifact.id=%s Artifact.uri=%s",
                                    current.getID(), current.getURI()));
                            this.artifactDAO.addSiteLocation(current, remoteSiteLocation);
                        }

                        log.debug("committing transaction");
                        this.transactionManager.commitTransaction();
                        log.debug("commit txn: OK");
                    } else {
                        log.debug(String.format("skip: %s %s reason: stale local Artifact",
                                            local.getID(), local.getURI()));
                        this.transactionManager.rollbackTransaction();
                    }
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
            }
        }
    }
    
    // true if remote is the winner, false if local is the winner
    // same logic in fenwick InventoryHarvester
    boolean isRemoteWinner(Artifact local, Artifact remote) {
        Boolean rem = InventoryUtil.isRemoteWinner(local, remote);
        if (rem != null) {
            return rem;
        }

        // equal timestamps and different instances:
        // likely caused by poorly behaved storage site that ingests duplicates 
        // from external system
        if (remoteSite != null) {
            // declare the artifact in global as the winner
            return false;
        }
        // this is storage site: remote is global
        return true;
    }

    /**
     * Get a remote Artifact
     */
    ArtifactQueryResult getRemoteArtifactQueryResult(UUID id)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {

        final TapClient<ArtifactQueryResult> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("%s, num_copies() %s WHERE id = '%s'",  ArtifactRowMapper.SELECT,  ArtifactRowMapper.FROM, id);
        log.debug("\nExecuting query '" + query + "'\n");
        
        ArtifactQueryResultRowMapper mapper = new ArtifactQueryResultRowMapper();
        try {
            return tapClient.queryForObject(query, mapper);
        }  catch (TransientException ex) {
            log.error("failed getRemote Artifact.id=" + id + " retryIn=2sec cause=" + ex);
            Thread.sleep(2000L);
            return tapClient.queryForObject(query, mapper);
        }
    }

    DeletedArtifactEvent getRemoteDeletedArtifactEvent(UUID id)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {

        final TapClient<DeletedArtifactEvent> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("%s WHERE id = '%s'", DeletedArtifactEventRowMapper.BASE_QUERY, id);
        log.debug("\nExecuting query '" + query + "'\n");
        
        DeletedArtifactEventRowMapper mapper = new DeletedArtifactEventRowMapper();
        try {
            return tapClient.queryForObject(query, mapper);
        } catch (TransientException ex) {
            log.error("failed getRemote DeletedArtifactEvent.id=" + id + " retryIn=2sec cause=" + ex);
            Thread.sleep(2000L);
            return tapClient.queryForObject(query, mapper);
        }
    }

    /*
    // no longer used in validateLocal(Artifact)
    DeletedStorageLocationEvent getRemoteDeletedStorageLocationEvent(UUID id)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {

        final TapClient<DeletedStorageLocationEvent> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("%s WHERE id = '%s'", DeletedStorageLocationEventRowMapper.BASE_QUERY, id);
        log.debug("\nExecuting query '" + query + "'\n");
        DeletedStorageLocationEventRowMapper mapper = new DeletedStorageLocationEventRowMapper();
        
        int n = 0;
        try {
            return tapClient.queryForObject(query, mapper);
        } catch (TransientException ex) {
            log.error("failed getRemote DeletedStorageLocationEvent.id=" + id + " retryIn=2sec cause=" + ex);
            Thread.sleep(2000L);
            return tapClient.queryForObject(query, mapper);
        }
    }
    */
    
    private void logNoAction(Artifact artifact, String message) {
        log.info(String.format("no action %s %s, reason: %s", artifact.getID(), artifact.getURI(), message));
    }

    private class ArtifactQueryResultRowMapper implements TapRowMapper<ArtifactQueryResult> {

        public ArtifactQueryResult mapRow(final List<Object> row) {
            ArtifactRowMapper mapper = new ArtifactRowMapper();
            Artifact artifact = mapper.mapRow(row);
            Integer numCopies = (Integer) row.get(row.size() - 1);

            ArtifactQueryResult result = new ArtifactQueryResult();
            result.artifact = artifact;
            result.numCopies = numCopies;
            return result;
        }
    }

    private static class ArtifactQueryResult {
        Artifact artifact;
        Integer numCopies;
    }
}


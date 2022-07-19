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
*
************************************************************************
*/

package org.opencadc.fenwick;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.util.Date;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.inventory.query.ArtifactRowMapper;
import org.opencadc.inventory.util.ArtifactSelector;
import org.opencadc.tap.TapClient;

/**
 * Artifact sync class.  This class is responsible for querying a remote TAP (Luskan) site to obtain desired Artifact
 * instances to then be used to store in the local inventory.
 */
public class ArtifactSync extends AbstractSync {

    private static final Logger log = Logger.getLogger(ArtifactSync.class);

    private final StorageSite storageSite;
    private final TapClient<Artifact> tapClient;
    private final String includeClause;

    public ArtifactSync(ArtifactDAO artifactDAO, URI resourceID, 
            int querySleepInterval, int maxRetryInterval, 
            ArtifactSelector selector, StorageSite storageSite) {
        super(artifactDAO, resourceID, querySleepInterval, maxRetryInterval);
        this.storageSite = storageSite;
        try {
            this.tapClient = new TapClient<>(resourceID);
        } catch (ResourceNotFoundException ex) {
            throw new IllegalArgumentException("invalid config: query service not found: " + resourceID);
        }
        try {
            this.includeClause = selector.getConstraint();
        } catch (IOException | ResourceNotFoundException ex) {
            throw new IllegalArgumentException("invalid config: failed to read artifact selector config: " + ex);
        }
    }
    
    // unit test ctor
    ArtifactSync(String includeClause) {
        super(true);
        this.storageSite = null;
        this.tapClient = null;
        this.includeClause = includeClause;
    }
    
    @Override
    void doit() throws ResourceNotFoundException, IOException, IllegalStateException, TransientException, InterruptedException {
        final MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("BUG: failed to get instance of MD5", e);
        }

        final SiteLocation remoteSiteLocation = (storageSite == null ? null : new SiteLocation(storageSite.getID()));
        
        HarvestState hs = this.harvestStateDAO.get(Artifact.class.getSimpleName(), resourceID);
        if (hs.curLastModified == null) {
            // TEMPORARY: check for pre-rename record and rename
            HarvestState orig = harvestStateDAO.get(Artifact.class.getName(), resourceID);
            if (orig.curLastModified != null) {
                orig.setName(Artifact.class.getSimpleName());
                harvestStateDAO.put(orig);
                hs = orig;
            }
        }
        final HarvestState harvestState = hs;
        harvestStateDAO.setUpdateBufferCount(99); // buffer 99 updates, do every 100
        harvestStateDAO.setMaintCount(9999); // skip for 9999, do every 10k
        
        final Date endTime = new Date();
        final Date lookBack = new Date(endTime.getTime() - LOOKBACK_TIME);
        Date startTime = getQueryLowerBound(lookBack, harvestState.curLastModified);
        
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        if (lookBack != null && harvestState.curLastModified != null) {
            log.debug("lookBack=" + df.format(lookBack) + " curLastModified=" + df.format(harvestState.curLastModified) 
                + " -> " + df.format(startTime));
        }
        String start = null;
        if (startTime != null) {
            start = df.format(startTime);
        }
        String end = null;
        if (endTime != null) {
            end = df.format(endTime);
        }
        log.info("Artifact.QUERY start=" + start + " end=" + end);
        
        final TransactionManager transactionManager = artifactDAO.getTransactionManager();
        final DeletedArtifactEventDAO daeDAO = new DeletedArtifactEventDAO(artifactDAO);
        
        boolean first = true;
        long t1 = System.currentTimeMillis();
        try (final ResourceIterator<Artifact> artifactResourceIterator = getEventStream(startTime, endTime)) {
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
                    log.info("ArtifactSync.skipArtifact id=" + artifact.getID() 
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
                            log.info("ArtifactSync.createDeletedArtifactEvent id=" + dae.getID()
                                    + " uri=" + currentArtifact.getURI()
                                    + " reason=resolve-collision");
                            daeDAO.put(new DeletedArtifactEvent(currentArtifact.getID()));
                            log.info("ArtifactSync.deleteArtifact id=" + currentArtifact.getID()
                                    + " uri=" + currentArtifact.getURI()
                                    + " contentLastModified=" + df.format(currentArtifact.getContentLastModified())
                                    + " reason=resolve-collision");
                            artifactDAO.delete(currentArtifact.getID());
                        } else {
                            log.info("ArtifactSync.skipArtifact id=" + artifact.getID() 
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

                        log.info("ArtifactSync.putArtifact id=" + artifact.getID() 
                                + " uri=" + artifact.getURI() 
                                + " lastModified=" + df.format(artifact.getLastModified()));
                        artifactDAO.put(artifact);
                        if (remoteSiteLocation != null) {
                            // explicit so addSiteLocation can force lastModified update in global
                            artifactDAO.addSiteLocation(artifact, remoteSiteLocation);
                        }
                    }
                    
                    transactionManager.commitTransaction();
                    
                    // update state outside transaction because experimental maintenance enabled
                    harvestState.curLastModified = harvestedLastModified;
                    harvestState.curID = artifact.getID();
                    harvestStateDAO.put(harvestState);
                    
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
        } finally {
            harvestStateDAO.flushBufferedState();
            logSummary(Artifact.class, true);
        }
    }
    
    // true if remote is the winner, false if local is the winner
    // same logic in ratik ArtifactValidator
    private boolean isRemoteWinner(Artifact local, Artifact remote, boolean remoteStorageSite) {
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

    ResourceIterator<Artifact> getEventStream(Date start, Date end)
            throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                   InterruptedException {
        final String query = buildQuery(start, end);
        log.debug("\nExecuting query '" + query + "'\n");
        return tapClient.query(query, new ArtifactRowMapper());
    }

    /**
     * Assemble the WHERE clause and return the full query.  Very useful for testing separately.
     * @return  String query.  Never null.
     */
    String buildQuery(Date startTime, Date endTime) {
        final StringBuilder query = new StringBuilder();
        query.append(ArtifactRowMapper.BASE_QUERY);

        DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
        if (startTime != null) {
            log.debug("\nInjecting lastModified date compare '" + startTime + "'\n");
            query.append(" WHERE lastModified >= '").append(df.format(startTime)).append("'");

            if (endTime != null) {
                query.append(" AND ").append("lastModified < '").append(df.format(endTime)).append("'");
            }
        } else if (endTime != null) {
            query.append(" WHERE ").append("lastModified < '").append(df.format(endTime)).append("'");
        }
        
        if (StringUtil.hasText(includeClause)) {
            log.debug("\nInjecting clause '" + includeClause + "'\n");

            if (query.indexOf("WHERE") < 0) {
                query.append(" WHERE ");
            } else {
                query.append(" AND ");
            }

            query.append("(").append(includeClause.trim()).append(")");
        }

        query.append(" ORDER BY lastModified");

        return query.toString();
    }
}

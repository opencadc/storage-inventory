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

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.util.Date;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageLocationEvent;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.inventory.query.StorageLocationEventRowMapper;
import org.opencadc.tap.TapClient;

/**
 *
 * @author pdowler
 */
public class StorageLocationEventSync extends AbstractSync {
    private static final Logger log = Logger.getLogger(StorageLocationEventSync.class);

    private final StorageSite storageSite;
    private final TapClient<StorageLocationEvent> tapClient;
    
    public StorageLocationEventSync(ArtifactDAO artifactDAO, URI resourceID, 
            int querySleepInterval, int maxRetryInterval, 
            StorageSite storageSite) {
        super(artifactDAO, resourceID, querySleepInterval, maxRetryInterval);
        InventoryUtil.assertNotNull(StorageLocationEventSync.class, "storageSite", storageSite);
        this.storageSite = storageSite;
        try {
            this.tapClient = new TapClient<>(resourceID);
        } catch (ResourceNotFoundException ex) {
            throw new IllegalArgumentException("invalid config: query service not found: " + resourceID);
        }
    }
    
    @Override
    void doit() throws ResourceNotFoundException, IOException, IllegalStateException, TransientException, InterruptedException {
        final MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("BUG: failed to get instance of MD5", e);
        }

        final HarvestState harvestState = this.harvestStateDAO.get(StorageLocationEvent.class.getSimpleName(), resourceID);
        harvestStateDAO.setUpdateBufferCount(99); // buffer 99 updates, do every 100
        
        final Date endTime = new Date();
        final Date lookBack = new Date(endTime.getTime() - LOOKBACK_TIME);
        Date startTime = getQueryLowerBound(lookBack, harvestState.curLastModified);
        
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        if (harvestState.curLastModified != null) {
            log.debug("lookBack=" + df.format(lookBack) + " curLastModified=" + df.format(harvestState.curLastModified) 
                + " -> " + df.format(startTime));
        }
        String start = null;
        if (startTime != null) {
            start = df.format(startTime);
        }
        String end = df.format(endTime);
        log.info("StorageLocationEvent.QUERY start=" + start + " end=" + end);
        
        final TransactionManager transactionManager = artifactDAO.getTransactionManager();
        
        final SiteLocation siteLocation = new SiteLocation(storageSite.getID());
        boolean first = true;
        long t1 = System.currentTimeMillis();
        try (final ResourceIterator<StorageLocationEvent> resourceIterator =
                     getEventStream(startTime, endTime)) {
            long dt = System.currentTimeMillis() - t1;
            log.info("StorageLocationEvent.QUERY start=" + start + " end=" + end + " duration=" + dt);
            while (resourceIterator.hasNext()) {
                final StorageLocationEvent syncEvent = resourceIterator.next();
                if (first) {
                    
                    first = false;
                    
                    if (syncEvent.getID().equals(harvestState.curID)
                        && syncEvent.getLastModified().equals(harvestState.curLastModified)) {
                        log.debug("SKIP: previously processed: " + syncEvent.getID());
                        // ugh but the skip is comprehensible: have to do this inside the loop when using
                        // try-with-resources
                        continue;
                    }
                }
                
                URI computedCS = syncEvent.computeMetaChecksum(messageDigest);
                if (!computedCS.equals(syncEvent.getMetaChecksum())) {
                    throw new IllegalStateException("checksum mismatch: " + syncEvent.getID()
                            + " provided=" + syncEvent.getMetaChecksum() + " actual=" + computedCS);
                }
                
                try {
                    transactionManager.startTransaction();
                    Artifact cur = artifactDAO.lock(syncEvent.getID());
                    if (cur != null) {
                        log.info("StorageLocationEventSync.addSiteLocation id=" + cur.getID() 
                                + " uri=" + cur.getURI() 
                                + " lastModified=" + df.format(syncEvent.getLastModified()));
                        artifactDAO.addSiteLocation(cur, siteLocation);
                    } else {
                        log.debug("StorageLocationEventSync.addSiteLocation SKIP id=" 
                                + syncEvent.getID() 
                                + " reason=no-matching-artifact");
                    }
                    harvestState.curLastModified = syncEvent.getLastModified();
                    harvestState.curID = syncEvent.getID();
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
                logSummary(StorageLocationEvent.class);
            }
        } finally {
            harvestStateDAO.flushBufferedState();
            logSummary(StorageLocationEvent.class, true);
        }
    }
    
    ResourceIterator<StorageLocationEvent> getEventStream(Date start, Date end)
            throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                   InterruptedException {
        final String query = buildQuery(start, end);
        log.debug("adql: " + query);
        return tapClient.query(query, new StorageLocationEventRowMapper());
    }
    
    private String buildQuery(Date startTime, Date endTime) {
        DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
        
        StringBuilder query = new StringBuilder();
        query.append(StorageLocationEventRowMapper.BASE_QUERY);
        String pre = " WHERE";
        if (startTime != null) {
            query.append(pre).append(" lastModified >= '").append(df.format(startTime)).append("'");
            pre = " AND";
        }    
        if (endTime != null) {
            query.append(pre).append(" lastModified < '").append(df.format(endTime)).append("'");
        }
        query.append(" ORDER BY lastModified");
        
        return query.toString();
    }
}

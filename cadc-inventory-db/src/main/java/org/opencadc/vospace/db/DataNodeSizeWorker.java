/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

package org.opencadc.vospace.db;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.inventory.db.HarvestStateDAO;
import org.opencadc.vospace.DataNode;

/**
 * This class performs the work of synchronizing the size of Data Nodes from 
 * inventory (Artifact) to vopsace (Node).
 * 
 * @author adriand
 */
public class DataNodeSizeWorker implements Runnable {
    private static final Logger log = Logger.getLogger(DataNodeSizeWorker.class);

    // lookback when doing incremental harvest because head of sequence is
    // not monotonic over short timescales (events arrive out of sequence)
    private static final long LOOKBACK_TIME_MS = 60 * 1000L;

    private final HarvestState harvestState;
    private final NodeDAO nodeDAO;
    private final ArtifactDAO artifactDAO;
    private final HarvestStateDAO harvestStateDAO;
    private final Namespace storageNamespace;
    private boolean isStorageSite;
    
    private long numArtifactsProcessed;

    /**
     * Worker constructor.
     * 
     * @param harvestStateDAO DAO class to persist progress in the vospace database
     * @param harvestState current HarvestState instance
     * @param artifactDAO DAO class to query for artifacts
     * @param namespace artifact namespace
     */
    public DataNodeSizeWorker(HarvestStateDAO harvestStateDAO, HarvestState harvestState, 
            ArtifactDAO artifactDAO, Namespace namespace, boolean isStorageSite) {
        this.harvestState = harvestState;
        this.harvestStateDAO = harvestStateDAO;
        this.nodeDAO = new NodeDAO(harvestStateDAO);
        this.artifactDAO = artifactDAO;
        this.storageNamespace = namespace;
        this.isStorageSite = isStorageSite;
    }

    public long getNumArtifactsProcessed() {
        return numArtifactsProcessed;
    }

    @Override
    public void run() {
        this.numArtifactsProcessed = 0L;
        String opName = DataNodeSizeWorker.class.getSimpleName() + ".artifactQuery";
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        if (harvestState.curLastModified != null) {
            log.debug(opName + " source=" + harvestState.getResourceID() 
                    + " instance=" + harvestState.instanceID 
                    + " start=" + df.format(harvestState.curLastModified));
        } else {
            log.debug(opName + " source=" + harvestState.getResourceID() 
                    + " instance=" + harvestState.instanceID
                    + " start=null");
        }

        final Date now = new Date();
        final Date lookBack = new Date(now.getTime() - LOOKBACK_TIME_MS);
        Date startTime = getQueryLowerBound(lookBack, harvestState.curLastModified);
        if (lookBack != null && harvestState.curLastModified != null) {
            log.debug("lookBack=" + df.format(lookBack) + " curLastModified=" + df.format(harvestState.curLastModified) 
                + " -> " + df.format(startTime));
        }

        String uriBucket = null; // process all artifacts in a single thread
        try (final ResourceIterator<Artifact> iter = artifactDAO.iterator(storageNamespace, uriBucket, startTime, true, isStorageSite)) {
            TransactionManager tm = nodeDAO.getTransactionManager();
            while (iter.hasNext()) {
                Artifact artifact = iter.next();
                DataNode node = nodeDAO.getDataNode(artifact.getURI());
                if (node != null  && !artifact.getContentLength().equals(node.bytesUsed)) {
                    log.debug(artifact.getURI() + " len=" + artifact.getContentLength() + " -> " + node.getName());
                    tm.startTransaction();
                    try {
                        node = (DataNode)nodeDAO.lock(node);
                        if (node == null) {
                            continue; // node gone - race condition
                        }
                        node.bytesUsed = artifact.getContentLength();
                        nodeDAO.put(node);
                        tm.commitTransaction();
                        log.debug("ArtifactSyncWorker.updateDataNode id=" + node.getID() 
                                + " bytesUsed=" + node.bytesUsed + " artifact.lastModified=" + df.format(artifact.getLastModified()));
                    } catch (Exception ex) {
                        log.debug("Failed to update data node size for " + node.getName(), ex);
                        tm.rollbackTransaction();
                        throw ex;
                    } finally {
                        if (tm.isOpen()) {
                            log.error("BUG: transaction open in finally. Rolling back...");
                            tm.rollbackTransaction();
                            log.error("Rollback: OK");
                            throw new RuntimeException("BUG: transaction open in finally");
                        }
                    }
                }
                harvestState.curLastModified = artifact.getLastModified();
                harvestState.curID = artifact.getID();
                harvestStateDAO.put(harvestState);
                numArtifactsProcessed++;
            }
        } catch (IOException ex) {
            log.error("Error closing iterator", ex);
            throw new RuntimeException("error while closing ResourceIterator", ex);
        }
        if (harvestState.curLastModified != null) {
            log.debug(opName + " source=" + harvestState.getResourceID() 
                    + " instance=" + harvestState.instanceID 
                    + " end=" + df.format(harvestState.curLastModified));
        } else {
            log.debug(opName + " source=" + harvestState.getResourceID() 
                    + " instance=" + harvestState.instanceID 
                    + " end=null");
        }
    }

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
}

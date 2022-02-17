/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

package org.opencadc.minoc;

import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.profiler.Profiler;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.db.EntityNotFoundException;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.permissions.WriteGrant;

/**
 * Interface with storage and inventory to update the metadata of an artifact.
 *
 * @author majorb
 */
public class PostAction extends ArtifactAction {
    
    private static final Logger log = Logger.getLogger(PostAction.class);

    /**
     * Default, no-arg constructor.
     */
    public PostAction() {
        super();
    }
    
    /**
     * Perform auth checks and initialize resources.
     */
    @Override
    public void initAction() throws Exception {
        checkWritable();
        initAndAuthorize(WriteGrant.class);
        initDAO();
        initStorageAdapter();
    }

    /**
     * Update artifact metadata.
     */
    @Override
    public void doAction() throws Exception {
        
        String newURI = syncInput.getParameter("uri");
        String newContentType = syncInput.getParameter("contentType");
        String newContentEncoding = syncInput.getParameter("contentEncoding");
        log.debug("new uri: " + newURI);
        log.debug("new contentType: " + newContentType);
        log.debug("new contentEncoding: " + newContentEncoding);
        
        String txnID = syncInput.getHeader(PUT_TXN_ID);
        String txnOP = syncInput.getHeader(PUT_TXN_OP);
        log.warn("transactionID: " + txnID + " " + txnOP);
        if (txnID != null) {
            if (PUT_TXN_OP_START.equalsIgnoreCase(txnOP) 
                    || PUT_TXN_OP_COMMIT.equalsIgnoreCase(txnOP)) {
                throw new IllegalArgumentException("invalid " + PUT_TXN_OP + "=" + txnOP + " must be done with PUT");
            } 
            if (PUT_TXN_OP_ABORT.equalsIgnoreCase(txnOP)) {
                log.warn("abortTransaction: " + txnID);
                storageAdapter.abortTransaction(txnID);
                syncOutput.setCode(204);
                return;
            }
            
            PutTransaction t;
            if (PUT_TXN_OP_REVERT.equalsIgnoreCase(txnOP)) {
                t = storageAdapter.revertTransaction(txnID);
                syncOutput.setCode(202);
            } else if (txnOP == null) {
                // POST without no OP:  no change or fail?
                t = storageAdapter.getTransactionStatus(txnID);
                syncOutput.setCode(204);
            } else {
                throw new IllegalArgumentException("invalid " + PUT_TXN_OP + "=" + txnOP);
            }
            
            StorageMetadata sm = t.storageMetadata;
            Artifact artifact = new Artifact(sm.artifactURI, sm.getContentChecksum(), sm.getContentLastModified(), sm.getContentLength());
            HeadAction.setTransactionHeaders(t, syncOutput);
            syncOutput.setDigest(artifact.getContentChecksum());
            syncOutput.setHeader("content-length", 0);
            return;
        }
        
        final Profiler profiler = new Profiler(PostAction.class);
        Artifact existing = getArtifact(artifactURI);
        profiler.checkpoint("artifactDAO.get.ok");
        
        TransactionManager txnMgr = artifactDAO.getTransactionManager();
        try {
            log.debug("starting transaction");
            txnMgr.startTransaction();
            log.debug("start txn: OK");
            
            boolean locked = false;
            while (existing != null && !locked) {
                existing = artifactDAO.lock(existing);
                profiler.checkpoint("artifactDAO.lock.ok");
                if (existing != null) {
                    locked = true;
                } else {
                    profiler.checkpoint("artifactDAO.lock.fail");
                    // try again via uri
                    existing = artifactDAO.get(artifactURI);
                    profiler.checkpoint("artifactDAO.get.ok");
                }
            }
            
            // TODO: enable modifying URIs when supported by DAO
            // TODO: how to support clearing values?
            if (newContentType != null) {
                existing.contentType = newContentType;
            }
            if (newContentEncoding != null) {
                existing.contentEncoding = newContentEncoding;
            }
            
            artifactDAO.put(existing);
            profiler.checkpoint("artifactDAO.put.ok");
        
            txnMgr.commitTransaction();
            log.debug("commit txn: OK");
            
            syncOutput.setCode(202); // Accepted
            HeadAction.setHeaders(existing, syncOutput);
        } catch (Exception e) {
            log.error("failed to persist " + artifactURI, e);
            txnMgr.rollbackTransaction();
            log.debug("rollback txn: OK");
            throw e;
        } finally {
            if (txnMgr.isOpen()) {
                log.error("BUG - open transaction in finally");
                txnMgr.rollbackTransaction();
                log.error("rollback txn: OK");
            }
        }
        
        
    }

}

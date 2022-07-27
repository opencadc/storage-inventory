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

package org.opencadc.minoc;

import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.profiler.Profiler;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.ObsoleteStorageLocation;
import org.opencadc.inventory.db.ObsoleteStorageLocationDAO;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.permissions.WriteGrant;

/**
 * Interface with storage and inventory to PUT an artifact.
 * 
 * @author majorb
 */
public class PutAction extends ArtifactAction {
    private static final Logger log = Logger.getLogger(PutAction.class);
    
    private static final String INLINE_CONTENT_TAG = "inputstream";

    public PutAction() {
        super();
    }
    
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return new InlineContentHandler() {
            public Content accept(String name, String contentType, InputStream inputStream)
                    throws InlineContentException, IOException, ResourceNotFoundException {
                Content content = new Content();
                content.name = INLINE_CONTENT_TAG;
                content.value = inputStream;
                return content;
            }
        };
    }
    
    @Override
    public void initAction() throws Exception {
        checkWritable();
        initAndAuthorize(WriteGrant.class);
        initDAO();
        initStorageAdapter();
    }

    @Override
    public void doAction() throws Exception {
        
        URI digest = syncInput.getDigest();
        String lengthHeader = syncInput.getHeader("Content-Length");
        String encodingHeader = syncInput.getHeader("Content-Encoding");
        String typeHeader = syncInput.getHeader("Content-Type");
        log.debug("Digest: " + (digest == null ? null : digest.toASCIIString()));
        log.debug("Content-Length: " + lengthHeader);
        log.debug("Content-Encoding: " + encodingHeader);
        log.debug("Content-Type: " + typeHeader);

        Long contentLength = null;
        if (lengthHeader != null) {
            try {
                contentLength = Long.parseLong(lengthHeader);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Illegal Content-Length header: " + lengthHeader);
            }
        }
        log.debug("Content-Length: " + contentLength);
        
        String txnID = syncInput.getHeader(PUT_TXN_ID);
        String txnOP = syncInput.getHeader(PUT_TXN_OP);
        String totalLengthHeader = syncInput.getHeader("x-total-length");
        Long totalLength = null;
        if (totalLengthHeader != null) {
            try {
                totalLength = Long.parseLong(totalLengthHeader);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Illegal x-total-length header: " + totalLengthHeader);
            }
        } 
        if (PUT_TXN_OP_START.equals(txnOP)) {
            // check for invalid transaction start
            if (txnID != null) {
                throw new IllegalArgumentException(PUT_TXN_OP + "=" + txnOP + " cannot include " + PUT_TXN_ID + "=" + txnID);
            }
            if (totalLength == null && contentLength != null && contentLength.longValue() != 0) {
                totalLength = contentLength;
            }
            PutTransaction t = storageAdapter.startTransaction(artifactURI, totalLength);
            txnID = t.getID();
            if (contentLength != null && contentLength == 0L) {
                // explicit start transaction, no data
                syncOutput.setCode(202); // accepted
                HeadAction.setTransactionHeaders(t, syncOutput);
                super.logInfo.setMessage("transaction: " + txnID);
                return;
            }
        }
        if (PUT_TXN_OP_COMMIT.equalsIgnoreCase(txnOP)) {
            // check for invalid transaction commit
            if (txnID == null) {
                throw new IllegalArgumentException(PUT_TXN_OP + "=" + txnOP + ": requires " + PUT_TXN_ID + "={transaction ID}");
            }
            if (contentLength != null && contentLength > 0) {
                throw new IllegalArgumentException(PUT_TXN_OP + "=" + txnOP + ": requires content-length=0");
            }
        }
        
        log.debug("transactionID: " + txnID + " " + txnOP);

        // here: txnID != null means in a transaction
        //       segmentSize == 0 means start transaction and return (header) info
        //       0 < segmentSize < contentLength means put a segment of the file
        //       segmentSize == null means put whole file
        //       segmentSize == contentLength means put whole file
        
        NewArtifact newArtifact = new NewArtifact(artifactURI);
        newArtifact.contentChecksum = digest;
        newArtifact.contentLength = contentLength;

        final Profiler profiler = new Profiler(PutAction.class);

        InputStream in = (InputStream) syncInput.getContent(INLINE_CONTENT_TAG);
        if (in == null) {
            throw new IllegalArgumentException("invalid input: no content");
        }

        profiler.checkpoint("content.init");

        // commit transaction or write data
        StorageMetadata artifactMetadata = null;
        if (PUT_TXN_OP_COMMIT.equalsIgnoreCase(txnOP)) {
            artifactMetadata = storageAdapter.commitTransaction(txnID);
            txnID = null;
            profiler.checkpoint("storageAdapter.put.commit.ok");
        } else {
            log.debug("writing new artifact to " + storageAdapter.getClass().getName());
            try {
                artifactMetadata = storageAdapter.put(newArtifact, in, txnID);
                profiler.checkpoint("storageAdapter.put.write.ok");
            } catch (ReadException ex) {
                profiler.checkpoint("storageAdapter.put.write.fail");
                if (contentLength != null) {
                    Throwable cause = ex.getCause();
                    while (cause != null) {
                        if (cause instanceof EOFException) {
                            throw new PreconditionFailedException("premature end-of-stream: expected content-length " + contentLength, cause);
                        }
                        cause = cause.getCause();
                    }
                }
                throw new IllegalArgumentException("read input failure", ex);
            } catch (StorageEngageException | WriteException ex) {
                profiler.checkpoint("storageAdapter.put.write.fail");
                throw new RuntimeException("backend storage failure", ex);
            } catch (ByteLimitExceededException | PreconditionFailedException | TransientException ex) {
                profiler.checkpoint("storageAdapter.put.write.fail");
                throw ex;
            }
            log.debug("writing new artifact to " + storageAdapter.getClass().getName() + " OK");
        }
        
        Artifact artifact = new Artifact(
            artifactURI, artifactMetadata.getContentChecksum(),
            artifactMetadata.getContentLastModified(), artifactMetadata.getContentLength());
        artifact.contentEncoding = encodingHeader;
        artifact.contentType = typeHeader;
        artifact.storageLocation = artifactMetadata.getStorageLocation();

        if (txnID != null) {
            PutTransaction t = storageAdapter.getTransactionStatus(txnID);
            syncOutput.setCode(202); // accepted
            HeadAction.setTransactionHeaders(t, syncOutput);
            syncOutput.setDigest(artifact.getContentChecksum());
            syncOutput.setHeader("content-length", 0);
            super.logInfo.setMessage("transaction: " + txnID);
            return;
        }
        
        ObsoleteStorageLocationDAO locDAO = new ObsoleteStorageLocationDAO(artifactDAO);
        Artifact existing = artifactDAO.get(artifactURI);
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

            ObsoleteStorageLocation prevOSL = locDAO.get(artifact.storageLocation);
            if (prevOSL != null) {
                // no longer obsolete
                locDAO.delete(prevOSL.getID());
                profiler.checkpoint("locDAO.delete.ok");
            }
            
            ObsoleteStorageLocation newOSL = null;
            if (existing != null) {
                DeletedArtifactEventDAO eventDAO = new DeletedArtifactEventDAO(artifactDAO);
                DeletedArtifactEvent deletedArtifact = new DeletedArtifactEvent(existing.getID());
                eventDAO.put(deletedArtifact);
                profiler.checkpoint("eventDAO.put.ok");
                
                artifactDAO.delete(existing.getID());
                profiler.checkpoint("artifactDAO.delete.ok");
            }
            
            artifactDAO.put(artifact);
            profiler.checkpoint("artifactDAO.put.ok");
            log.debug("put artifact in database: " + artifactURI);
            
            if (existing != null && existing.storageLocation != null) {
                if (!artifact.storageLocation.equals(existing.storageLocation)) {
                    newOSL = new ObsoleteStorageLocation(existing.storageLocation);
                    locDAO.put(newOSL);
                    profiler.checkpoint("locDAO.put.ok");
                    log.debug("marked obsolete: " + existing.storageLocation);
                }
            }
            
            log.debug("committing transaction");
            txnMgr.commitTransaction();
            profiler.checkpoint("transaction.commit.ok");
            log.debug("commit txn: OK");
            
            syncOutput.setCode(201); // created
            syncOutput.setDigest(artifact.getContentChecksum());
            syncOutput.setHeader("content-length", 0);
            
            super.logInfo.setBytes(artifact.getContentLength());
            
            // this block could be passed off to a thread so request completes??
            if (newOSL != null) {
                log.debug("deleting from storage...");
                storageAdapter.delete(newOSL.getLocation());
                profiler.checkpoint("storageAdapter.delete.ok");
                log.debug("delete from storage: OK");
                // obsolete tracker record no longer needed
                locDAO.delete(newOSL.getID());
                profiler.checkpoint("locDAO.delete.ok");
            }
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

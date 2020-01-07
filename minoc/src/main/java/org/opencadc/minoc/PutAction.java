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
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StreamCorruptedException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedEventDAO;
import org.opencadc.inventory.permissions.WriteGrant;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * Interface with storage and inventory to PUT an artifact.
 * 
 * @author majorb
 */
public class PutAction extends ArtifactAction {
    private static final Logger log = Logger.getLogger(PutAction.class);
    
    private static final String INLINE_CONTENT_TAG = "inputstream";

    /**
     * Default, no-arg constructor.
     */
    public PutAction() {
        super();
    }
    
    /**
     * Return the input stream.
     * @return The Object representing the input stream.
     */
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

    /**
     * Perform the PUT.
     */
    @Override
    public void doAction() throws Exception {
        
        initAndAuthorize(WriteGrant.class);
        
        String md5Header = syncInput.getHeader("Content-MD5");
        String lengthHeader = syncInput.getHeader("Content-Length");
        String encodingHeader = syncInput.getHeader("Content-Encoding");
        String typeHeader = syncInput.getHeader("Content-Type");
        log.debug("Content-MD5: " + md5Header);
        log.debug("Content-Length: " + lengthHeader);
        log.debug("Content-Encoding: " + encodingHeader);
        log.debug("Content-Type: " + typeHeader);
        
        URI contentMD5 = null;
        Long contentLength = null;
        if (md5Header != null) {
            try {
                contentMD5 = new URI("md5:" + md5Header);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Illegal Content-MD5 header: " + md5Header);
            }
        }
        if (lengthHeader != null) {
            try {
                contentLength = new Long(lengthHeader);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Illegal Content-Length header: " + lengthHeader);
            }
        }
        log.debug("Content-MD5: " + contentMD5);
        log.debug("Content-Length: " + contentLength);
                
        NewArtifact newArtifact = new NewArtifact(artifactURI);
        newArtifact.contentChecksum = contentMD5;
        newArtifact.contentLength = contentLength;
        
        InputStream in = (InputStream) syncInput.getContent(INLINE_CONTENT_TAG);
        
        StorageMetadata artifactMetadata = null;
        
        log.debug("writing new artifact to storage...");
        artifactMetadata = getStorageAdapter().put(newArtifact, in);
        log.debug("wrote new artifact to storage");

        Artifact artifact = new Artifact(
            artifactURI, artifactMetadata.getContentChecksum(),
            new Date(), artifactMetadata.getContentLength());
        artifact.contentEncoding = encodingHeader;
        artifact.contentType = typeHeader;
        artifact.storageLocation = artifactMetadata.getStorageLocation();
        
        ArtifactDAO artifactDAO = getArtifactDAO();
        Artifact existing = artifactDAO.get(artifactURI);
        if (existing == null) {
            artifactDAO.put(artifact);
            log.debug("put artifact in database: " + artifactURI);
        } else {
            DeletedEventDAO deletedEventDAO = getDeletedEventDAO(artifactDAO);
            TransactionManager txnMgr = artifactDAO.getTransactionManager();
            try {
                txnMgr.startTransaction();
                DeletedArtifactEvent deletedArtifact = new DeletedArtifactEvent(existing.getID());
                artifactDAO.delete(existing.getID());
                artifactDAO.put(artifact);
                deletedEventDAO.put(deletedArtifact);
                txnMgr.commitTransaction();
                log.debug("replaced artifact in database: " + deletedArtifact.getID()
                    + " with " + artifact.getID());
            } catch (Throwable t) {
                String msg = "failed to replace inventory artifact";
                log.error(msg, t);
                throw t;
            } finally {
                if (txnMgr.isOpen()) {
                    try {
                        log.debug("rolling back transaction...");
                        txnMgr.rollbackTransaction();
                        log.error("rollback successful");
                    } catch (Exception e) {
                        log.error("failed to rollback transaction", e);
                    }
                }
            }
        }
        
    }

}

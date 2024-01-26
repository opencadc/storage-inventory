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

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.rest.SyncOutput;
import java.text.DateFormat;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.permissions.ReadGrant;

/**
 * Interface with storage and inventory to get the metadata of an artifact.
 *
 * @author majorb
 */
public class HeadAction extends ArtifactAction {
    
    private static final Logger log = Logger.getLogger(HeadAction.class);
    
    public static final String ARTIFACT_ID_HDR = "x-artifact-id";

    /**
     * Default, no-arg constructor.
     */
    public HeadAction() {
        super();
        this.extractFilenameOverride = true;
    }
    
    /**
     * Perform auth checks and initialize resources.
     */
    @Override
    public void initAction() throws Exception {
        super.initAction();
        checkReadable();
        initAndAuthorize(ReadGrant.class, true); // allowReadWithWriteGrant for head after put
        initDAO();
        initStorageAdapter();
    }

    /**
     * Return the artifact metadata as repsonse headers.
     */
    @Override
    public void doAction() throws Exception {
        
        String txnID = syncInput.getHeader(PUT_TXN_ID);
        log.debug("transactionID: " + txnID);
        Artifact artifact = null;
        if (txnID != null) {
            PutTransaction t = storageAdapter.getTransactionStatus(txnID);
            setTransactionHeaders(t, syncOutput);
            StorageMetadata sm = t.storageMetadata;
            if (sm.isValid()) {
                artifact = new Artifact(sm.getArtifactURI(), sm.getContentChecksum(), sm.getContentLastModified(), sm.getContentLength());
            } else {
                log.debug("invalid artifact in transaction " +  txnID + " -- assuming 0 bytes stored");
                syncOutput.setHeader("Content-Length", "0");
            }
            super.logInfo.setMessage("transaction: " + txnID);
        } else {
            artifact = getArtifact(artifactURI);
        }
        if (artifact != null) {
            setHeaders(artifact, filenameOverride, syncOutput);
        }
    }
    
    /**
     * Set the HTTP response headers for an artifact.
     * @param artifact The artifact with metadata
     * @param syncOutput The target response
     */
    static void setHeaders(Artifact artifact, String filenameOverride, SyncOutput syncOutput) {
        syncOutput.setHeader(ARTIFACT_ID_HDR, artifact.getID().toString());
        syncOutput.setDigest(artifact.getContentChecksum());
        syncOutput.setLastModified(artifact.getContentLastModified());
        syncOutput.setHeader("Content-Length", artifact.getContentLength());
        
        DateFormat df = DateUtil.getDateFormat(DateUtil.HTTP_DATE_FORMAT, DateUtil.GMT);
        syncOutput.setHeader("Last-Modified", df.format(artifact.getContentLastModified()));

        String filename = filenameOverride;
        if (filename == null) {
            filename = InventoryUtil.computeArtifactFilename(artifact.getURI());
        }
        syncOutput.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");

        if (artifact.contentEncoding != null) {
            syncOutput.setHeader("Content-Encoding", artifact.contentEncoding);
        }
        if (artifact.contentType != null) {
            syncOutput.setHeader("Content-Type", artifact.contentType);
        }
        syncOutput.setHeader("Accept-Ranges", "bytes");
    }

    static void setTransactionHeaders(PutTransaction txn, SyncOutput syncOutput) {
        syncOutput.setHeader(PUT_TXN_ID, txn.getID());
        if (txn.getMinSegmentSize() != null) {
            syncOutput.setHeader(PUT_TXN_MIN_SIZE, txn.getMinSegmentSize());
        }
        if (txn.getMaxSegmentSize() != null) {
            syncOutput.setHeader(PUT_TXN_MAX_SIZE, txn.getMaxSegmentSize());
        }
    }
}

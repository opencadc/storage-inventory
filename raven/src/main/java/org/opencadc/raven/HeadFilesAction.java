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
************************************************************************
*/

package org.opencadc.raven;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.TokenTool;

/**
 * Interface with inventory to get the metadata of an artifact.
 *
 * @author adriand
 */
public class HeadFilesAction extends FilesAction {
    
    private static final Logger log = Logger.getLogger(HeadFilesAction.class);

    /**
     * Default, no-arg constructor.
     */
    public HeadFilesAction() {
        super();
    }

    /**
     * Response to the "files" HEAD with the artifact metadata as response headers.
     */
    @Override
    public void doAction() throws Exception {
        initAndAuthorize();
        log.debug("Starting HEAD action for " + artifactURI.toASCIIString());
        Artifact artifact = artifactDAO.get(artifactURI);
        
        if (artifact == null) {
            if (this.preventNotFound) {
                // check known storage sites
                ProtocolsGenerator pg = new ProtocolsGenerator(this.artifactDAO, this.publicKeyFile, this.privateKeyFile,
                        this.user, this.siteAvailabilities, this.siteRules, this.preventNotFound, this.storageResolver);
                StorageSiteDAO storageSiteDAO = new StorageSiteDAO(artifactDAO);
                Transfer transfer = new Transfer(artifactURI, Direction.pullFromVoSpace);
                Protocol proto = new Protocol(VOS.PROTOCOL_HTTPS_GET);
                proto.setSecurityMethod(Standards.SECURITY_METHOD_ANON);
                transfer.getProtocols().add(proto);
                TokenTool tk = new TokenTool(publicKeyFile, privateKeyFile);
                String authToken = tk.generateToken(artifactURI, ReadGrant.class, user);
                artifact = pg.getUnsyncedArtifact(artifactURI, transfer, storageSiteDAO.list(), authToken);
            }
        }
        
        if (artifact == null) {
            if (storageResolver != null) {
                // redirect to external site
                try {
                    URL externalURL = storageResolver.toURL(artifactURI);
                    if (externalURL != null) {
                        syncOutput.setCode(HttpURLConnection.HTTP_SEE_OTHER);
                        syncOutput.setHeader("Location", externalURL);
                        logInfo.setMessage("external redirect: " + externalURL.toExternalForm());
                        return;
                    }
                } catch (IllegalArgumentException e) {
                    // nothing to be done here
                }
            }
        }
        
        if (artifact == null) {
            throw new ResourceNotFoundException(artifactURI.toASCIIString());
        }
        
        setHeaders(artifact, syncOutput);
    }
    
    /**
     * Set the HTTP response headers for an artifact.
     * @param artifact The artifact with metadata
     * @param syncOutput The target response
     */
    public static void setHeaders(Artifact artifact, SyncOutput syncOutput) {
        syncOutput.setDigest(artifact.getContentChecksum());
        syncOutput.setLastModified(artifact.getContentLastModified());
        syncOutput.setHeader("Content-Length", artifact.getContentLength());
        String filename = InventoryUtil.computeArtifactFilename(artifact.getURI());
        syncOutput.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
        if (artifact.contentEncoding != null) {
            syncOutput.setHeader("Content-Encoding", artifact.contentEncoding);
        }
        if (artifact.contentType != null) {
            syncOutput.setHeader("Content-Type", artifact.contentType);
        }
    }

}

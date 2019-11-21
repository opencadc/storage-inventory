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


import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.AccessControlException;

import org.apache.log4j.Logger;

/**
 *
 * @author majorb
 */
public class GetAction extends ArtifactAction {
    private static final Logger log = Logger.getLogger(GetAction.class);

    public GetAction() {
        super();
    }

    @Override
    public void doAction() throws Exception {
        /*
        try {
            URI artifactURI = getArtifactURI();
            FileSystem fs = FileSystems.getDefault();
            Path source = fs.getPath(getRoot(), nodeURI.getPath());
            if (!Files.exists(source)) {
                // not found
                syncOutput.setCode(404);
                return;
            }
            if (!Files.isReadable(source)) {
                // permission denied
                syncOutput.setCode(403);
                return;
            }
            
            // set HTTP headers.  To get node, resolve links but no authorization (null authorizer) 
            NodePersistence nodePersistence = new FileSystemNodePersistence();
            PathResolver pathResolver = new PathResolver(nodePersistence, true);
            Node node = pathResolver.resolveWithReadPermissionCheck(nodeURI, null, true);
            String contentEncoding = node.getPropertyValue(VOS.PROPERTY_URI_CONTENTENCODING);
            String contentLength = node.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH);
            String contentMD5 = node.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5);
            syncOutput.setHeader("Content-Disposition", "inline; filename=" + nodeURI.getName());
            syncOutput.setHeader("Content-Type", node.getPropertyValue(VOS.PROPERTY_URI_TYPE));
            syncOutput.setHeader("Content-Encoding", contentEncoding);
            syncOutput.setHeader("Content-Length", contentLength);
            syncOutput.setHeader("Content-MD5", contentMD5);
            
            OutputStream out = syncOutput.getOutputStream();
            log.debug("Starting copy of file " + source);
            Files.copy(source, out);
            log.debug("Completed copy of file " + source);
            out.flush();
        } catch (FileNotFoundException | NoSuchFileException e) {
            log.debug(e);
            syncOutput.setCode(404);
        } catch (AccessControlException | AccessDeniedException e) {
            log.debug(e);
            syncOutput.setCode(403);
        }
        */
    }

    @Override
    public void execute(URI artifactURI) throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public AuthorizationType getAuthorizationType() {
        // TODO Auto-generated method stub
        return null;
    }
}

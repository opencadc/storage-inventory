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

import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.PropertiesReader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.security.AccessControlException;

import org.apache.log4j.Logger;

/**
 *
 * @author majorb
 */
public abstract class ArtifactAction extends RestAction {
    private static final Logger log = Logger.getLogger(ArtifactAction.class);
    
    protected enum AuthorizationType {
        READ, WRITE
    };

    URI artifactURI;
    String authToken;

    protected ArtifactAction() {
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }
    
    public abstract void execute(URI artifactURI) throws Exception;
    public abstract AuthorizationType getAuthorizationType();
    
    @Override
    public void doAction() throws Exception {
        
        parsePath();
        
        // do authorization (with token or subject)
        
    }
    
    void parsePath() {
        String path = syncInput.getPath();
        int colonIndex = path.indexOf(":");
        int firstSlashIndex = path.indexOf("/");
        
        if (colonIndex < 0) {
            if (firstSlashIndex > 0 && path.length() - 1 > firstSlashIndex) {
                throw new IllegalArgumentException("Missing scheme in artifactURI: " + path.substring(firstSlashIndex + 1));
            } else {
                throw new IllegalArgumentException("Missing artifactURI in path: " + path);
            }
        }
        
        int secondSlashIndex = path.indexOf("/", firstSlashIndex + 1);
        if (secondSlashIndex < 0 || secondSlashIndex > colonIndex) {
            // artifact URI is all after first slash
            createArtifactURI(path.substring(firstSlashIndex + 1));
            return;
        }
        
        // authToken between slashes, uri after second slash
        createArtifactURI(path.substring(secondSlashIndex + 1));
        authToken = path.substring(firstSlashIndex + 1, secondSlashIndex);
        log.debug("authToken: " + authToken);
    }
    
    private void createArtifactURI(String uri) {
        try {
            artifactURI = new URI(uri);
            
            // TODO: do this checking with InventoryUtil when available
            
            log.debug("artifactURI: " + artifactURI);
            if (artifactURI.getFragment() != null) {
                throw new IllegalArgumentException("fragments not allowed in artifactURIs: " + uri);
            }
            if (artifactURI.getQuery() != null) {
                throw new IllegalArgumentException("query params not allowed in artifactURIs: " + uri);
            }
            if (artifactURI.getHost() != null) {
                throw new IllegalArgumentException("host not allowed in artifactURIs: " + uri);
            }
            if (artifactURI.getPort() != -1) {
                throw new IllegalArgumentException("port not allowed in artifactURIs: " + uri);
            }
            if (artifactURI.getAuthority() != null) {
                throw new IllegalArgumentException("authority not allowed in artifactURIs: " + uri);
            }
        } catch (URISyntaxException e) {
            String message = "Illegal artifact URI: " + uri;
            log.debug(message, e);
            throw new IllegalArgumentException(message);
        }
    }

}

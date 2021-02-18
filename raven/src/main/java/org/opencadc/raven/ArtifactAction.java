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

package org.opencadc.raven;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferParsingException;
import ca.nrc.cadc.vos.TransferReader;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.server.PermissionsCheck;
import org.opencadc.permissions.Grant;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.TokenTool;
import org.opencadc.permissions.WriteGrant;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.security.cert.CertificateException;
import java.util.*;

/**
 * Abstract class for all that raven action classes have in common,
 * including request parsing, authentication, and authentication.
 *
 * @author adriand
 */
public abstract class ArtifactAction extends RestAction {
    private static final Logger log = Logger.getLogger(ArtifactAction.class);

    // The target artifact
    URI artifactURI;
    Transfer transfer;

    // The (possibly null) authentication token.
    String authToken;
    String user;

    // immutable state set in constructor
    protected final ArtifactDAO artifactDAO;
    protected final File publicKeyFile;
    protected final File privateKeyFile;
    protected final List<URI> readGrantServices = new ArrayList<>();
    protected final List<URI> writeGrantServices = new ArrayList<>();

    private static final String INLINE_CONTENT_TAG = "inputstream";
    private static final String CONTENT_TYPE = "text/xml";

    protected final boolean authenticateOnly;

    // constructor for unit tests with no config/init
    ArtifactAction(boolean init) {
        super();
        this.authenticateOnly = false;
        this.publicKeyFile = null;
        this.privateKeyFile = null;
        this.artifactDAO = null;
    }

    protected ArtifactAction() {
        super();
        MultiValuedProperties props = RavenInitAction.getConfig();

        List<String> readGrants = props.getProperty(RavenInitAction.READ_GRANTS_KEY);
        if (readGrants != null) {
            for (String s : readGrants) {
                try {
                    URI u = new URI(s);
                    readGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + RavenInitAction.READ_GRANTS_KEY + "=" + s + " must be a valid URI");
                }
            }
        }

        List<String> writeGrants = props.getProperty(RavenInitAction.WRITE_GRANTS_KEY);
        if (writeGrants != null) {
            for (String s : writeGrants) {
                try {
                    URI u = new URI(s);
                    writeGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + RavenInitAction.WRITE_GRANTS_KEY + "=" + s + " must be a valid URI");
                }
            }
        }
        
        String ao = props.getFirstPropertyValue(RavenInitAction.DEV_AUTH_ONLY_KEY);
        if (ao != null) {
            try {
                this.authenticateOnly = Boolean.valueOf(ao);
                log.warn("(configuration) authenticateOnly = " + authenticateOnly);
            } catch (Exception ex) {
                throw new IllegalStateException("invalid config: " + RavenInitAction.DEV_AUTH_ONLY_KEY + "=" + ao + " must be true|false or not set");
            }
        } else {
            authenticateOnly = false;
        }

        // technically, raven only needs the private key to generate pre-auth tokens
        // but both are requied here for clarity
        // - in principle, raven could export it's public key and minoc(s) could retrieve it
        // - for now, minoc(s) need to be configured with the public key to validate pre-auth

        String pubkeyFileName = props.getFirstPropertyValue(RavenInitAction.PUBKEYFILE_KEY);
        String privkeyFileName = props.getFirstPropertyValue(RavenInitAction.PRIVKEYFILE_KEY);
        this.publicKeyFile = new File(System.getProperty("user.home") + "/config/" + pubkeyFileName);
        this.privateKeyFile = new File(System.getProperty("user.home") + "/config/" + privkeyFileName);
        if (!publicKeyFile.exists() || !privateKeyFile.exists()) {
            throw new IllegalStateException("invalid config: missing public/private key pair files -- " + publicKeyFile + " | " + privateKeyFile);
        }

        Map<String, Object> config = RavenInitAction.getDaoConfig(props);
        this.artifactDAO = new ArtifactDAO();
        artifactDAO.setConfig(config); // connectivity tested

        // set the user for logging
        user = AuthMethod.ANON.toString();
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (authMethod != null && !authMethod.equals(AuthMethod.ANON)) {
            Set<String> userids = AuthenticationUtil.getUseridsFromSubject();
            if (userids.size() > 0) {
                user = userids.iterator().next();
            }
        }
    }

    protected void initAndAuthorize() throws Exception {
        init();

        Class grantClass = ReadGrant.class;
        if ((transfer != null) && (transfer.getDirection().equals(Direction.pushToVoSpace))) {
            grantClass = WriteGrant.class;
        }
        // do authorization (with token or subject)
        Subject subject = AuthenticationUtil.getCurrentSubject();
        if (authToken != null) {
            TokenTool tk = new TokenTool(publicKeyFile, privateKeyFile);
            String tokenUser = tk.validateToken(authToken, artifactURI, grantClass);
            subject.getPrincipals().clear();
            subject.getPrincipals().add(new HttpPrincipal(tokenUser));
            logInfo.setSubject(subject);
        } else {
            PermissionsCheck permissionsCheck = new PermissionsCheck(this.artifactURI, this.authenticateOnly,
                                                                     this.logInfo);
            if (ReadGrant.class.isAssignableFrom(grantClass)) {
                permissionsCheck.checkReadPermission(this.readGrantServices);
            } else if (WriteGrant.class.isAssignableFrom(grantClass)) {
                permissionsCheck.checkWritePermission(this.writeGrantServices);
            } else {
                throw new IllegalStateException("Unsupported grant class: " + grantClass);
            }
        }
    }
    
    void init() throws IOException, TransferParsingException {
        parsePath();
        parseContent();
        if (artifactURI == null) {
            throw new IllegalArgumentException("Missing artifact URI from path or request content");
        }
    }

    /**
     * Parse the request path.
     */
    void parsePath() {
        String path = syncInput.getPath();
        log.debug("path: " + path);
        if (path == null) {
            return;
        }
        int colonIndex = path.indexOf(":");
        int firstSlashIndex = path.indexOf("/");
        
        if (colonIndex < 0) {
            if (firstSlashIndex > 0 && path.length() > firstSlashIndex + 1) {
                throw new IllegalArgumentException(
                    "missing scheme in artifact URI: "
                        + path.substring(firstSlashIndex + 1));
            } else {
                throw new IllegalArgumentException(
                    "missing artifact URI in path: " + path);
            }
        }
        
        if (firstSlashIndex < 0 || firstSlashIndex > colonIndex) {
            // no auth token--artifact URI is complete path
            artifactURI = createArtifactURI(path);
            return;
        }
        
        artifactURI = createArtifactURI(path.substring(firstSlashIndex + 1));
        
        authToken = path.substring(0, firstSlashIndex);
        log.debug("authToken: " + authToken);
    }

    /**
     * Parse the content in case of a POST action
     */
    void parseContent() throws IOException, TransferParsingException {
        TransferReader reader = new TransferReader();
        InputStream in = (InputStream) syncInput.getContent(INLINE_CONTENT_TAG);
        if (in == null) {
            return;
        }
        transfer = reader.read(in, null);

        log.debug("transfer request: " + transfer);
        Direction direction = transfer.getDirection();
        if (!Direction.pullFromVoSpace.equals(direction) && !Direction.pushToVoSpace.equals(direction)) {
            throw new IllegalArgumentException("direction not supported: " + transfer.getDirection());
        }
        artifactURI = transfer.getTarget();
        InventoryUtil.validateArtifactURI(PostAction.class, artifactURI);
    }

    /**
     * Create a valid artifact uri.
     * @param uri The input string.
     * @return The artifact uri object.
     */
    private URI createArtifactURI(String uri) {
        try {
            log.debug("artifactURI: " + uri);
            artifactURI = new URI(uri);
            InventoryUtil.validateArtifactURI(ArtifactAction.class, artifactURI);
            return artifactURI;
        } catch (URISyntaxException e) {
            String message = "illegal artifact URI: " + uri;
            log.debug(message, e);
            throw new IllegalArgumentException(message);
        }
    }

    protected List<String> getReadGrantServices(MultiValuedProperties props) {
        String key = ReadGrant.class.getName() + ".resourceID";
        List<String> values = props.getProperty(key);
        if (values == null) {
            return Collections.emptyList();
        }
        return values;
    }
    
    protected List<String> getWriteGrantServices(MultiValuedProperties props) {
        String key = WriteGrant.class.getName() + ".resourceID";
        List<String> values = props.getProperty(key);
        if (values == null) {
            return Collections.emptyList();
        }
        return values;
    }
    
    static MultiValuedProperties readConfig() {
        PropertiesReader pr = new PropertiesReader("raven.properties");
        MultiValuedProperties props = pr.getAllProperties();

        if (log.isDebugEnabled()) {
            log.debug("raven.properties:");
            Set<String> keys = props.keySet();
            for (String key : keys) {
                log.debug("    " + key + " = " + props.getProperty(key));
            }
        }
        return props;
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
                if (!CONTENT_TYPE.equals(contentType)) {
                    throw new IllegalArgumentException("expecting text/xml input document");
                }
                Content content = new Content();
                content.name = INLINE_CONTENT_TAG;
                content.value = inputStream;
                return content;
            }
        };
    }
}

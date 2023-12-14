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

package org.opencadc.minoc;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.rest.SyncInput;
import ca.nrc.cadc.rest.Version;
import ca.nrc.cadc.util.MultiValuedProperties;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.permissions.Grant;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.TokenTool;
import org.opencadc.permissions.WriteGrant;
import org.opencadc.permissions.client.PermissionsCheck;

/**
 * Abstract class for performing tasks all action classes have in common,
 * including request parsing, authentication, and authentication.
 *
 * @author majorb
 */
public abstract class ArtifactAction extends RestAction {
    private static final Logger log = Logger.getLogger(ArtifactAction.class);
    
    static final String PUT_TXN_ID = "x-put-txn-id"; // request/response header
    static final String PUT_TXN_OP = "x-put-txn-op"; // request header
    static final String PUT_TXN_MIN_SIZE = "x-put-segment-minbytes"; // response header
    static final String PUT_TXN_MAX_SIZE = "x-put-segment-maxbytes"; // response header
    
    // PUT_TXN_OP values
    static final String PUT_TXN_OP_ABORT = "abort"; // request header
    static final String PUT_TXN_OP_COMMIT = "commit"; // request header
    static final String PUT_TXN_OP_REVERT = "revert"; // request header
    static final String PUT_TXN_OP_START = "start"; // request header
    
    // The target artifact
    URI artifactURI;
    
    // The (possibly null) authentication token.
    String authToken;

    // servlet path minus the auth token
    String loggablePath;
    
    protected MinocConfig config;
    
    // lazy init
    protected ArtifactDAO artifactDAO;
    protected StorageAdapter storageAdapter;
    
    // constructor for unit tests with no config/init
    ArtifactAction(boolean init) {
        super();
        this.config = null;
        this.artifactDAO = null;
        this.storageAdapter = null;
    }

    protected ArtifactAction() {
        super();
    }

    @Override
    public void initAction() throws Exception {
        super.initAction();
        this.config = MinocInitAction.getConfig(appName);
    }

    @Override
    protected String getServerImpl() {
        // no null version checking because fail to build correctly can't get past basic testing
        Version v = getVersionFromResource();
        String ret = "storage-inventory/minoc-" + v.getMajorMinor();
        return ret;
    }

    /**
     * Default implementation.
     * @return No InlineContentHander
     */
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

    // override setLogInfo and setSyncInput so we can reset the path before START
    // logging to not include the preauth-token (if present) but keep the artifact uri
    @Override
    public void setLogInfo(WebServiceLogInfo logInfo) {
        super.setLogInfo(logInfo);
        if (this.artifactURI != null && this.loggablePath != null) {
            this.logInfo.setPath(this.loggablePath + "/" + this.artifactURI.toASCIIString());
        }
    }

    @Override
    public void setSyncInput(SyncInput syncInput) {
        super.setSyncInput(syncInput);
        this.loggablePath = syncInput.getContextPath() + syncInput.getComponentPath();
        parsePath();
        if (this.artifactURI != null && this.logInfo != null) {
            this.logInfo.setPath(this.loggablePath + "/" + this.artifactURI.toASCIIString());
        }
    }

    protected void initAndAuthorize(Class<? extends Grant> grantClass)
        throws AccessControlException, CertificateException, IOException, InterruptedException,
               ResourceNotFoundException, TransientException {
        initAndAuthorize(grantClass, false);
    }
    
    @SuppressWarnings("unchecked")
    protected void initAndAuthorize(Class<? extends Grant> grantClass, boolean allowReadWithWriteGrant)
        throws AccessControlException, CertificateException, IOException, InterruptedException,
               ResourceNotFoundException, TransientException {

        init();

        // do authorization (with token or subject)
        Subject subject = AuthenticationUtil.getCurrentSubject();
        if (authToken != null) {
            //if (publicKey == null) {
            //    throw new IllegalArgumentException("unexpected pre-auth token in URL");
            //}
            //TokenTool tk = new TokenTool(publicKey);
            for (Map.Entry<URI,byte[]> me : config.getTrustedServices().entrySet()) {
                URI ts = me.getKey();
                if (me.getValue() != null) {
                    TokenTool tk = new TokenTool(me.getValue());
                    log.warn("validate preauth with key from " + me.getKey());
                    try {
                        String tokenUser;
                        if (allowReadWithWriteGrant && ReadGrant.class.isAssignableFrom(grantClass)) {
                            // treat a WriteGrant as also granting read permission
                            tokenUser = tk.validateToken(authToken, artifactURI, grantClass, WriteGrant.class);
                        } else {
                            tokenUser = tk.validateToken(authToken, artifactURI, grantClass);
                        }
                        subject.getPrincipals().clear();
                        if (tokenUser != null) {
                            subject.getPrincipals().add(new HttpPrincipal(tokenUser));
                        }
                        logInfo.setSubject(subject);
                        logInfo.setResource(artifactURI);
                        logInfo.setPath(syncInput.getContextPath() + syncInput.getComponentPath());
                        if (ReadGrant.class.isAssignableFrom(grantClass)) {
                            logInfo.setGrant("read: preauth-token from " + ts);
                        } else if (WriteGrant.class.isAssignableFrom(grantClass)) {
                            logInfo.setGrant("write: preauth-token from " + ts);
                        } else {
                            throw new IllegalStateException("Unsupported grant class: " + grantClass);
                        }
                        return;
                    } catch (AccessControlException ex) {
                        log.warn("token invalid vs keys from " + ts, ex);
                    }
                } else {
                    log.warn("no keys from " + ts + " -- SKIP");
                }
                // no return from inside check
                throw new AccessControlException("invalid auth token");
            }
        }
            
        // augment subject (minoc is configured so augment is not done in rest library)
        AuthenticationUtil.augmentSubject(subject);
        logInfo.setSubject(subject);
        logInfo.setResource(artifactURI);
        logInfo.setPath(syncInput.getContextPath() + syncInput.getComponentPath());
        PermissionsCheck permissionsCheck = new PermissionsCheck(artifactURI, config.isAuthenticateOnly(), logInfo);
        // TODO: allowReadWithWriteGrant could be implemented here, but grant services are probably configured
        // that way already so it's complexity that probably won't allow/enable any actions
        if (ReadGrant.class.isAssignableFrom(grantClass)) {
            permissionsCheck.checkReadPermission(config.getReadGrantServices());
        } else if (WriteGrant.class.isAssignableFrom(grantClass)) {
            permissionsCheck.checkWritePermission(config.getWriteGrantServices());
        } else {
            throw new IllegalStateException("Unsupported grant class: " + grantClass);
        }
    }

    void init() {
        if (this.artifactURI == null) {
            throw new IllegalArgumentException("missing or invalid artifact URI");
        }
    }

    protected void initDAO() {
        if (artifactDAO == null) {
            Map<String, Object> configMap = config.getDaoConfig();
            this.artifactDAO = new ArtifactDAO();
            artifactDAO.setConfig(configMap); // connectivity tested
        }
    }
    
    protected void initStorageAdapter() {
        if (storageAdapter == null) {
            this.storageAdapter = config.getStorageAdapter();
        }
    }
    
    /**
     * Parse the request path.
     */
    void parsePath() {
        String path = this.syncInput.getPath();
        log.debug("path: " + path);
        if (path != null) {
            int colonIndex = path.indexOf(":");
            int firstSlashIndex = path.indexOf("/");
            if (colonIndex != -1) {
                if (firstSlashIndex < 0 || firstSlashIndex > colonIndex) {
                    // no auth token--artifact URI is complete path
                    this.artifactURI = createArtifactURI(path);
                } else {
                    this.artifactURI = createArtifactURI(path.substring(firstSlashIndex + 1));
                    this.authToken = path.substring(0, firstSlashIndex);
                    log.debug("authToken: " + this.authToken);
                }
            }
        }
    }
    
    Artifact getArtifact(URI artifactURI) throws ResourceNotFoundException {
        Artifact artifact = artifactDAO.get(artifactURI);
        if (artifact == null || artifact.storageLocation == null) {
            throw new ResourceNotFoundException("not found: " + artifactURI);
        }
        return artifact;
    }
    
    /**
     * Create a valid artifact uri.
     * @param uri The input string.
     * @return The artifact uri object.
     */
    private URI createArtifactURI(String uri) {
        log.debug("artifact URI: " + uri);
        URI ret;
        try {
            ret = new URI(uri);
            InventoryUtil.validateArtifactURI(ArtifactAction.class, ret);
        } catch (URISyntaxException | IllegalArgumentException e) {
            ret = null;
            log.debug("illegal artifact URI: " + uri, e);
        }
        return ret;
    }
}

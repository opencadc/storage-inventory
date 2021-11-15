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

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.rest.SyncInput;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.server.PermissionsCheck;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.permissions.Grant;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.TokenTool;
import org.opencadc.permissions.WriteGrant;

/**
 * Abstract class for performing tasks all action classes have in common,
 * including request parsing, authentication, and authentication.
 *
 * @author majorb
 */
public abstract class ArtifactAction extends RestAction {
    private static final Logger log = Logger.getLogger(ArtifactAction.class);
    
    // The target artifact
    URI artifactURI;
    
    // The (possibly null) authentication token.
    String authToken;

    // servlet path minus the auth token
    String loggablePath;
    
    // immutable state set in constructor
    protected final MultiValuedProperties config;
    protected final File publicKey;
    protected final List<URI> readGrantServices = new ArrayList<>();
    protected final List<URI> writeGrantServices = new ArrayList<>();
    
    // lazy init
    protected ArtifactDAO artifactDAO;
    protected StorageAdapter storageAdapter;
    
    private final boolean authenticateOnly;

    // constructor for unit tests with no config/init
    ArtifactAction(boolean init) {
        super();
        this.config = null;
        this.artifactDAO = null;
        this.storageAdapter = null;
        this.authenticateOnly = false;
        this.publicKey = null;
    }

    protected ArtifactAction() {
        super();
        this.config = MinocInitAction.getConfig();

        List<String> readGrants = config.getProperty(MinocInitAction.READ_GRANTS_KEY);
        if (readGrants != null) {
            for (String s : readGrants) {
                try {
                    URI u = new URI(s);
                    readGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + MinocInitAction.READ_GRANTS_KEY + "=" + s + " INVALID", ex);
                }
            }
        }

        List<String> writeGrants = config.getProperty(MinocInitAction.WRITE_GRANTS_KEY);
        if (writeGrants != null) {
            for (String s : writeGrants) {
                try {
                    URI u = new URI(s);
                    writeGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + MinocInitAction.WRITE_GRANTS_KEY + "=" + s + " INVALID", ex);
                }
            }
        }
        
        String ao = config.getFirstPropertyValue(MinocInitAction.DEV_AUTH_ONLY_KEY);
        if (ao != null) {
            try {
                this.authenticateOnly = Boolean.valueOf(ao);
                log.warn("(configuration) authenticateOnly = " + authenticateOnly);
            } catch (Exception ex) {
                throw new IllegalStateException("invalid config: " + MinocInitAction.DEV_AUTH_ONLY_KEY + "=" + ao + " must be true|false or not set");
            }
        } else {
            authenticateOnly = false;
        }

        String pubkeyFileName = config.getFirstPropertyValue(MinocInitAction.PUBKEYFILE_KEY);
        this.publicKey = new File(System.getProperty("user.home") + "/config/" + pubkeyFileName);
    }

    /**
     * Default implementation.
     * @return No InlineContentHander
     */
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

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
        this.loggablePath = syncInput.getComponentPath();
        parsePath();
        if (this.artifactURI != null && this.logInfo != null) {
            this.logInfo.setPath(this.loggablePath + "/" + this.artifactURI.toASCIIString());
        }
    }

    protected void initAndAuthorize(Class<? extends Grant> grantClass)
        throws AccessControlException, CertificateException, IOException,
               ResourceNotFoundException, TransientException {

        init();

        // do authorization (with token or subject)
        Subject subject = AuthenticationUtil.getCurrentSubject();
        if (authToken != null) {
            TokenTool tk = new TokenTool(publicKey);
            String tokenUser = tk.validateToken(authToken, artifactURI, grantClass);
            subject.getPrincipals().clear();
            if (tokenUser != null) {
                subject.getPrincipals().add(new HttpPrincipal(tokenUser));
            }
            logInfo.setSubject(subject);
        } else {
            // augment subject (minoc is configured so augment
            // is not done in rest library)
            AuthenticationUtil.augmentSubject(subject);
            logInfo.setSubject(subject);
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

    void init() {
        if (this.artifactURI == null) {
            throw new IllegalArgumentException("missing or invalid artifact URI");
        }
    }

    protected void initDAO() {
        if (artifactDAO == null) {
            Map<String, Object> configMap = MinocInitAction.getDaoConfig(config);
            this.artifactDAO = new ArtifactDAO();
            artifactDAO.setConfig(configMap); // connectivity tested
        }
    }
    
    protected void initStorageAdapter() {
        if (storageAdapter == null) {
            this.storageAdapter = InventoryUtil.loadPlugin(config.getFirstPropertyValue(MinocInitAction.SA_KEY));
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
        if (artifact == null) {
            throw new ResourceNotFoundException("not found: " + artifactURI);
        }
        if (artifact.storageLocation == null) {
            throw new ResourceNotFoundException("not available: " + artifactURI);
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
        PropertiesReader pr = new PropertiesReader("minoc.properties");
        MultiValuedProperties props = pr.getAllProperties();

        if (log.isDebugEnabled()) {
            log.debug("minoc.properties:");
            Set<String> keys = props.keySet();
            for (String key : keys) {
                log.debug("    " + key + " = " + props.getProperty(key));
            }
        }
        return props;
    }
    
    static Map<String, Object> getDaoConfig(MultiValuedProperties props) {
        Map<String, Object> config = new HashMap<String, Object>();
        Class cls = null;
        List<String> sqlGenList = props.getProperty(MinocInitAction.SQLGEN_KEY);
        if (sqlGenList != null && sqlGenList.size() > 0) {
            try {
                String sqlGenClass = sqlGenList.get(0);
                cls = Class.forName(sqlGenClass);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(
                    "could not load SQLGenerator class: " + e.getMessage(), e);
            }
        } else {
            // use the default SQL generator
            cls = SQLGenerator.class;
        }

        config.put(MinocInitAction.SQLGEN_KEY, cls);
        config.put("jndiDataSourceName", MinocInitAction.JNDI_DATASOURCE);
        List<String> schemaList = props.getProperty(MinocInitAction.SCHEMA_KEY);
        if (schemaList == null || schemaList.size() < 1) {
            throw new IllegalStateException("a value for " + MinocInitAction.SCHEMA_KEY
                + " is needed in minoc.properties");
        }
        config.put("schema", schemaList.get(0));
        config.put("database", null);

        return config;
    }

}

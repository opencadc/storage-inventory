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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.opencadc.gms.GroupClient;
import org.opencadc.gms.GroupURI;
import org.opencadc.gms.GroupUtil;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.permissions.Grant;
import org.opencadc.inventory.permissions.PermissionsClient;
import org.opencadc.inventory.permissions.ReadGrant;
import org.opencadc.inventory.permissions.TokenUtil;
import org.opencadc.inventory.permissions.WriteGrant;
import org.opencadc.inventory.storage.StorageAdapter;

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
    
    // immutable state set in constructor
    protected final ArtifactDAO artifactDAO;
    protected final StorageAdapter storageAdapter;
    protected final List<URI> readGrantServices = new ArrayList<>();
    protected final List<URI> writeGrantServices = new ArrayList<>();
    
    private final boolean authenticateOnly;

    // constructor for unit tests with no config/init
    ArtifactAction(boolean init) {
        super();
        this.artifactDAO = null;
        this.storageAdapter = null;
        this.authenticateOnly = false;
    }

    protected ArtifactAction() {
        super();
        MultiValuedProperties props = InitDatabaseAction.getConfig();

        List<String> readGrants = props.getProperty(InitDatabaseAction.READ_GRANTS_KEY);
        if (readGrants != null) {
            for (String s : readGrants) {
                try {
                    URI u = new URI(s);
                    readGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + InitDatabaseAction.READ_GRANTS_KEY + "=" + s + " must be a valid URI");
                }
            }
        }

        List<String> writeGrants = props.getProperty(InitDatabaseAction.WRITE_GRANTS_KEY);
        if (writeGrants != null) {
            for (String s : writeGrants) {
                try {
                    URI u = new URI(s);
                    writeGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + InitDatabaseAction.WRITE_GRANTS_KEY + "=" + s + " must be a valid URI");
                }
            }
        }
        
        String ao = props.getFirstPropertyValue(InitDatabaseAction.DEV_AUTH_ONLY_KEY);
        if (ao != null) {
            try {
                this.authenticateOnly = Boolean.valueOf(ao);
                log.warn("(configuration) authenticateOnly = " + authenticateOnly);
            } catch (Exception ex) {
                throw new IllegalStateException("invalid config: " + InitDatabaseAction.DEV_AUTH_ONLY_KEY + "=" + ao + " must be true|false or not set");
            }
        } else {
            authenticateOnly = false;
        }
        
        
        Map<String, Object> config = InitDatabaseAction.getDaoConfig(props);
        this.artifactDAO = new ArtifactDAO();
        artifactDAO.setConfig(config); // connectivity tested

        this.storageAdapter = InventoryUtil.loadPlugin(props.getFirstPropertyValue(InitDatabaseAction.SA_KEY));
    }

    /**
     * Default implementation.
     * @return No InlineContentHander
     */
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }
    
    protected void initAndAuthorize(Class<? extends Grant> grantClass)
        throws AccessControlException, IOException,
               ResourceNotFoundException, TransientException {
        
        init();
        
        // do authorization (with token or subject)
        Subject subject = AuthenticationUtil.getCurrentSubject();
        if (authToken != null) {
            String tokenUser = TokenUtil.validateToken(
                authToken, artifactURI, grantClass);
            subject.getPrincipals().clear();
            subject.getPrincipals().add(new HttpPrincipal(tokenUser));
            logInfo.setSubject(subject);
        } else {
            // augment subject (minoc is configured so augment
            // is not done in rest library)
            AuthenticationUtil.augmentSubject(subject);
            if (ReadGrant.class.isAssignableFrom(grantClass)) {
                checkReadPermission();
            } else if (WriteGrant.class.isAssignableFrom(grantClass)) {
                checkWritePermission();
            } else {
                throw new IllegalStateException("Unsupported grant class: " + grantClass);
            }
        }
    }
    
    void init() {
        parsePath();
    }
        
    public void checkReadPermission()
        throws AccessControlException, TransientException {
        
        if (authenticateOnly) {
            log.warn(InitDatabaseAction.DEV_AUTH_ONLY_KEY + "=true: allowing unrestricted access");
            return;
        }
        
        // TODO: could call multiple services in parallel
        Set<GroupURI> granted = new TreeSet<>();
        for (URI ps : readGrantServices) {
            try {
                PermissionsClient pc = new PermissionsClient(ps);
                ReadGrant grant = pc.getReadGrant(artifactURI);
                if (grant != null) {
                    if (grant.isAnonymousAccess()) {
                        logInfo.setMessage("read grant: anonymous");
                        return;
                    }
                    granted.addAll(grant.getGroups());
                }
            } catch (ResourceNotFoundException ex) {
                log.warn("failed to find granting service: " + ps + " -- cause: " + ex);
            }
        }
        if (granted.isEmpty()) {
            throw new AccessControlException("permission denied: no read grants for " + artifactURI);
        }
        
        // TODO: add profiling
        // if the granted group list is small, it would be better to use GroupClient.isMember()
        // rather than getting all groups... experiment to determine threshold? 
        // unfortunately, the speed of GroupClient.getGroups() will depend on how many groups the
        // caller belomgs to...
        LocalAuthority loc = new LocalAuthority();
        URI resourceID = loc.getServiceURI(Standards.GMS_SEARCH_01.toString());
        GroupClient client = GroupUtil.getGroupClient(resourceID);
        List<GroupURI> userGroups = client.getMemberships();
        for (GroupURI gg : granted) {
            for (GroupURI userGroup : userGroups) {
                if (gg.equals(userGroup)) {
                    logInfo.setMessage("read grant: " + gg);
                    return;
                }
            }
        }
        
        throw new AccessControlException("permission denied");
    }
    
    public void checkWritePermission()
        throws AccessControlException, TransientException {
        log.warn("checkWritePermission: authenticateOnly = " + authenticateOnly);

        AuthMethod am = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (am != null && am.equals(AuthMethod.ANON)) {
            // never support anon write
            throw new AccessControlException("permission denied");
        }
        
        if (authenticateOnly) {
            log.warn(InitDatabaseAction.DEV_AUTH_ONLY_KEY + "=true: allowing unrestricted access");
            return;
        }
        
        Set<GroupURI> granted = new TreeSet<>();

        // TODO: could call multiple services in parallel
        for (URI ps : writeGrantServices) {
            try {
                PermissionsClient pc = new PermissionsClient(ps);
                WriteGrant grant = pc.getWriteGrant(artifactURI);
                if (grant != null) {
                    granted.addAll(grant.getGroups());
                }
            } catch (ResourceNotFoundException ex) {
                log.warn("failed to find granting service: " + ps + " -- cause: " + ex);
            }
        }
        if (granted.isEmpty()) {
            throw new AccessControlException("permission denied: no write grants for " + artifactURI);
        }
        
        // TODO: add profiling
        // if the granted group list is small, it would be better to use GroupClient.isMember()
        // rather than getting all groups...
        // the speed of GroupClient.getGroups() will depend on how many groups the caller belongs to... 
        // for write permission we expect granted to be small and caller to be an operations or staff 
        // member so lots of memberships and few or 1 granted group: assume isMember() is better
        LocalAuthority loc = new LocalAuthority();
        URI resourceID = loc.getServiceURI(Standards.GMS_SEARCH_01.toString());
        GroupClient client = GroupUtil.getGroupClient(resourceID);
        for (GroupURI gg : granted) {
            if (client.isMember(gg)) {
                logInfo.setMessage("write grant: " + gg);
                return;
            }
        }
        
        throw new AccessControlException("permission denied");
    }
    
    /**
     * Parse the request path.
     */
    void parsePath() {
        String path = syncInput.getPath();
        log.debug("path: " + path);
        if (path == null) {
            throw new IllegalArgumentException("missing artifact URI");
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

    static StorageAdapter getStorageAdapter(MultiValuedProperties config) {
        // lazy init
        String adapterKey = StorageAdapter.class.getName();
        List<String> adapterList = config.getProperty(adapterKey);
        String adapterClass = null;
        if (adapterList != null && adapterList.size() > 0) {
            adapterClass = adapterList.get(0);
        } else {
            throw new IllegalStateException("no storage adapter specified in minoc.properties");
        }
        try {
            Class c = Class.forName(adapterClass);
            Object o = c.newInstance();
            StorageAdapter sa = (StorageAdapter) o;
            log.debug("StorageAdapter: " + sa);
            return sa;
        } catch (Throwable t) {
            throw new IllegalStateException("failed to load storage adapter: " + adapterClass, t);
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
        List<String> sqlGenList = props.getProperty(InitDatabaseAction.SQLGEN_KEY);
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

        config.put(InitDatabaseAction.SQLGEN_KEY, cls);
        config.put("jndiDataSourceName", InitDatabaseAction.JNDI_DATASOURCE);
        List<String> schemaList = props.getProperty(InitDatabaseAction.SCHEMA_KEY);
        if (schemaList == null || schemaList.size() < 1) {
            throw new IllegalStateException("a value for " + InitDatabaseAction.SCHEMA_KEY
                + " is needed in minoc.properties");
        }
        config.put("schema", schemaList.get(0));
        config.put("database", null);

        return config;
    }
}

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
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedEventDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageSiteDAO;
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
    
    public static final String JNDI_DATASOURCE = "jdbc/inventory";
    static final String SQL_GEN_KEY = SQLGenerator.class.getName();
    static final String SCHEMA_KEY = SQLGenerator.class.getPackage().getName() + ".schema";
    static final String RESOURCE_ID_KEY = "org.opencadc.minoc.resourceID";
    
    // The target artifact
    URI artifactURI;
    
    // The (possibly null) authentication token.
    String authToken;
    
    // minoc properties config
    MultiValuedProperties props = null;

    /**
     * Default implementation.
     * @return No InlineContentHander
     */
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }
    
    protected void initAndAuthorize(Class<? extends Grant> grantClass)
        throws AccessControlException, IOException, ResourceNotFoundException, TransientException {
        
        init();
        
        // do authorization (with token or subject)
        Subject subject = AuthenticationUtil.getCurrentSubject();
        if (authToken != null) {
            String tokenUser = TokenUtil.validateToken(authToken, artifactURI, grantClass);
            subject.getPrincipals().clear();
            subject.getPrincipals().add(new HttpPrincipal(tokenUser));
            logInfo.setSubject(subject);
        } else {
            // augment subject (minoc is configured so augment is not done in rest library)
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
        if (props == null) {
            this.props = readConfig();
        }
        initStorageSite();
    }
        
    // HACK: lazy single init in first request thread
    private static URI SELF_RESOURCE_ID;
    
    private void initStorageSite() {
        if (SELF_RESOURCE_ID != null) {
            // HACK: already did the init
            return;
        }
        List<String> rids = props.getProperty(RESOURCE_ID_KEY);
        if (rids.size() != 1) {
            throw new IllegalStateException("CONFIG: found " + rids.size() + " values for " + RESOURCE_ID_KEY + " in config; expected 1");
        }
        String rid = rids.get(0);
        try {
            URI resourceID = new URI(rid);
            StorageSiteDAO ssdao = new StorageSiteDAO();
            ssdao.setConfig(getDaoConfig(props));
            List<StorageSite> curlist = ssdao.list();
            if (curlist.size() > 1) {
                throw new IllegalStateException("found: " + curlist.size() + " StorageSite(s) in database; expected 0 or 1");
            }
            // TODO: get display name from config
            // use path from resourceID as default
            String name = resourceID.getPath();
            if (name.charAt(0) == '/') {
                name = name.substring(1);
            }
            
            if (curlist.isEmpty()) {
                StorageSite self = new StorageSite(resourceID, name);
                ssdao.put(self);
            } else {
                StorageSite cur = curlist.get(0);
                cur.setResourceID(resourceID);
                cur.setName(name);
                ssdao.put(cur);
            }
            log.info("initStorageSite: " + resourceID + " " + name);
            SELF_RESOURCE_ID = resourceID;
        } catch (URISyntaxException ex) {
            throw new IllegalStateException("CONFIG: invalid " + RESOURCE_ID_KEY + " = " + rid, ex);
        }
    }
    
    public void checkReadPermission() throws AccessControlException, ResourceNotFoundException, TransientException {
        List<String> readGrantServices = getReadGrantServices(props);
        PermissionsClient pc = null;

        /*
        // TODO: optimize with threads
        for (String readService : readGrantServices) {
            try {
                URI serviceID = new URI(readService);
                // TODO: add this argument to PermissionsClient constructor
                pc = new PermissionsClient(serviceID);
                
                // uncomment when ready
                pc = new PermissionsClient();
                ReadGrant grant = pc.getReadGrant(artifactURI);
                if (grant.isAnonymousAccess()) {
                    log.debug("anonymous read access granted");
                    return;
                }
                if (grant.getGroups().size() > 0) {
                    // TODO: check group membership
                    return;
                }
                return;
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Invalid read grant service: " + readService);
            }
        }
        throw new AccessControlException("read permission denied");
        */
    }
    
    public void checkWritePermission() throws AccessControlException, ResourceNotFoundException, TransientException {
        List<String> writeGrantServices = getWriteGrantServices(props);
        PermissionsClient pc = null;
        
        // current write auth is simply to be non-anonymous
        AuthMethod am = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (am != null && !am.equals(AuthMethod.ANON)) {
            return;
        }
        
        /*
        // TODO: optimize with threads
        for (String writeService : writeGrantServices) {
            try {
                URI serviceID = new URI(writeService);
                // TODO: add this argument to PermissionsClient constructor
                pc = new PermissionsClient(serviceID);
                
                // uncomment when ready
                pc = new PermissionsClient();
                WriteGrant grant = pc.getWriteGrant(artifactURI);
                if (grant.getGroups().size() > 0) {
                    // TODO: check group membership
                    return;
                }
                return;
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Invalid read grant service: " + writeService);
            }
        }
        */
        throw new AccessControlException("write permission denied");
    }
    
    /**
     * Parse the request path.
     */
    void parsePath() {
        String path = syncInput.getPath();
        log.debug("path: " + path);
        if (path == null) {
            throw new IllegalArgumentException("mising artifact URI");
        }
        int colonIndex = path.indexOf(":");
        int firstSlashIndex = path.indexOf("/");
        
        if (colonIndex < 0) {
            if (firstSlashIndex > 0 && path.length() > firstSlashIndex + 1) {
                throw new IllegalArgumentException("missing scheme in artifact URI: " + path.substring(firstSlashIndex + 1));
            } else {
                throw new IllegalArgumentException("missing artifact URI in path: " + path);
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
    
    Artifact getArtifact(URI artifactURI, ArtifactDAO dao) throws ResourceNotFoundException {
        Artifact artifact = dao.get(artifactURI);
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
    
    protected StorageAdapter getStorageAdapter() {
        return getStorageAdapter(props);
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
    
    protected ArtifactDAO getArtifactDAO() {
        ArtifactDAO dao = new ArtifactDAO();
        dao.setConfig(getDaoConfig(props));
        return dao;
    }
    
    protected List<String> getReadGrantServices(MultiValuedProperties props) {
        String key = ReadGrant.class.getName() + ".serviceID";
        List<String> values = props.getProperty(key);
        if (values == null) {
            return Collections.emptyList();
        }
        return values;
    }
    
    protected List<String> getWriteGrantServices(MultiValuedProperties props) {
        String key = WriteGrant.class.getName() + ".serviceID";
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
        List<String> sqlGenList = props.getProperty(SQL_GEN_KEY);
        if (sqlGenList != null && sqlGenList.size() > 0) {
            try {
                String sqlGenClass = sqlGenList.get(0);
                cls = Class.forName(sqlGenClass);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("could not load SQLGenerator class: " + e.getMessage(), e);
            }
        } else {
            // use the default SQL generator
            cls = SQLGenerator.class;
        }

        config.put(SQL_GEN_KEY, cls);
        config.put("jndiDataSourceName", JNDI_DATASOURCE);
        List<String> schemaList = props.getProperty(SCHEMA_KEY);
        if (schemaList == null || schemaList.size() < 1) {
            throw new IllegalStateException("a value for " + SCHEMA_KEY + " is needed"
                + " in minoc.properties");
        }
        config.put("schema", schemaList.get(0));
        config.put("database", null); 
            
        return config;
    }

}

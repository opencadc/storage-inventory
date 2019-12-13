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
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.PropertiesReader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedEventDAO;
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
    
    public static final String JNDI_DATASOURCE = "jdbc/inventory";
    static final String SQL_GEN_KEY = SQLGenerator.class.getName();
    static final String DATABASE_KEY = SQLGenerator.class.getPackage().toString() + ".database";
    static final String SCHEMA_KEY = SQLGenerator.class.getPackage().toString() + ".schema";
    
    // The target artifact
    URI artifactURI;
    
    // The (possibly null) authentication token.
    String authToken;
    
    // interface to storage
    private StorageAdapter storage = null;
    
    // Database Access Objects
    private ArtifactDAO artifactDAO = null;
    private DeletedEventDAO deletedEventDAO = null;

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

            // TODO: consult the two permissions services (ams and cadc-write) and
            // consolidate the responses.
            PermissionsClient pc = new PermissionsClient();
            if (ReadGrant.class.isAssignableFrom(grantClass)) {
                ReadGrant grant = pc.getReadGrant(artifactURI);
                //if (grant == null) {
                //    throw new AccessControlException("no grant information on artifact");
                //}
                // TODO: complete auth check
            } else if (WriteGrant.class.isAssignableFrom(grantClass)) {
                WriteGrant grant = pc.getWriteGrant(artifactURI);
                //if (grant == null) {
                //    throw new AccessControlException("no grant information on artifact");
                //}
                // TODO: complete auth check
            } else {
                throw new IllegalStateException("Unsupported grant class: " + grantClass);
            }
        }
    }
    
    /**
     * Parse the request path.
     */
    void init() {
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
     * @return The artifact uri objecdt.
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
        // lazy init
        if (storage == null) {
            PropertiesReader pr = new PropertiesReader("minoc.properties");
            String adapterKey = StorageAdapter.class.getName();
            String adapterClass = pr.getFirstPropertyValue(adapterKey);
            if (adapterClass == null) {
                throw new IllegalStateException("no storage adapter specified in minoc.properties");
            }
            try {
                Class c = Class.forName(adapterClass);
                Object o = c.newInstance();
                StorageAdapter sa = (StorageAdapter) o;
                log.debug("StorageAdapter: " + sa);
                storage = sa;
                return sa;
            } catch (Throwable t) {
                throw new IllegalStateException("failed to load storage adapter: " + adapterClass, t);
            }
        }
        return storage;
    }
    
    protected ArtifactDAO getArtifactDAO() {
        // lazy init
        if (artifactDAO == null) {
            ArtifactDAO newDAO = new ArtifactDAO();
            newDAO.setConfig(getDaoConfig());
            artifactDAO = newDAO;
        }
        return artifactDAO;
    }
    
    protected DeletedEventDAO getDeletedEventDAO(ArtifactDAO src) {
        // lazy init
        if (deletedEventDAO == null) {
            DeletedEventDAO newDAO = new DeletedEventDAO(src);
            deletedEventDAO = newDAO;
        }
        return deletedEventDAO;
    }
    
    static Map<String, Object> getDaoConfig() {
        Map<String, Object> config = new HashMap<String, Object>();
        PropertiesReader pr = new PropertiesReader("minoc.properties");
        Class cls = null;
        try {
            String sqlGenClass = pr.getFirstPropertyValue(SQL_GEN_KEY);
            if (sqlGenClass == null) {
                throw new IllegalStateException("a value for " + SQLGenerator.class.getName()
                    + " is needed in minoc.properties");
            }
            cls = Class.forName(sqlGenClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("could not load SQLGenerator class: " + e.getMessage(), e);
        }
        config.put(SQL_GEN_KEY, cls);
        config.put("jndiDataSourceName", JNDI_DATASOURCE);
        String database = pr.getFirstPropertyValue(DATABASE_KEY);
        String schema = pr.getFirstPropertyValue(SCHEMA_KEY);
        if (database == null || schema == null) {
            throw new IllegalStateException("values for " + DATABASE_KEY + " and "
                + SCHEMA_KEY + " are needed in minoc.properties");
        }
        config.put("database", database);
        config.put("schema", schema);
            
        return config;
    }

}

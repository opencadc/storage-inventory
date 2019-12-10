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
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.PropertiesReader;

import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.minoc.ArtifactUtil.HttpMethod;

/**
 * Abstract class for performing tasks all action classes have in common,
 * including request parsing, authentication, and authentication.
 *
 * @author majorb
 */
public abstract class ArtifactAction extends RestAction {
    private static final Logger log = Logger.getLogger(ArtifactAction.class);
    
    public static final String JNDI_DATASOURCE = "jdbc/inventory";
    public static final String DATABASE = "content";
    public static final String SCHEMA = "inventory";
    
    // The target artifact
    URI artifactURI;
    
    // The (possibly null) authentication token.
    String authToken;
    
    // The current http method
    private HttpMethod httpMethod;
    
    // interface to storage
    private StorageAdapter storage = null;
    
    // artifact dao
    private ArtifactDAO dao = null;

    /**
     * Default, no-arg constructor.
     */
    protected ArtifactAction(HttpMethod httpMethod) {
        this.httpMethod = httpMethod;
    }

    /**
     * Default implementation.
     * @return No InlineContentHander
     */
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }
    
    /**
     * Do the work of the subclass.
     * 
     * @param artifactURI The target artifact
     * @throws Exception If an something goes wrong.
     */
    public abstract Artifact execute(URI artifactURI) throws Exception;
    
    /**
     * Do authorization and perform the action.
     */
    @Override
    public void doAction() throws Exception {
        
        parsePath();
        
        // do authorization (with token or subject)
        if (authToken != null) {
            String tokenUser = ArtifactUtil.validateToken(authToken, artifactURI, httpMethod);
            Subject subject = AuthenticationUtil.getCurrentSubject();
            if (subject == null) {
                subject = new Subject();
            }
            subject.getPrincipals().clear();
            subject.getPrincipals().add(new HttpPrincipal(tokenUser));
            logInfo.setSubject(subject);
        } else {
            // TODO get permissions and perform authorization
        }
        
        execute(artifactURI);
        
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
        if (dao == null) {
            ArtifactDAO newDAO = new ArtifactDAO();
            Map<String, Object> config = new HashMap<String, Object>();
            PropertiesReader pr = new PropertiesReader("minoc.properties");
            Class cls = null;
            String sqlGenKey = SQLGenerator.class.getName();
            try {
                String sqlGenClass = pr.getFirstPropertyValue(sqlGenKey);
                cls = Class.forName(sqlGenClass);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("could not load SQLGenerator class: " + e.getMessage(), e);
            }
            config.put(sqlGenKey, cls);
            config.put("jndiDataSourceName", JNDI_DATASOURCE);
            config.put("database", DATABASE);
            config.put("schema", SCHEMA);
            newDAO.setConfig(config);
            dao = newDAO;
        }
        return dao;
    }

}

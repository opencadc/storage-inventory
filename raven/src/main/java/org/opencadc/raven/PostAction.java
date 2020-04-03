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

import ca.nrc.cadc.ac.Group;
import ca.nrc.cadc.ac.Role;
import ca.nrc.cadc.ac.client.GMSClient;
import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferReader;
import ca.nrc.cadc.vos.TransferWriter;
import ca.nrc.cadc.vos.VOS;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessControlException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedEventDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.permissions.PermissionsClient;
import org.opencadc.inventory.permissions.ReadGrant;
import org.opencadc.inventory.permissions.TokenUtil;

/**
 * Given a transfer request object return a transfer response object with all
 * available endpoints to the target artifact.
 *
 * @author majorb
 */
public class PostAction extends RestAction {
    
    private static final Logger log = Logger.getLogger(PostAction.class);
    
    public static final String JNDI_DATASOURCE = "jdbc/inventory";

    // config keys
    static final String SQL_GEN_KEY = SQLGenerator.class.getName();
    static final String SCHEMA_KEY = SQLGenerator.class.getPackage().getName() + ".schema";
    static final String READ_GRANTS_KEY = ReadGrant.class.getName() +  ".serviceID";

    // immutable state set in constructor
    protected final ArtifactDAO artifactDAO;
    protected final List<URI> readGrantServices = new ArrayList<>();
    
    private static final String INLINE_CONTENT_TAG = "inputstream";
    private static final String CONTENT_TYPE = "text/xml";

    /**
     * Default, no-arg constructor.
     */
    public PostAction() {
        super();
        StringBuilder sb = new StringBuilder();
        try {
            MultiValuedProperties props = readConfig();
            sb.append("incomplete config: ");
            boolean ok = true;

            String sqlgen = getSingleProperty(props, SQL_GEN_KEY);
            sb.append("\n\t").append(SQL_GEN_KEY).append(": ");
            if (sqlgen == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }

            String schema = getSingleProperty(props, SCHEMA_KEY);
            sb.append("\n\t").append(SCHEMA_KEY).append(": ");
            if (schema == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }

            if (!ok) {
                throw new IllegalStateException(sb.toString());
            }

            List<String> readGrants = props.getProperty(READ_GRANTS_KEY);
            if (readGrants != null) {
                for (String s : readGrants) {
                    URI u = new URI(s);
                    readGrantServices.add(u);
                }
            }

            Map<String, Object> config = new HashMap<String, Object>();
            config.put(SQL_GEN_KEY, Class.forName(sqlgen));
            config.put("jndiDataSourceName", JNDI_DATASOURCE);
            config.put("schema", schema);
            config.put("database", null);
            artifactDAO = new ArtifactDAO();
            artifactDAO.setConfig(config);

        } catch (URISyntaxException | ClassNotFoundException ex) {
            throw new IllegalStateException("invalid config: " + sb.toString(), ex);
        }
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

    /**
     * Perform transfer negotiation.
     */
    @Override
    public void doAction() throws Exception {
        
        TransferReader reader = new TransferReader();
        InputStream in = (InputStream) syncInput.getContent(INLINE_CONTENT_TAG);
        Transfer transfer = reader.read(in, null);
        
        log.debug("transfer request: " + transfer);
        if (!Direction.pullFromVoSpace.equals(transfer.getDirection())) {
            throw new IllegalArgumentException("direction not supported: " + transfer.getDirection());
        }
        
        // only support https over anonymous auth method for now
        Protocol supportedProtocol = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        if (!transfer.getProtocols().contains(supportedProtocol)) {
            throw new IllegalArgumentException("no supported protocols (require https with anon security method)");
        }
        transfer.getProtocols().clear();
        
        // ensure artifact uri is valid and exists
        URI artifactURI = transfer.getTarget();
        InventoryUtil.validateArtifactURI(PostAction.class, artifactURI);
        Artifact artifact = artifactDAO.get(artifactURI);
        if (artifact == null) {
            throw new ResourceNotFoundException(artifactURI.toString());
        }
        
        // check read permission
        checkReadPermission(artifactURI);
        
        // get the user for logging
        String user = AuthMethod.ANON.toString();
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (authMethod != null && !authMethod.equals(AuthMethod.ANON)) {
            Set<String> userids = AuthenticationUtil.getUseridsFromSubject();
            if (userids.size() > 0) {
                user = userids.iterator().next();
            }
        }
        
        // gather all copies of the artifact
        List<SiteLocation> locations = artifact.siteLocations;
        if (locations == null || locations.size() == 0) {
            throw new ResourceNotFoundException("no copies");
        }
        
        // create an auth token
        String authToken = TokenUtil.generateToken(artifactURI, ReadGrant.class, user);
        
        RegistryClient regClient = new RegistryClient();
        StorageSiteDAO storageSiteDAO = new StorageSiteDAO(artifactDAO);
        StorageSite storageSite = null;
        URI resourceID = null;
        URL baseURL = null;
        String endpointURL = null;
        
        // TODO: Cache the full list of storage sites (using storageSiteDAO.list())
        // and check for updates to that list periodically (every 5 min or so)
        
        // produce URLs to each of the copies
        for (SiteLocation site : locations) {
            storageSite = storageSiteDAO.get(site.getSiteID());
            resourceID = storageSite.getResourceID();
            baseURL = regClient.getServiceURL(resourceID, Standards.SI_FILES, AuthMethod.ANON);
            log.debug("base url for site " + site.toString() + ": " + baseURL);
            if (baseURL != null) {
                endpointURL = baseURL.toString() + "/" + authToken + "/" + artifactURI.toString();
                Protocol p = new Protocol(supportedProtocol.getUri());
                if (transfer.version == VOS.VOSPACE_21) {
                    p.setSecurityMethod(Standards.SECURITY_METHOD_ANON);
                }
                p.setEndpoint(endpointURL);
                transfer.getProtocols().add(p);
                log.debug("added endpoint url: " + endpointURL);
            }
        }
        
        TransferWriter transferWriter = new TransferWriter();
        transferWriter.write(transfer, syncOutput.getOutputStream());
    }
    
    private void checkReadPermission(URI artifactURI) throws AccessControlException, TransientException {

        List<GroupURI> userGroups;
        try {
            userGroups = getUsersGroups();
        } catch (PrivilegedActionException e) {
            throw new IllegalStateException("Error getting user groups", e);
        }

        // TODO: optimize with threads
        for (URI readService : readGrantServices) {
            PermissionsClient pc = new PermissionsClient(readService);
            ReadGrant grant = pc.getReadGrant(artifactURI);
            if (grant.isAnonymousAccess()) {
                log.debug("anonymous read access granted");
                return;
            }
            if (grant.getGroups().size() > 0) {
                for (GroupURI readGroupUri : grant.getGroups()) {
                    for (GroupURI userGroup : userGroups) {
                        if (userGroup == readGroupUri) {
                            return;
                        }
                    }
                }
            }
        }
        throw new AccessControlException("read permission denied");
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

    static final String getSingleProperty(MultiValuedProperties props, String key) {
        List<String> vals = props.getProperty(key);
        if (vals.isEmpty()) {
            return null;
        }
        return vals.get(0);
    }
    
    protected DeletedEventDAO getDeletedEventDAO(ArtifactDAO src) {
        return new DeletedEventDAO(src);
    }

    List<GroupURI> getUsersGroups() throws PrivilegedActionException {
        PrivilegedExceptionAction<List<GroupURI>> action = () -> {
            LocalAuthority localAuthority = new LocalAuthority();
            URI groupsURI = localAuthority.getServiceURI(Standards.GMS_SEARCH_01.toString());
            GMSClient client = new GMSClient(groupsURI);
            return client.getMemberships();
        };

        Subject subject = AuthenticationUtil.getCurrentSubject();
        return Subject.doAs(subject, action);
    }

}

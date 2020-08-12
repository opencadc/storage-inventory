/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020.                            (c) 2020.
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupClient;
import org.opencadc.gms.GroupURI;
import org.opencadc.gms.GroupUtil;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedEventDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.TokenUtil;
import org.opencadc.permissions.WriteGrant;
import org.opencadc.permissions.client.PermissionsClient;

/**
 * Given a transfer request object return a transfer response object with all
 * available endpoints to the target artifact.
 *
 * @author majorb
 */
public class PostAction extends RestAction {
    
    private static final Logger log = Logger.getLogger(PostAction.class);

    static final String JNDI_DATASOURCE = "jdbc/inventory"; // context.xml

    private static final String KEY_BASE = PostAction.class.getPackage().getName();
    static final String SCHEMA_KEY =  KEY_BASE + ".db.schema";
    static final String READ_GRANTS_KEY = KEY_BASE + ".readGrantProvider";
    static final String WRITE_GRANTS_KEY = KEY_BASE + ".writeGrantProvider";
    static final String DEV_AUTH_ONLY_KEY = KEY_BASE + ".authenticateOnly";
    
    // immutable state set in constructor
    protected final ArtifactDAO artifactDAO;
    private final List<URI> readGrantServices = new ArrayList<>();
    private final List<URI> writeGrantServices = new ArrayList<>();
    private final boolean authenticateOnly;
    
    private static final String INLINE_CONTENT_TAG = "inputstream";
    private static final String CONTENT_TYPE = "text/xml";

    /**
     * Default, no-arg constructor.
     */
    public PostAction() {
        super();
        MultiValuedProperties props = getConfig();

        List<String> readGrants = props.getProperty(READ_GRANTS_KEY);
        if (readGrants != null) {
            for (String s : readGrants) {
                try {
                    URI u = new URI(s);
                    readGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + READ_GRANTS_KEY + "=" + s + " must be a valid URI");
                }
            }
        }
        
        List<String> writeGrants = props.getProperty(WRITE_GRANTS_KEY);
        if (writeGrants != null) {
            for (String s : writeGrants) {
                try {
                    URI u = new URI(s);
                    writeGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + WRITE_GRANTS_KEY + "=" + s + " must be a valid URI");
                }
            }
        }
        
        String ao = props.getFirstPropertyValue(DEV_AUTH_ONLY_KEY);
        if (ao != null) {
            try {
                this.authenticateOnly = Boolean.valueOf(ao);
                log.warn("(configuration) authenticateOnly = " + authenticateOnly);
            } catch (Exception ex) {
                throw new IllegalStateException("invalid config: " + DEV_AUTH_ONLY_KEY + "=" + ao + " must be true|false or not set");
            }
        } else {
            authenticateOnly = false;
        }

        Map<String, Object> config = getDaoConfig(props);
        this.artifactDAO = new ArtifactDAO();
        artifactDAO.setConfig(config); // connectivity tested
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
        Direction direction = transfer.getDirection();
        if (!Direction.pullFromVoSpace.equals(direction) && !Direction.pushToVoSpace.equals(direction)) {
            throw new IllegalArgumentException("direction not supported: " + transfer.getDirection());
        }
        
        // only support https over anonymous auth method for now
        // TODO: if the protocol includes an auth method that the storage site supports, generate a non-token URL
        Protocol supportedProtocol;
        if (direction.equals(Direction.pullFromVoSpace)) {
            supportedProtocol = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        } else {
            supportedProtocol = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
        }
        if (!transfer.getProtocols().contains(supportedProtocol)) {
            throw new UnsupportedOperationException("no supported protocols (require https with anon security method)");
        }
        transfer.getProtocols().clear();
        
        // ensure artifact uri is valid and exists
        URI artifactURI = transfer.getTarget();
        InventoryUtil.validateArtifactURI(PostAction.class, artifactURI);
        Artifact artifact = artifactDAO.get(artifactURI);
        if (artifact == null) {
            throw new ResourceNotFoundException(artifactURI.toString());
        }

        // TODO: checkReadPermission and checkWritePermission are identical in minoc so move to a
        //       service utility library rather than implement here
        if (direction.equals(Direction.pullFromVoSpace)) {
            checkReadPermission(artifactURI);
        } else {
            checkWritePermissions(artifactURI);
        }
        
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
        if (artifact.siteLocations.isEmpty()) {
            throw new ResourceNotFoundException("no sitelocation's found");
        }
        
        // create an auth token
        String authToken;
        if (direction.equals(Direction.pullFromVoSpace)) {
            authToken = TokenUtil.generateToken(artifactURI, ReadGrant.class, user);
        } else {
            authToken = TokenUtil.generateToken(artifactURI, WriteGrant.class, user);
        }
        
        RegistryClient regClient = new RegistryClient();
        StorageSiteDAO storageSiteDAO = new StorageSiteDAO(artifactDAO);
        StorageSite storageSite = null;
        URI resourceID = null;
        URL baseURL = null;
        String endpointURL = null;
        
        // TODO: Cache the full list of storage sites (using storageSiteDAO.list())
        // and check for updates to that list periodically (every 5 min or so)
        
        // produce URLs to each of the copies
        for (SiteLocation site : artifact.siteLocations) {
            storageSite = storageSiteDAO.get(site.getSiteID());
            if (direction.equals(Direction.pushToVoSpace) && !storageSite.getAllowWrite()) {
                continue;
            }
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

        if (authenticateOnly) {
            log.warn(DEV_AUTH_ONLY_KEY + "=true: allowing unrestricted access");
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
        // unfortunately, the speed of GroupClient.getGroups() will also depend on how many groups the
        // caller belongs to...
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

        throw new AccessControlException("read permission denied");
    }

    private void checkWritePermissions(URI artifactURI) throws AccessControlException, TransientException {
        if (authenticateOnly) {
            log.warn(DEV_AUTH_ONLY_KEY + "=true: allowing unrestricted access");
            return;
        }

        // TODO: could call multiple services in parallel
        Set<GroupURI> granted = new TreeSet<>();
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
        // rather than getting all groups... experiment to determine threshold?
        // unfortunately, the speed of GroupClient.getGroups() will also depend on how many groups the
        // caller belongs to...
        LocalAuthority loc = new LocalAuthority();
        URI resourceID = loc.getServiceURI(Standards.GMS_SEARCH_01.toString());
        GroupClient client = GroupUtil.getGroupClient(resourceID);
        List<GroupURI> userGroups = client.getMemberships();
        for (GroupURI gg : granted) {
            for (GroupURI userGroup : userGroups) {
                if (gg.equals(userGroup)) {
                    logInfo.setMessage("write grant: " + gg);
                    return;
                }
            }
        }

        throw new AccessControlException("read permission denied");
    }

    protected DeletedEventDAO getDeletedEventDAO(ArtifactDAO src) {
        return new DeletedEventDAO(src);
    }

    /**
     * Read config file and verify that all required entries are present.
     *
     * @return MultiValuedProperties containing the application config
     */
    static MultiValuedProperties getConfig() {
        PropertiesReader r = new PropertiesReader("raven.properties");
        MultiValuedProperties mvp = r.getAllProperties();

        StringBuilder sb = new StringBuilder();
        sb.append("incomplete config: ");
        boolean ok = true;

        String schema = mvp.getFirstPropertyValue(SCHEMA_KEY);
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

        return mvp;
    }

    static Map<String,Object> getDaoConfig(MultiValuedProperties props) {
        String cname = props.getFirstPropertyValue(SQLGenerator.class.getName());
        try {
            Map<String,Object> ret = new TreeMap<>();
            Class clz = Class.forName(cname);
            ret.put(SQLGenerator.class.getName(), clz);
            ret.put("jndiDataSourceName", JNDI_DATASOURCE);
            ret.put("schema", props.getFirstPropertyValue(SCHEMA_KEY));
            //config.put("database", null);
            return ret;
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("invalid config: failed to load SQLGenerator: " + cname);
        }
    }

}

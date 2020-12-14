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
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.server.PermissionsCheck;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.TokenTool;
import org.opencadc.permissions.WriteGrant;

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
    static final String SCHEMA_KEY =  KEY_BASE + ".inventory.schema";
    static final String PUBKEY_KEY = KEY_BASE + ".publicKeyFile";
    static final String PRIVATEKEY_KEY = KEY_BASE + ".privateKeyFile";
    static final String READ_GRANTS_KEY = KEY_BASE + ".readGrantProvider";
    static final String WRITE_GRANTS_KEY = KEY_BASE + ".writeGrantProvider";
    static final String DEV_AUTH_ONLY_KEY = KEY_BASE + ".authenticateOnly";
    
    // immutable state set in constructor
    protected final ArtifactDAO artifactDAO;
    private final File publicKeyFile;
    private final File privateKeyFile;
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
        
        // technically, raven only needs the private key to generate pre-auth tokens
        // but both are requied here for clarity
        // - in principle, raven could export it's public key and minoc(s) could retrieve it
        // - for now, minoc(s) need to be configured with the public key to validate pre-auth
        
        String publicKey = props.getFirstPropertyValue(PUBKEY_KEY);
        String privateKey = props.getFirstPropertyValue(PRIVATEKEY_KEY);
        // verify that these files exist in $HOME/config -- would prefer not to care about that location here
        this.publicKeyFile = new File(System.getProperty("user.home") + "/config/" + publicKey);
        this.privateKeyFile = new File(System.getProperty("user.home") + "/config/" + privateKey);
        if (!publicKeyFile.exists() || !privateKeyFile.exists()) {
            throw new IllegalStateException("invalid config: missing public/private key pair files -- " + publicKeyFile + " | " + privateKey);
        }
        
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
        final Transfer transfer = reader.read(in, null);
        
        log.debug("transfer request: " + transfer);
        Direction direction = transfer.getDirection();
        if (!Direction.pullFromVoSpace.equals(direction) && !Direction.pushToVoSpace.equals(direction)) {
            throw new IllegalArgumentException("direction not supported: " + transfer.getDirection());
        }
        
        URI artifactURI = transfer.getTarget();
        InventoryUtil.validateArtifactURI(PostAction.class, artifactURI);
        
        PermissionsCheck permissionsCheck = new PermissionsCheck(artifactURI, this.authenticateOnly, this.logInfo);
        if (direction.equals(Direction.pullFromVoSpace)) {
            permissionsCheck.checkReadPermission(this.readGrantServices);
        } else {
            permissionsCheck.checkWritePermission(this.writeGrantServices);
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
        
        String authToken = null;
        // create an auth token
        TokenTool tk = new TokenTool(publicKeyFile, privateKeyFile);
        if (direction.equals(Direction.pullFromVoSpace)) {
            authToken = tk.generateToken(artifactURI, ReadGrant.class, user);
        } else {
            authToken = tk.generateToken(artifactURI, WriteGrant.class, user);
        }
        
        List<Protocol> protos = null;
        if (Direction.pullFromVoSpace.equals(direction)) {
            protos = doPullFrom(artifactURI, transfer, authToken);
        } else {
            protos = doPushTo(artifactURI, transfer, authToken);
        }
        
        // TODO: sort protocols as caller will try them in order until success
        // - depends on client and site proximity
        // - sort pre-auth before non-pre-auth because we already did the permission check
        
        Transfer ret = new Transfer(artifactURI, direction, protos);
        ret.version = VOS.VOSPACE_21;
                        
        TransferWriter transferWriter = new TransferWriter();
        transferWriter.write(ret, syncOutput.getOutputStream());
    }
    
    private List<Protocol> doPullFrom(URI artifactURI, Transfer transfer, String authToken) throws ResourceNotFoundException, IOException {
        RegistryClient regClient = new RegistryClient();
        StorageSiteDAO storageSiteDAO = new StorageSiteDAO(artifactDAO);
        Set<StorageSite> sites = storageSiteDAO.list(); // this set could be cached
        
        List<Protocol> protos = new ArrayList<>();
        Artifact artifact = artifactDAO.get(artifactURI);
        if (artifact == null) {
            throw new ResourceNotFoundException(artifactURI.toString());
        }

        // TODO: this can currently happen but maybe should not: 
        // --- when the last siteLocation is removed, the artifact should be deleted?
        if (artifact.siteLocations.isEmpty()) {
            throw new ResourceNotFoundException("TBD: no copies available");
        }

        // produce URLs to each of the copies for each of the protocols
        for (SiteLocation site : artifact.siteLocations) {
            StorageSite storageSite = getSite(sites, site.getSiteID());
            Capability filesCap = null;
            try {
                Capabilities caps = regClient.getCapabilities(storageSite.getResourceID());
                filesCap = caps.findCapability(Standards.SI_FILES);
                if (filesCap == null) {
                    log.warn("service: " + storageSite.getResourceID() + " does not provide " + Standards.SI_FILES);
                }
            } catch (ResourceNotFoundException ex) {
                log.warn("failed to find service: " + storageSite.getResourceID());
            }
            if (filesCap != null) {
                for (Protocol proto : transfer.getProtocols()) {
                    if (storageSite.getAllowRead()) {
                        URI sec = proto.getSecurityMethod();
                        if (sec == null) {
                            sec = Standards.SECURITY_METHOD_ANON;
                        }
                        Interface iface = filesCap.findInterface(sec);
                        if (iface != null) {
                            URL baseURL = iface.getAccessURL().getURL();
                            log.debug("base url for site " + storageSite.getResourceID() + ": " + baseURL);
                            if (protocolCompat(proto, baseURL)) {
                                StringBuilder sb = new StringBuilder();
                                sb.append(baseURL.toExternalForm()).append("/");
                                if (proto.getSecurityMethod() == null || Standards.SECURITY_METHOD_ANON.equals(proto.getSecurityMethod())) {
                                    sb.append(authToken).append("/");
                                }
                                sb.append(artifactURI.toASCIIString());
                                Protocol p = new Protocol(proto.getUri());
                                if (transfer.version == VOS.VOSPACE_21) {
                                    p.setSecurityMethod(proto.getSecurityMethod());
                                }
                                p.setEndpoint(sb.toString());
                                protos.add(p);
                                log.debug("added: " + p);
                            } else {
                                log.debug("reject protocol: " + proto 
                                        + " reason: no compatible URL protocol");
                            }
                        } else {
                            log.debug("reject protocol: " + proto 
                                    + " reason: unsupported security method: " + proto.getSecurityMethod());
                        }
                    }
                }
            }
        }
        
        return protos;
    }
    
    private List<Protocol> doPushTo(URI artifactURI, Transfer transfer, String authToken) throws IOException {
        RegistryClient regClient = new RegistryClient();
        StorageSiteDAO storageSiteDAO = new StorageSiteDAO(artifactDAO);
        Set<StorageSite> sites = storageSiteDAO.list(); // this set could be cached
        
        List<Protocol> protos = new ArrayList<>();
        // produce URLs for all writable sites
        for (StorageSite storageSite : sites) {
            //log.warn("PUT: " + storageSite);
            Capability filesCap = null;
            try {
                Capabilities caps = regClient.getCapabilities(storageSite.getResourceID());
                filesCap = caps.findCapability(Standards.SI_FILES);
                if (filesCap == null) {
                    log.warn("service: " + storageSite.getResourceID() + " does not provide " + Standards.SI_FILES);
                }
            } catch (ResourceNotFoundException ex) {
                log.warn("failed to find service: " + storageSite.getResourceID());
            }
            if (filesCap != null) {
                for (Protocol proto : transfer.getProtocols()) {
                    //log.warn("PUT: " + storageSite + " proto: " + proto);
                    if (storageSite.getAllowWrite()) {
                        URI sec = proto.getSecurityMethod();
                        if (sec == null) {
                            sec = Standards.SECURITY_METHOD_ANON;
                        }
                        Interface iface = filesCap.findInterface(sec);
                        log.debug("PUT: " + storageSite + " proto: " + proto + " iface: " + iface);
                        if (iface != null) {
                            URL baseURL = iface.getAccessURL().getURL();
                            //log.debug("base url for site " + storageSite.getResourceID() + ": " + baseURL);
                            if (protocolCompat(proto, baseURL)) {
                                StringBuilder sb = new StringBuilder();
                                sb.append(baseURL.toExternalForm()).append("/");
                                if (proto.getSecurityMethod() == null || Standards.SECURITY_METHOD_ANON.equals(proto.getSecurityMethod())) {
                                    sb.append(authToken).append("/");
                                }
                                sb.append(artifactURI.toASCIIString());
                                Protocol p = new Protocol(proto.getUri());
                                if (transfer.version == VOS.VOSPACE_21) {
                                    p.setSecurityMethod(proto.getSecurityMethod());
                                }
                                p.setEndpoint(sb.toString());
                                protos.add(p);
                                log.debug("added: " + p);
                            } else {
                                log.debug("PUT: " + storageSite + "PUT: reject protocol: " + proto 
                                        + " reason: no compatible URL protocol");
                            }
                        } else {
                            log.debug("PUT: " + storageSite + "PUT: reject protocol: " + proto 
                                    + " reason: unsupported security method: " + proto.getSecurityMethod());
                        }
                    }
                }
            }
        }
        return protos;
    }
    
    private StorageSite getSite(Set<StorageSite> sites, UUID id) {
        for (StorageSite s : sites) {
            if (s.getID().equals(id)) {
                return s;
            }
        }
        throw new IllegalStateException("BUG: could not find StorageSite with id=" +  id);
    }
    
    private boolean protocolCompat(Protocol p, URL u) {
        if ("https".equals(u.getProtocol())) {
            return VOS.PROTOCOL_HTTPS_GET.equals(p.getUri()) || VOS.PROTOCOL_HTTPS_PUT.equals(p.getUri());
        }
        if ("http".equals(u.getProtocol())) {
            return VOS.PROTOCOL_HTTP_GET.equals(p.getUri()) || VOS.PROTOCOL_HTTP_PUT.equals(p.getUri());
        }
        return false;
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

        // validate required config here
        String schema = mvp.getFirstPropertyValue(SCHEMA_KEY);
        sb.append("\n\t").append(SCHEMA_KEY).append(": ");
        if (schema == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }
        
        String pub = mvp.getFirstPropertyValue(PUBKEY_KEY);
        sb.append("\n\t").append(PUBKEY_KEY).append(": ");
        if (pub == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }
        
        String priv = mvp.getFirstPropertyValue(PRIVATEKEY_KEY);
        sb.append("\n\t").append(PUBKEY_KEY).append(": ");
        if (priv == null) {
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

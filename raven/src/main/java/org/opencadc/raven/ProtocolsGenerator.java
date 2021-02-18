/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2021.                            (c) 2021.
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

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.TokenTool;
import org.opencadc.permissions.WriteGrant;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Class for generating protocol lists corresponding to transfer requests.
 *
 * @author adriand
 */
public class ProtocolsGenerator{

    private static final Logger log = Logger.getLogger(ProtocolsGenerator.class);

    private ArtifactDAO artifactDAO;
    private String user;
    private final File publicKeyFile;
    private final File privateKeyFile;
    /**
     * Ctor
     */
    public ProtocolsGenerator(ArtifactDAO artifactDAO, File publicKeyFile, File privateKeyFile, String user) {
        this.artifactDAO = artifactDAO;
        this.user = user;
        this.publicKeyFile = publicKeyFile;
        this.privateKeyFile = privateKeyFile;
    }

    List<Protocol> getProtocols(Transfer transfer) throws ResourceNotFoundException, IOException {
        String authToken = null;
        // create an auth token
        URI artifactURI = transfer.getTarget();
        TokenTool tk = new TokenTool(publicKeyFile, privateKeyFile);
        if (transfer.getDirection().equals(Direction.pullFromVoSpace)) {
            authToken = tk.generateToken(artifactURI, ReadGrant.class, user);
        } else {
            authToken = tk.generateToken(artifactURI, WriteGrant.class, user);
        }

        List<Protocol> protos = null;
        if (Direction.pullFromVoSpace.equals(transfer.getDirection())) {
            protos = doPullFrom(artifactURI, transfer, authToken);
        } else {
            protos = doPushTo(artifactURI, transfer, authToken);
        }
        return protos;
    }

    List<Protocol> doPullFrom(URI artifactURI, Transfer transfer, String authToken) throws ResourceNotFoundException, IOException {
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

}

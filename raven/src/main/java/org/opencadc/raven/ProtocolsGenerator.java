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

import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.net.HttpGet;
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
import ca.nrc.cadc.vosi.Availability;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.TokenTool;
import org.opencadc.permissions.WriteGrant;

/**
 * Class for generating protocol lists corresponding to transfer requests.
 *
 * @author adriand
 */
public class ProtocolsGenerator {

    private static final Logger log = Logger.getLogger(ProtocolsGenerator.class);

    public static final String ARTIFACT_ID_HDR = "x-artifact-id";  // matches minoc.HeadAction.ARTIFACT_ID_HDR

    private final ArtifactDAO artifactDAO;
    private final DeletedArtifactEventDAO deletedArtifactEventDAO;
    private final String user;
    private final File publicKeyFile;
    private final File privateKeyFile;
    private final Map<URI, Availability> siteAvailabilities;
    private final Map<URI, StorageSiteRule> siteRules;
    private final boolean preventNotFound;


    public ProtocolsGenerator(ArtifactDAO artifactDAO, File publicKeyFile, File privateKeyFile, String user,
                              Map<URI, Availability> siteAvailabilities, Map<URI, StorageSiteRule> siteRules,
                              boolean preventNotFound) {
        this.artifactDAO = artifactDAO;
        this.deletedArtifactEventDAO = new DeletedArtifactEventDAO(this.artifactDAO);
        this.user = user;
        this.publicKeyFile = publicKeyFile;
        this.privateKeyFile = privateKeyFile;
        this.siteAvailabilities = siteAvailabilities;
        this.siteRules = siteRules;
        this.preventNotFound = preventNotFound;
    }

    List<Protocol> getProtocols(Transfer transfer) throws ResourceNotFoundException, IOException {
        String authToken = null;
        // create an auth token
        URI artifactURI = transfer.getTargets().get(0); // see PostAction line ~127
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

    static void prioritizePullFromSites(List<StorageSite> storageSites) {
        // contains the algorithm for prioritizing storage sites to pull from. Currently
        // read/write sites have higher priority
        storageSites.sort((site1, site2) -> Boolean.compare(!site1.getAllowWrite(), !site2.getAllowWrite()));
    }

    Artifact getRemoteArtifact(URL location, URI artifactURI) {
        try {
            HttpGet head = new HttpGet(location, true);
            head.setHeadOnly(true);
            head.setReadTimeout(10000);
            head.run();
            if (head.getResponseCode() != 200) {
                // caught at the end of the method
                throw new RuntimeException("Unsuccessful HEAD request: " + head.getResponseCode());
            }
            UUID id = UUID.fromString(head.getResponseHeader(ARTIFACT_ID_HDR));
            Artifact result = new
                    Artifact(id, artifactURI, head.getDigest(), head.getLastModified(), head.getContentLength());
            result.contentType = head.getContentType();
            result.contentEncoding = head.getContentEncoding();
            return result;
        } catch (Throwable t) {
            log.debug("Could not retrieve artifact " + artifactURI.toASCIIString() + " from " + location, t);
            return null;
        }
    }

    private Capability getFilesCapability(StorageSite storageSite) {
        if (!isAvailable(storageSite.getResourceID())) {
            log.warn("storage site is offline: " + storageSite.getResourceID());
            return null;
        }
        Capability filesCap = null;
        try {
            RegistryClient regClient = new RegistryClient();
            Capabilities caps = regClient.getCapabilities(storageSite.getResourceID());
            filesCap = caps.findCapability(Standards.SI_FILES);
            if (filesCap == null) {
                log.warn("service: " + storageSite.getResourceID() + " does not provide " + Standards.SI_FILES);
            }
        } catch (ResourceNotFoundException ex) {
            log.warn("storage site not found: " + storageSite.getResourceID());
        } catch (Exception ex) {
            log.warn("storage site not responding (capabilities): " + storageSite.getResourceID(), ex);
        }
        return filesCap;
    }

    Artifact getUnsyncedArtifact(URI artifactURI, Transfer transfer, Set<StorageSite> storageSites, String authToken) {
        Artifact result = null;
        for (StorageSite storageSite : storageSites) {
            // check if site is currently offline
            Capability filesCap = getFilesCapability(storageSite);
            if (filesCap == null) {
                log.warn("Capabilities not found for storage site " + storageSite.getResourceID());
                continue;
            }
            Iterator<Protocol> pi = transfer.getProtocols().iterator();
            while (result == null && pi.hasNext()) {
                Protocol proto = pi.next();

                if (storageSite.getAllowRead()) {
                    URI sec = proto.getSecurityMethod();
                    if (sec == null) {
                        sec = Standards.SECURITY_METHOD_ANON;
                    } else if (Standards.SECURITY_METHOD_CERT.equals(sec)) {
                        try {
                            if (!CredUtil.checkCredentials()) {
                                // skip this protocol
                                continue;
                            }
                        } catch (Exception e) {
                            log.debug("Failed to check user credentials", e);
                            continue;
                        }
                    }
                    Interface iface = filesCap.findInterface(sec);
                    if (iface != null) {
                        URL baseURL = iface.getAccessURL().getURL();
                        log.debug("base url for site " + storageSite.getResourceID() + ": " + baseURL);
                        StringBuilder sb = new StringBuilder();
                        sb.append(baseURL.toExternalForm()).append("/");
                        List<URL> urls = new ArrayList<URL>();
                        try {
                            if (authToken != null && Standards.SECURITY_METHOD_ANON.equals(sec)) {
                                // add the pre-auth url
                                URL pa = new URL(sb.toString() + "/" + authToken + "/" + artifactURI.toASCIIString());
                                urls.add(pa);
                            }
                            urls.add(new URL(sb.append(artifactURI.toASCIIString()).toString()));
                        } catch (MalformedURLException ex) {
                            throw new RuntimeException("BUG: Malformed URL to the site", ex);
                        }
                        Iterator<URL> ui = urls.iterator();
                        while (result == null && ui.hasNext()) {
                            Artifact remoteArtifact = getRemoteArtifact(ui.next(), artifactURI);
                            if (remoteArtifact == null) {
                                continue;
                            }
                            if (deletedArtifactEventDAO.get(remoteArtifact.getID()) != null) {
                                // the artifact was already deleted from global
                                log.debug("Artifact " + artifactURI + " already deleted from global");
                                continue;
                            }
                            remoteArtifact.siteLocations.add(new SiteLocation(storageSite.getID()));
                            if (result != null && result.getID().equals(remoteArtifact.getID())) {
                                result.siteLocations.addAll(remoteArtifact.siteLocations);
                            } else if (result == null || InventoryUtil.isRemoteWinner(result, remoteArtifact)) {
                                log.debug("Artifact " + artifactURI.toASCIIString() + " use copy from "
                                        + storageSite.getResourceID());
                                result = remoteArtifact;
                            } // else: just retain current result
                        }
                    }
                }
            }

        }
        return result;
    }

    List<Protocol> doPullFrom(URI artifactURI, Transfer transfer, String authToken) throws ResourceNotFoundException, IOException {
        StorageSiteDAO storageSiteDAO = new StorageSiteDAO(artifactDAO);
        Set<StorageSite> sites = storageSiteDAO.list(); // this set could be cached

        List<Protocol> protos = new ArrayList<>();
        Artifact artifact = artifactDAO.get(artifactURI);
        // produce URLs to each of the copies for each of the protocols
        List<StorageSite> storageSites = new ArrayList<>();
        if (artifact == null) {
            if (this.preventNotFound) {
                log.debug("Artifact " + artifactURI.toASCIIString() + " not found in global. Check sites.");
                artifact = getUnsyncedArtifact(artifactURI, transfer, sites, authToken);
            }
        }

        if (artifact == null) {
            throw new ResourceNotFoundException("not found: " + artifactURI.toString());
        }

        // TODO: this can currently happen but maybe should not:
        // --- when the last siteLocation is removed, the artifact should be deleted (fenwick, ratik)
        if (artifact.siteLocations.isEmpty()) {
            throw new ResourceNotFoundException("not found: " + artifactURI.toString());
        }

        for (SiteLocation site : artifact.siteLocations) {
            StorageSite storageSite = getSite(sites, site.getSiteID());
            storageSites.add(storageSite);
        }

        prioritizePullFromSites(storageSites);
        for (StorageSite storageSite : storageSites) {
            Capability filesCap = getFilesCapability(storageSite);
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
                                if (authToken != null && Standards.SECURITY_METHOD_ANON.equals(sec)) {
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

                                // add a plain anon URL
                                if (authToken != null && Standards.SECURITY_METHOD_ANON.equals(sec)) {
                                    sb = new StringBuilder();
                                    sb.append(baseURL.toExternalForm()).append("/");
                                    sb.append(artifactURI.toASCIIString());
                                    p = new Protocol(proto.getUri());
                                    p.setEndpoint(sb.toString());
                                    protos.add(p);
                                    log.debug("added: " + p);
                                }
                            } else {
                                log.debug("reject protocol: " + proto
                                        + " reason: no compatible URL protocol");
                            }
                        } else {
                            log.debug("reject protocol: " + proto
                                    + " reason: unsupported security method: " + proto.getSecurityMethod());
                        }
                    } else {
                        log.debug("Storage not allowed read " + storageSite.getName());
                    }
                }
            }
        }
        return protos;
    }

    static SortedSet<StorageSite> prioritizePushToSites(Set<StorageSite> storageSites, URI artifactURI,
                                                        Map<URI, StorageSiteRule> siteRules) {
        PrioritizingStorageSiteComparator comparator = new PrioritizingStorageSiteComparator(siteRules, artifactURI, null);
        TreeSet<StorageSite> orderedSet = new TreeSet<>(comparator);
        for (StorageSite storageSite : storageSites) {
            if (storageSite.getAllowWrite()) {
                orderedSet.add(storageSite);
            }
        }
        return orderedSet;
    }

    private List<Protocol> doPushTo(URI artifactURI, Transfer transfer, String authToken) throws IOException {
        RegistryClient regClient = new RegistryClient();
        StorageSiteDAO storageSiteDAO = new StorageSiteDAO(artifactDAO);
        Set<StorageSite> storageSites = storageSiteDAO.list(); // this set could be cached

        List<Protocol> protos = new ArrayList<>();
        SortedSet<StorageSite> orderedSites = prioritizePushToSites(storageSites, artifactURI, this.siteRules);
        // produce URLs for all writable sites
        for (StorageSite storageSite : orderedSites) {
            // check if site is currently offline
            if (!isAvailable(storageSite.getResourceID())) {
                log.warn("storage site is offline: " + storageSite.getResourceID());
                continue;
            }

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

                                // no plain anon URL for put
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

    private boolean isAvailable(URI resourceID) {
        Availability availability = siteAvailabilities.get(resourceID);
        log.debug("checking availablity: " + resourceID + " " + availability);
        if (availability != null && !availability.isAvailable()) {
            return false;
        }
        return true;
    }

    /**
     * Compare two StorageSite's. A site with a namespace matching the given Artifact URI
     * is ordered higher than a site without a matching namespace. If two StorageSite's
     * both have a matching namespace, or both do not have a matching namespace,
     * they are ordered using StorageSite default ordering. The StorageSite's are ordered
     * in descending order.
     */
    static class PrioritizingStorageSiteComparator implements Comparator<StorageSite> {

        private final Map<URI, StorageSiteRule> siteRules;
        private final URI artifactURI;
        private InetAddress clientIP;

        public PrioritizingStorageSiteComparator(Map<URI, StorageSiteRule> siteRules,
                                                 URI artifactURI, InetAddress clientIP) {
            this.siteRules = siteRules;
            this.artifactURI = artifactURI;
            this.clientIP = clientIP;
        }

        @Override
        public int compare(StorageSite site1, StorageSite site2) {

            // nothing to compare so considered equal.
            if (site1 == null && site2 == null) {
                return 0;
            }
            if (site1 == null) {
                return 1;
            }
            if (site2 == null) {
                return -1;
            }

            // get the rules for each site
            StorageSiteRule rule1 = this.siteRules.get(site1.getResourceID());
            StorageSiteRule rule2 = this.siteRules.get(site2.getResourceID());

            // check if a site has a namespace matching the ArtifactURI
            boolean site1Match = false;
            if (rule1 != null) {
                for (Namespace ns : rule1.getNamespaces()) {
                    if (ns.matches(this.artifactURI)) {
                        site1Match = true;
                        break;
                    }
                }
            }

            boolean site2match = false;
            if (rule2 != null) {
                for (Namespace ns : rule2.getNamespaces()) {
                    if (ns.matches(this.artifactURI)) {
                        site2match = true;
                        break;
                    }
                }
            }

            // give higher priority to the site with a namespace that matches the Artifact URI.
            if (site1Match && !site2match) {
                return -1;
            }
            if (!site1Match && site2match) {
                return 1;
            }
            // default: StorageSite order
            return site1.compareTo(site2);
        }
    }

}

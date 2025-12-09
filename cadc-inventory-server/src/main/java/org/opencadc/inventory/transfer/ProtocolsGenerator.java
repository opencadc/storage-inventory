/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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

package org.opencadc.inventory.transfer;

import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.StorageResolver;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.vosi.Availability;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.TokenTool;
import org.opencadc.permissions.WriteGrant;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;

/**
 * Class for generating protocol lists corresponding to transfer requests.
 *
 * @author adriand
 */
public class ProtocolsGenerator {

    private static final Logger log = Logger.getLogger(ProtocolsGenerator.class);

    public static final URI SECURITY_EMBEDDED_TOKEN = URI.create("https://www.opencadc.org/std/storage#embedded-token");
    
    public static final String ARTIFACT_ID_HDR = "x-artifact-id";  // matches minoc.HeadAction.ARTIFACT_ID_HDR

    private final ArtifactDAO artifactDAO;
    private final DeletedArtifactEventDAO deletedArtifactEventDAO;
    
    private final Map<URI, Availability> siteAvailabilities;
    private final Map<URI, StorageSiteRule> siteRules = new TreeMap<>();
    
    /**
     * Sites to avoid when generating GET URLs. 
     */
    public final List<URI> getAvoid = new ArrayList<>();
    
    /**
     * Sites to avoid when generating PUT URLs.
     */
    public final List<URI> putAvoid = new ArrayList<>();
    
    /**
     * Optional StorageResolver to resolve Artifact.uri to an external data provider.
     */
    public StorageResolver storageResolver;
    
    /**
     * Optional flag to enable prevention of 404 NotFound failure due to eventual
     * consistency. Setting this to true will cause the code to make HTTP HEAD
     * requests to all known storage sites looking for an artifact that is not
     * in the local database.
     */
    public boolean preventNotFound = false;
    
    /**
     * Optional user value to put into generated preauth token.
     */
    public String user;
    
    /**
     * Optional TokenTool to generate and inject preauth tokens into otherwise anon URL.
     */
    public TokenTool tokenGen;
    
    /**
     * Optional restriction so that all anon URLs must have a preauth token.
     */
    public boolean requirePreauthAnon = false;
    
    // for use by FilesAction subclasses to enhance logging
    boolean storageResolverAdded = false;
    
    // needed by vault files:
    public Artifact resolvedArtifact;

    public ProtocolsGenerator(ArtifactDAO artifactDAO, Map<URI, Availability> siteAvailabilities, Map<URI, StorageSiteRule> siteRules) {
        this.artifactDAO = artifactDAO;
        this.deletedArtifactEventDAO = new DeletedArtifactEventDAO(this.artifactDAO);
        this.siteAvailabilities = siteAvailabilities;
        if (siteRules != null) {
            this.siteRules.putAll(siteRules);
        }
    }

    // unit testing
    public ProtocolsGenerator(Map<URI, StorageSiteRule> siteRules) {
        this.artifactDAO = null;
        this.deletedArtifactEventDAO = null;
        this.siteAvailabilities = null;
        if (siteRules != null) {
            this.siteRules.putAll(siteRules);
        }
    }
    
    public boolean getStorageResolverAdded() {
        return storageResolverAdded;
    }

    public List<Protocol> getProtocols(Transfer transfer) throws ResourceNotFoundException, IOException {
        return getProtocols(transfer, null);
    }
    
    public List<Protocol> getProtocols(Transfer transfer, String filenameOverride) throws ResourceNotFoundException, IOException {
        String authToken = null;
        URI artifactURI = transfer.getTargets().get(0); // see PostAction line ~127
        if (tokenGen != null) {
            // create an auth token
            if (transfer.getDirection().equals(Direction.pullFromVoSpace)) {
                authToken = tokenGen.generateToken(artifactURI, ReadGrant.class, user);
            } else {
                authToken = tokenGen.generateToken(artifactURI, WriteGrant.class, user);
            }
        }

        List<Protocol> protos = null;
        if (Direction.pullFromVoSpace.equals(transfer.getDirection())) {
            // filename override only on GET
            protos = doPullFrom(artifactURI, transfer, authToken, filenameOverride);
        } else if (Direction.pushToVoSpace.equals(transfer.getDirection())) {
            protos = doPushTo(artifactURI, transfer, authToken);
        } else {
            throw new UnsupportedOperationException("unexpected transfer direction: " + transfer.getDirection().getValue());
                    
        }
        return protos;
    }
    
    public void filterReadable(Set<StorageSite> sites) {
        // filter sites for readable
        Iterator<StorageSite> iter = sites.iterator();
        while (iter.hasNext()) {
            StorageSite s = iter.next();
            boolean avail = isAvailable(s.getResourceID());
            // getAvoid is a soft-avoid so handled below
            if (s.getAllowRead() && avail) {
                log.debug("doPullFrom: " + s.getResourceID() + " OK");
            } else {
                log.debug("doPullFrom: "  + s.getResourceID() + " SKIP readable=" + s.getAllowRead() + " available=" + avail);
                iter.remove();
            }
        }
    }

    public Artifact getUnsyncedArtifact(URI artifactURI, Transfer transfer, Set<StorageSite> storageSites, String authToken) {
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

                if (storageSite.getAllowRead()) { // redundant: storageSites already filtered for readable
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
                            if (!requirePreauthAnon) {
                                urls.add(new URL(sb.append(artifactURI.toASCIIString()).toString()));
                            }
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
    
    Artifact getRemoteArtifact(URL location, URI artifactURI) {
        try {
            HttpGet head = new HttpGet(location, true);
            head.setHeadOnly(true);
            head.setConnectionTimeout(6000);
            head.setReadTimeout(9000);
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

    // contains the algorithm for prioritizing storage sites to get file
    List<StorageSite> prioritizePullFromSites(Set<StorageSite> storageSites, URI artifactURI) {
        // random
        List<StorageSite> ret = new ArrayList<>(storageSites.size());
        ret.addAll(storageSites);
        Collections.shuffle(ret);
        return ret;
    }
    
    List<Protocol> doPullFrom(URI artifactURI, Transfer transfer, String authToken, String filenameOverride) 
            throws ResourceNotFoundException, IOException {
        StorageSiteDAO storageSiteDAO = new StorageSiteDAO(artifactDAO);
        Set<StorageSite> sites = storageSiteDAO.list(); // this set could be cached
        final int knownSites = sites.size();
        
        filterReadable(sites);

        // find artifact
        Artifact artifact = artifactDAO.get(artifactURI);
        if (artifact == null) {
            if (this.preventNotFound) {
                log.debug("Artifact " + artifactURI.toASCIIString() + " not found in global. Check sites.");
                artifact = getUnsyncedArtifact(artifactURI, transfer, sites, authToken);
            }
        }
        log.debug(artifactURI + " found: " + artifact);
        this.resolvedArtifact = artifact;
        
        // filter sites for locations
        if (artifact != null) {
            if (artifact.storageLocation != null) {
                if (sites.size() > 1) {
                    throw new RuntimeException("BUG: found second StorageSite in database with assigned Artifact.storageLocation");
                }
            } else {
                // global - find intersection: artifact.siteLocations X sites
                Iterator<StorageSite> iter = sites.iterator();
                while (iter.hasNext()) {
                    StorageSite s = iter.next();
                    StorageSite ss = findSite(s, artifact.siteLocations);
                    if (ss == null) {
                        iter.remove();
                    }
                }
            }
        }
        
        List<StorageSite> orderedSites = prioritizePullFromSites(sites, artifactURI);
        log.debug("pullFrom: known=" + knownSites + " -> usable=" + orderedSites.size());
        
        List<Protocol> protos = new ArrayList<>();
        List<Protocol> avoidable = new ArrayList<>();
        
        for (StorageSite storageSite : orderedSites) {
            boolean avoid = getAvoid.contains(storageSite.getResourceID());
            log.debug("trying site: " + storageSite.getResourceID());
            Capability filesCap = getFilesCapability(storageSite);
            if (artifact != null && filesCap != null) {
                for (Protocol proto : transfer.getProtocols()) {
                    log.debug("\tprotocol: " + proto);
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
                            if (filenameOverride != null) {
                                sb.append(":fo/").append(filenameOverride);
                            }
                            Protocol p = new Protocol(proto.getUri());
                            if (transfer.version == VOS.VOSPACE_21) {
                                p.setSecurityMethod(proto.getSecurityMethod());
                            }
                            p.setEndpoint(sb.toString());
                            if (avoid) {
                                avoidable.add(p);
                            } else {
                                protos.add(p);
                            }
                            log.debug("added: " + p);

                            // add a plain anon URL
                            if (!requirePreauthAnon && Standards.SECURITY_METHOD_ANON.equals(sec)) {
                                sb = new StringBuilder();
                                sb.append(baseURL.toExternalForm()).append("/");
                                sb.append(artifactURI.toASCIIString());
                                p = new Protocol(proto.getUri());
                                if (filenameOverride != null) {
                                    sb.append(":fo/").append(filenameOverride);
                                }
                                p.setEndpoint(sb.toString());
                                if (avoid) {
                                    avoidable.add(p);
                                } else {
                                    protos.add(p);
                                }
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
                }
            }
        }
        
        if (storageResolver != null) {
            try {
                URL externalURL = storageResolver.toURL(artifactURI);
                if (externalURL != null) {
                    for (Protocol p : transfer.getProtocols()) {
                        Protocol proto = new Protocol(p.getUri());
                        if (transfer.version == VOS.VOSPACE_21) {
                            proto.setSecurityMethod(p.getSecurityMethod());
                        }
                        proto.setEndpoint(externalURL.toString());
                        protos.add(proto);
                        log.debug("added external " + proto);
                        storageResolverAdded = true;
                    }
                }
            } catch (IllegalArgumentException ex) {
                // storageResolver does not support that URI
            }
        }

        if (protos.isEmpty()) {
            protos.addAll(avoidable);
        }
        
        if (protos.isEmpty()) {
            // unable to generate any URLs - maybe "no copies available"?
            throw new ResourceNotFoundException("not found: " + artifactURI.toString());
        }
        
        return protos;
    }

    // the algorithm for prioritizing storage sites to put file:
    // use applicable site rule || random
    List<StorageSite> prioritizePushToSites(Set<StorageSite> storageSites, URI artifactURI) {
        List<StorageSite> ret = new ArrayList<>(storageSites.size());
        ret.addAll(storageSites);
        boolean siteRuleExists = findSiteRule(artifactURI);
        if (siteRules.isEmpty() || !siteRuleExists) {
            Collections.shuffle(ret);
        } else {
            PrioritizingStorageSiteComparator comparator = new PrioritizingStorageSiteComparator(siteRules, artifactURI, null);
            Collections.sort(ret, comparator);
        }
        return ret;
    }

    private List<Protocol> doPushTo(URI artifactURI, Transfer transfer, String authToken) throws IOException {
        StorageSiteDAO storageSiteDAO = new StorageSiteDAO(artifactDAO);
        Set<StorageSite> sites = storageSiteDAO.list(); // this set could be cached
        final int knownSites = sites.size();

        // filter sites for writable
        Iterator<StorageSite> iter = sites.iterator();
        while (iter.hasNext()) {
            StorageSite s = iter.next();
            boolean avail = isAvailable(s.getResourceID());
            boolean avoid = putAvoid.contains(s.getResourceID());
            if (s.getAllowWrite() && avail && !avoid) {
                log.debug("doPushTo: " + s.getResourceID() + " OK");
            } else {
                log.debug("doPushTo: "  + s.getResourceID() 
                        + " SKIP writable=" + s.getAllowWrite() + " available=" + avail + " avoid=" + avoid);
                iter.remove();
            }
        }
        
        List<StorageSite> orderedSites = prioritizePushToSites(sites, artifactURI);
        log.debug("pushTo: known sites " + knownSites + " -> writableSites " + orderedSites.size());
        
        List<Protocol> protos = new ArrayList<>();
        for (StorageSite storageSite : orderedSites) {
            log.debug("pushTo: trying site " + storageSite.getResourceID());
            Capability filesCap = getFilesCapability(storageSite);
            if (filesCap != null) {
                for (Protocol proto : transfer.getProtocols()) {
                    log.debug("pushTo: " + storageSite + " proto: " + proto);
                    URI sec = proto.getSecurityMethod();
                    if (sec == null) {
                        sec = Standards.SECURITY_METHOD_ANON;
                    }
                    boolean anon = Standards.SECURITY_METHOD_ANON.equals(sec);
                    Interface iface = filesCap.findInterface(sec);
                    log.debug("pushTo: " + storageSite + " proto: " + proto + " iface: " + iface);
                    if (iface != null) {
                        URL baseURL = iface.getAccessURL().getURL();
                        //log.debug("base url for site " + storageSite.getResourceID() + ": " + baseURL);
                        if (protocolCompat(proto, baseURL)) {
                            // // no plain anon URL for put: !anon or anon+token
                            boolean gen = (!anon || (anon && authToken != null));
                            if (gen) {
                                StringBuilder sb = new StringBuilder();
                                sb.append(baseURL.toExternalForm()).append("/");
                                if (authToken != null && anon) {
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
                            }

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
        return protos;
    }

    private StorageSite findSite(StorageSite site, Set<SiteLocation> locs) {
        for (SiteLocation loc : locs) {
            if (site.getID().equals(loc.getSiteID())) {
                return site;
            }
        }
        return null;
    }
    
    private boolean findSiteRule(URI artifactURI) {
        for (StorageSiteRule r : siteRules.values()) {
            for (Namespace ns : r.getNamespaces()) {
                if (ns.matches(artifactURI)) {
                    return true;
                }
            }
        }
        return false;
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

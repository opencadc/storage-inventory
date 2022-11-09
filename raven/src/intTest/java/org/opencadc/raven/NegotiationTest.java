/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2022.                            (c) 2022.
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
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.View;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.db.version.InitDatabase;

/**
 * Test transfer negotiation.
 * 
 * @author majorb
 */
public class NegotiationTest extends RavenTest {
    
    private static final Logger log = Logger.getLogger(NegotiationTest.class);
    
    static {
        Log4jInit.setLevel("org.opencadc.raven", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
    }

    private static final URI CONSIST_RESOURCE_ID = URI.create("ivo://cadc.nrc.ca/minoc");

    ArtifactDAO artifactDAO;
    StorageSiteDAO siteDAO;
    
    public NegotiationTest() throws Exception {
        super();
        this.artifactDAO = new ArtifactDAO(false);
        artifactDAO.setConfig(config);
        this.siteDAO = new StorageSiteDAO(artifactDAO);
        
        InitDatabase init = new InitDatabase(artifactDAO.getDataSource(), DATABASE, SCHEMA);
        init.doInit();
    }
    
    @Before
    public void cleanup() {
        Set<StorageSite> sites = siteDAO.list();
        for (StorageSite s : sites) {
            siteDAO.delete(s.getID());
        }
    }

    @Test
    public void testGetExternalResolver() throws Exception {
        List<Protocol> requested = new ArrayList<>();

        // https+anon
        Protocol sa = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        requested.add(sa);

        // https+cert
        Protocol sc = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        sc.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        requested.add(sc);

        // token
        Protocol st = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        st.setSecurityMethod(Standards.SECURITY_METHOD_TOKEN);
        requested.add(st);

        // test expects that raven gets a grant from dev setting or dev baldur service
        URI mastURI = new URI("mast:FOO/product/no-such-file.fits");
        Transfer transfer = new Transfer(mastURI, Direction.pullFromVoSpace);
        transfer.getProtocols().add(sa);
        transfer.getProtocols().add(sc);
        transfer.getProtocols().add(st);
        transfer.version = VOS.VOSPACE_21;

        URI resourceID = URI.create("ivo://negotiation-test-site1");
        StorageSite site = new StorageSite(resourceID, "site1", true, true);

        // file not in raven. Check external URLs
        Transfer response = Subject.doAs(userSubject, new PrivilegedExceptionAction<Transfer>() {
            public Transfer run() throws Exception {
                return negotiate(transfer);
            }
        });
        log.info("transfer: " + response);

        Assert.assertEquals(requested.size(), response.getAllEndpoints().size());
        for (String endPoint : response.getAllEndpoints()) {
            Assert.assertTrue(endPoint.contains("FOO/product/no-such-file.fits"));
            Assert.assertFalse(endPoint.toLowerCase().contains("minoc"));
        }

        // repeat test after adding the artifact to a location
        URI checksum = URI.create("md5:d41d8cd98f00b204e9800998ecf84278");
        Artifact artifact = new Artifact(mastURI, checksum, new Date(), 1L);

        Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                try {
                    siteDAO.put(site);
                    artifactDAO.put(artifact);
                    final SiteLocation location = new SiteLocation(site.getID());
                    artifactDAO.addSiteLocation(artifact, location);

                    Transfer response = negotiate(transfer);
                    log.info("transfer: " + response);

                    List<String> allEndPoints = response.getAllEndpoints();
                    Assert.assertEquals(2 * requested.size() + 1, allEndPoints.size());
                    // first are the minoc requested.size() + 1 for the pre-auth entries, the rest are external
                    for (int i = 0; i <= requested.size(); i++) {
                        Assert.assertTrue(allEndPoints.get(i).toLowerCase().contains("minoc"));
                    }
                    for (int i = requested.size() + 1; i <= 2 * requested.size(); i++) {
                        Assert.assertFalse(allEndPoints.get(i).toLowerCase().contains("minoc"));
                    }

                    return null;
                } finally {
                    // cleanup sites
                    siteDAO.delete(site.getID());
                    artifactDAO.delete(artifact.getID());
                }
            }
        });


    }

    @Test
    public void testGET() {
        List<Protocol> requested = new ArrayList<>();
        
        // https+anon
        Protocol sa = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        requested.add(sa);

        // https+cert
        Protocol sc = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        sc.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        requested.add(sc);
        
        for (Protocol p : requested) {
            log.info("testGET: " + p);
            try {
                Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                    public Object run() throws Exception {

                        URI resourceID1 = URI.create("ivo://negotiation-test-site1");
                        URI resourceID2 = URI.create("ivo://negotiation-test-site2");


                        StorageSite site1 = new StorageSite(resourceID1, "site1", true, true);
                        StorageSite site2 = new StorageSite(resourceID2, "site2", true, true);

                        URI artifactURI = URI.create("cadc:TEST/" + UUID.randomUUID() + ".fits");
                        URI checksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
                        Artifact artifact = new Artifact(artifactURI, checksum, new Date(), 1L);

                        try {
                            siteDAO.put(site1);
                            siteDAO.put(site2);

                            final SiteLocation location1 = new SiteLocation(site1.getID());
                            final SiteLocation location2 = new SiteLocation(site2.getID());

                            Transfer transfer = new Transfer(artifactURI, Direction.pullFromVoSpace);
                            transfer.getProtocols().add(p);
                            transfer.version = VOS.VOSPACE_21;

                            artifactDAO.put(artifact);

                            // test that there are no copies available
                            try {
                                negotiate(transfer);
                                Assert.fail("should have received file not found exception");
                            } catch (ResourceNotFoundException e) {
                                log.info("caught expected: " + e);
                            }

                            log.info("add: " + location1);
                            artifactDAO.addSiteLocation(artifact, location1);
                            artifact = artifactDAO.get(artifact.getID());

                            // test that there's one copy * 2 URLs per copy
                            Transfer response = negotiate(transfer);
                            log.info("transfer: " + response);
                            if (p.getSecurityMethod() == null || p.getSecurityMethod().equals(Standards.SECURITY_METHOD_ANON)) {
                                // anon: pre-auth URL and plain
                                Assert.assertEquals(2, response.getProtocols().size());
                            } else {
                                // cert: one URL
                                Assert.assertEquals(1, response.getProtocols().size());
                            }
                            
                            Protocol actual = response.getProtocols().get(0);
                            log.info("first: " + actual);
                            
                            
                            if (p.getSecurityMethod() == null || p.getSecurityMethod().equals(Standards.SECURITY_METHOD_ANON)) {
                                Assert.assertNotNull(actual.getEndpoint());
                                Assert.assertEquals(p.getUri(), actual.getUri());
                                Assert.assertEquals(p.getSecurityMethod(), actual.getSecurityMethod());
                            
                                // path: minoc/endpoint/{pre-auth}/cadc:TEST/{uuid}.fits == 5
                                // verify that pre-auth chunk is present in URL
                                String surl = actual.getEndpoint();
                                URL url = new URL(surl);
                                String path = url.getPath().substring(1);
                                log.debug("path: " + path);

                                String[] elems = path.split("/");
                                
                                Assert.assertEquals(5, elems.length);
                                Assert.assertEquals("cadc:TEST", elems[3]);
                                
                                // path: minoc/endpoint/cadc:TEST/{uuid}.fits == 4
                                // verify that pre-auth is NOT present in the second anon URL
                                actual = response.getProtocols().get(1);
                                log.info("second: " + actual);
                                surl = actual.getEndpoint();
                                url = new URL(surl);
                                path = url.getPath().substring(1);
                                log.debug("path: " + path);

                                elems = path.split("/");
                                Assert.assertEquals(4, elems.length);
                                Assert.assertEquals("cadc:TEST", elems[2]);
                                
                            } else {
                                // path: minoc/endpoint/cadc:TEST/{uuid}.fits == 4
                                // verify that pre-auth is NOT present in URL
                                String surl = actual.getEndpoint();
                                URL url = new URL(surl);
                                String path = url.getPath().substring(1);
                                log.debug("path: " + path);

                                String[] elems = path.split("/");
                                
                                Assert.assertEquals(4, elems.length);
                                Assert.assertEquals("cadc:TEST", elems[2]);
                            }

                            log.info("add: " + location2);
                            artifactDAO.addSiteLocation(artifact, location2);
                            artifact = artifactDAO.get(artifact.getID());

                            // test that there are now two copies
                            response = negotiate(transfer);
                            if (p.getSecurityMethod() == null || p.getSecurityMethod().equals(Standards.SECURITY_METHOD_ANON)) {
                                Assert.assertEquals(4, response.getAllEndpoints().size());
                            } else {
                                Assert.assertEquals(2, response.getAllEndpoints().size());
                            }

                            return null;

                        } finally {
                            // cleanup sites
                            siteDAO.delete(site1.getID());
                            siteDAO.delete(site2.getID());
                            artifactDAO.delete(artifact.getID());
                        }
                    }
                });
            } catch (Exception e) {
                log.error("unexpected exception", e);
                Assert.fail("unexpected exception: " + e);
            }
        }
    }

    @Test
    public void testGetWithView() {
        List<Protocol> requested = new ArrayList<>();
        
        // https+anon
        Protocol sa = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        requested.add(sa);

        // https+cert
        Protocol sc = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        sc.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        requested.add(sc);
        
        URI resourceID1 = URI.create("ivo://negotiation-test-site1");

        StorageSite site1 = new StorageSite(resourceID1, "site1", true, true);

        URI artifactURI = URI.create("cadc:TEST/" + UUID.randomUUID() + ".fits");
        URI checksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
        Artifact artifact = new Artifact(artifactURI, checksum, new Date(), 1L);

        try {
            siteDAO.put(site1);

            final SiteLocation location1 = new SiteLocation(site1.getID());

            Transfer transfer = new Transfer(artifactURI, Direction.pullFromVoSpace);
            transfer.getProtocols().addAll(requested);
            transfer.version = VOS.VOSPACE_21;
            
            // ad-hoc soda param view
            String soda = "ivo://ivoa.net/std/SODA";
            transfer.setView(new View(URI.create(soda + "#sync-1.0"))); // standardID
            transfer.getView().getParameters().add(
                    new View.Parameter(URI.create(soda + "#CIRCLE"), "12.0 23.0 0.5"));

            artifactDAO.put(artifact);
            artifactDAO.addSiteLocation(artifact, location1);
            artifact = artifactDAO.get(artifact.getID());
        
            Transfer response = Subject.doAs(userSubject, new PrivilegedExceptionAction<Transfer>() {
                public Transfer run() throws Exception {
                    return negotiate(transfer);
                }
            });
            
            log.info("transfer: " + response);
            for (Protocol p : response.getProtocols()) {
                Assert.assertNotNull(p.getEndpoint());
                URL u = new URL(p.getEndpoint());
                log.info("result URL: " + u);
                Assert.assertNotNull(u.getQuery());
                Assert.assertTrue(u.getQuery().contains("CIRCLE"));
            }
        } catch (Exception e) {
            log.error("unexpected exception", e);
            Assert.fail("unexpected exception: " + e);
        } finally {
            // cleanup sites
            siteDAO.delete(site1.getID());
            artifactDAO.delete(artifact.getID());
        }
    }

    @Test
    public void testPUT() {
        List<Protocol> requested = new ArrayList<>();
        // https+anon
        Protocol sa = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
        requested.add(sa);

        // https+cert
        Protocol sc = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
        sc.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        requested.add(sc);
        
        for (Protocol p : requested) {
            log.info("testPUT: " + p);
            try {
                Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                    public Object run() throws Exception {

                        StorageSite site1 = new StorageSite(URI.create("ivo://negotiation-test-site1"), "site1", true, false);
                        StorageSite site2 = new StorageSite(URI.create("ivo://negotiation-test-site2"), "site2", true, true);
                        StorageSite site3 = new StorageSite(URI.create("ivo://negotiation-test-site3"), "site3", true, true);

                        URI artifactURI = URI.create("cadc:TEST/" + UUID.randomUUID() + ".fits");

                        try {
                            log.debug("adding site1 (not writable)...");
                            siteDAO.put(site1); // not writable

                            Transfer transfer = new Transfer(artifactURI, Direction.pushToVoSpace);
                            transfer.getProtocols().add(p);
                            transfer.version = VOS.VOSPACE_21;

                            log.debug("test that there's no place to put...");
                            Transfer response = negotiate(transfer);
                            Assert.assertEquals(0, response.getAllEndpoints().size());

                            log.debug("adding site2 (writable)...");
                            siteDAO.put(site2); // writable
                            
                            response = negotiate(transfer);
                            Assert.assertEquals(1, response.getProtocols().size());
                            Protocol actual = response.getProtocols().get(0);
                            log.info("actual: " + actual);
                            
                            Assert.assertNotNull(actual.getEndpoint());
                            Assert.assertEquals(p.getUri(), actual.getUri());
                            Assert.assertEquals(p.getSecurityMethod(), actual.getSecurityMethod());
                                                      
                            if (p.getSecurityMethod() == null || p.getSecurityMethod().equals(Standards.SECURITY_METHOD_ANON)) {
                                // verify that pre-auth is present in URL, sort of
                                String surl = actual.getEndpoint();
                                URL url = new URL(surl);
                                String path = url.getPath().substring(1);
                                log.debug("path: " + path);

                                String[] elems = path.split("/");
                                // path: minoc/endpoint/{pre-auth}/cadc:TEST/{uuid}.fits == 5
                                Assert.assertEquals(5, elems.length);
                                Assert.assertEquals("cadc:TEST", elems[3]);
                            } else {
                                // verify that pre-auth is NOT present in URL
                                String surl = actual.getEndpoint();
                                URL url = new URL(surl);
                                String path = url.getPath().substring(1);
                                log.debug("path: " + path);

                                String[] elems = path.split("/");
                                // path: minoc/endpoint/cadc:TEST/{uuid}.fits == 5
                                Assert.assertEquals(4, elems.length);
                                Assert.assertEquals("cadc:TEST", elems[2]);
                            }
                            
                            log.debug("adding site3 (writable)...");
                            siteDAO.put(site3); // writable
                            response = negotiate(transfer);
                            Assert.assertEquals(2, response.getProtocols().size());
                            Assert.assertEquals(2, response.getAllEndpoints().size());

                            return null;

                        } finally {
                            // cleanup sites
                            siteDAO.delete(site1.getID());
                            siteDAO.delete(site2.getID());
                            siteDAO.delete(site3.getID());
                        }
                    }
                });
            } catch (Exception e) {
                log.error("unexpected exception", e);
                Assert.fail("unexpected exception: " + e);
            }
        }
    }
    
    @Test
    public void testMultiProtocol() {
        List<Protocol> protos = new ArrayList<>();
        // https+anon
        Protocol p = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        protos.add(p);

        // https+cert
        p = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        p.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        protos.add(p);

        // https+cookie
        p = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        p.setSecurityMethod(Standards.SECURITY_METHOD_COOKIE);
        protos.add(p);

        // http+anon
        p = new Protocol(VOS.PROTOCOL_HTTP_GET);
        protos.add(p);
        
        // http+cookie
        p = new Protocol(VOS.PROTOCOL_HTTP_GET);
        p.setSecurityMethod(Standards.SECURITY_METHOD_COOKIE);
        protos.add(p);
        
        // sshfs
        p = new Protocol(VOS.PROTOCOL_SSHFS);
        protos.add(p);
                        
        try {
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {

                    URI resourceID1 = URI.create("ivo://negotiation-test-site1");
                    StorageSite site1 = new StorageSite(resourceID1, "site1", true, true);

                    URI artifactURI = URI.create("cadc:TEST/" + UUID.randomUUID() + ".fits");
                    URI checksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
                    Artifact artifact = new Artifact(artifactURI, checksum, new Date(), 1L);

                    try {
                        siteDAO.put(site1);

                        Transfer transfer = new Transfer(artifactURI, Direction.pullFromVoSpace);
                        transfer.getProtocols().addAll(protos);
                        transfer.version = VOS.VOSPACE_21;

                        artifactDAO.put(artifact);
                        
                        SiteLocation location1 = new SiteLocation(site1.getID());
                        artifactDAO.addSiteLocation(artifact, location1);

                        Transfer response = negotiate(transfer);
                        
                        // expect: 4 https supported, no http
                        Assert.assertEquals("protos supported", 4, response.getAllEndpoints().size());
                        
                        boolean foundPreAuthAnon = false;
                        boolean foundPlainAnon = false;
                        for (Protocol ap : response.getProtocols()) {
                            String surl = ap.getEndpoint();
                            log.info("endpoint: " + surl + " " + ap.getSecurityMethod());
                            URL url = new URL(surl);
                            
                            if (VOS.PROTOCOL_HTTPS_GET.equals(ap.getUri())) {
                                Assert.assertEquals("https", url.getProtocol());
                            } else if (VOS.PROTOCOL_HTTP_GET.equals(ap.getUri())) {
                                Assert.assertEquals("http", url.getProtocol());
                            } else {
                                Assert.fail("unexpected response protocol: " + ap.getUri());
                            }
                            
                            String path = url.getPath().substring(1);
                            String[] elems = path.split("/");
                            // path: minoc/endpoint/{pre-auth}/cadc:TEST/{uuid}.fits == 5
                            
                            if (ap.getSecurityMethod() == null) {
                                if (elems.length == 5) {
                                    foundPreAuthAnon = true;
                                    Assert.assertEquals("cadc:TEST", elems[3]);
                                } else if (elems.length == 4) {
                                    foundPlainAnon = true;
                                    Assert.assertEquals("cadc:TEST", elems[2]);
                                } else {
                                    Assert.fail("wrong number of path elements: " + elems.length);
                                }
                            } else {
                                Assert.assertEquals("pre-auth absent", 4, elems.length); // pre-auth absent: caller authenticates to files service
                                Assert.assertEquals("cadc:TEST", elems[2]);
                            }
                        }
                        Assert.assertTrue("pre-auth+anon found", foundPreAuthAnon);
                        Assert.assertTrue("plain+anon found", foundPlainAnon);
                        
                        return null;

                    } finally {
                        // cleanup sites
                        siteDAO.delete(site1.getID());
                        artifactDAO.delete(artifact.getID());
                    }
                }
            });
        } catch (Exception e) {
            log.error("unexpected exception", e);
            Assert.fail("unexpected exception: " + e);
        }
    }

    @Test
    public void testPrioritizedSites() throws Exception {
        /**
         * org.opencadc.raven.putPreference=@SITE1
         * @SITE1.resourceID=ivo://negotiation-test-site1
         * @SITE1.namespaces=cadc:TEST/
         *
         * org.opencadc.raven.putPreference=@SITE2
         * @SITE2.resourceID=ivo://negotiation-test-site2
         * @SITE2.namespaces=cadc:INT-TEST/
         *
         * org.opencadc.raven.putPreference=@SITE3
         * @SITE3.resourceID=ivo://negotiation-test-site3
         * @SITE3.namespaces=cadc:TEST/
         */

        URI testSite1ID = URI.create("ivo://negotiation-test-site1");
        URI testSite2ID = URI.create("ivo://negotiation-test-site2");
        URI testSite3ID = URI.create("ivo://negotiation-test-site3");

        // testSite1 is readonly, testSite1&2 are read write
        StorageSite testSite1 = new StorageSite(testSite1ID, "testSite1", true, false);
        StorageSite testSite2 = new StorageSite(testSite2ID, "testSite2", true, true);
        StorageSite testSite3 = new StorageSite(testSite3ID, "testSite3", true, true);

        RegistryClient registryClient = new RegistryClient();
        URL testSite2URL = registryClient.getServiceURL(testSite2ID, Standards.SI_FILES, AuthMethod.CERT);
        URL testSite3URL = registryClient.getServiceURL(testSite3ID, Standards.SI_FILES, AuthMethod.CERT);

        try {
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {

                    URI artifactURI = URI.create("cadc:TEST/" + UUID.randomUUID() + ".fits");

                    try {
                        // add readonly storage site
                        siteDAO.put(testSite1);

                        Protocol p = new Protocol(VOS.PROTOCOL_HTTPS_GET);
                        p.setSecurityMethod(Standards.SECURITY_METHOD_CERT);

                        Transfer transfer = new Transfer(artifactURI, Direction.pushToVoSpace);
                        transfer.getProtocols().add(p);
                        transfer.version = VOS.VOSPACE_21;
                        Transfer response = negotiate(transfer);

                        // read only storage site should not return an PUT endpoints
                        Assert.assertEquals(0, response.getAllEndpoints().size());

                        // add read write storage site
                        siteDAO.put(testSite2);
                        response = negotiate(transfer);

                        // read write storage site should return a single endpoint
                        Assert.assertEquals(1, response.getProtocols().size());
                        Protocol actual = response.getProtocols().get(0);
                        log.debug("single rw endpoint: " + actual.getEndpoint());
                        Assert.assertNotNull(actual.getEndpoint());
                        Assert.assertEquals(p.getUri(), actual.getUri());
                        Assert.assertTrue(actual.getEndpoint().startsWith(testSite2URL.toString()));

                        // add read write and preferred site
                        siteDAO.put(testSite3);
                        response = negotiate(transfer);

                        // preferred storage site should be first in the list of endpoints
                        Assert.assertEquals(2, response.getAllEndpoints().size());
                        Protocol preferred = response.getProtocols().get(0);
                        Protocol other = response.getProtocols().get(1);
                        log.debug("preferred: " + preferred.getEndpoint());
                        log.debug("other: " + other.getEndpoint());
                        Assert.assertTrue(preferred.getEndpoint().startsWith(testSite3URL.toString()));
                        Assert.assertTrue(other.getEndpoint().startsWith(testSite2URL.toString()));

                        return null;

                    } finally {
                        // cleanup sites
                        siteDAO.delete(testSite1.getID());
                        siteDAO.delete(testSite2.getID());
                        siteDAO.delete(testSite3.getID());
                    }
                }
           });
        } catch (Exception e) {
            log.error("unexpected exception", e);
            Assert.fail("unexpected exception: " + e);
        }
    }

    @Test
    public void testConsistencyPreventNotFound() throws Exception {
        // Tests raven finding artifacts before they are synced from a location.
        // Requires raven to be configured with org.opencadc.raven.consistency.preventNotFound=true
        StorageSite site1 = new StorageSite(CONSIST_RESOURCE_ID, "site1", true, true);
        RegistryClient regClient = new RegistryClient();
        siteDAO.put(site1);
        final URL filesURL = regClient.getServiceURL(CONSIST_RESOURCE_ID, Standards.SI_FILES, AuthMethod.ANON);
        URI artifactURI = URI.create("cadc:TEST/negotiate-test.txt");
        URL artifactURL = new URL(filesURL.toString() + "/" + artifactURI.toString());

        String content = "abcdefghijklmnopqrstuvwxyz1234567890";
        String encoding = "test-encoding";
        String type = "text/plain";
        byte[] data = content.getBytes();
        URI expectedChecksum = TestUtils.computeChecksumURI(data);

        InputStream in = new ByteArrayInputStream(data);
        HttpUpload put = new HttpUpload(in, artifactURL);
        put.setRequestProperty(HttpTransfer.CONTENT_TYPE, type);
        put.setRequestProperty(HttpTransfer.CONTENT_ENCODING, encoding);
        put.setDigest(expectedChecksum);

        Subject.doAs(userSubject, new RunnableAction(put));
        log.info("put: " + put.getResponseCode() + " " + put.getThrowable());
        log.info("headers: " + put.getResponseHeader("content-length") + " " + put.getResponseHeader("digest"));
        Assert.assertNull(put.getThrowable());
        Assert.assertEquals("Created", 201, put.getResponseCode());
        // at this point the artifact is created on remote but it is unknown to raven

        List<Protocol> protos = new ArrayList<>();
        // https+anon
        Protocol sa = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        protos.add(sa);
        // https+cert
        Protocol sc = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        sc.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        protos.add(sc);

        Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {

                Transfer transfer = new Transfer(artifactURI, Direction.pullFromVoSpace);
                transfer.getProtocols().addAll(protos);
                transfer.version = VOS.VOSPACE_21;

                Transfer response = negotiate(transfer);
                log.info("transfer: " + response);
                // 3 URLs: pre-auth anon, anon and cert
                Assert.assertEquals(3, response.getProtocols().size());
                return null;
            }
        });

        // add the artifact to the deleted artifact table. global should return "not found" in this case
        HttpGet locationHead = new HttpGet(artifactURL, false);
        locationHead.setHeadOnly(true);
        locationHead.run();
        UUID artifactID = UUID.fromString(locationHead.getResponseHeader(ProtocolsGenerator.ARTIFACT_ID_HDR));

        DeletedArtifactEventDAO daeDAO = new DeletedArtifactEventDAO(false);
        daeDAO.setConfig(config);
        DeletedArtifactEvent dae = new DeletedArtifactEvent(artifactID);
        daeDAO.put(dae);

        Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {

                Transfer transfer = new Transfer(artifactURI, Direction.pullFromVoSpace);
                transfer.getProtocols().addAll(protos);
                transfer.version = VOS.VOSPACE_21;
                try {
                    negotiate(transfer);
                    Assert.fail("should have received file not found exception");
                } catch (ResourceNotFoundException e) {
                    log.info("caught expected: " + e);
                }
                return null;
            }
        });


    }

}

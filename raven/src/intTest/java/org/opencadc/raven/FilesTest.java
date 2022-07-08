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
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
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
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.db.version.InitDatabase;

/**
 * Test files endpoint.
 * 
 * @author adriand
 */
public class FilesTest extends RavenTest {

    private static final Logger log = Logger.getLogger(FilesTest.class);
    private static final URI RESOURCE_ID1 = URI.create("ivo://negotiation-test-site1");
    private static final URI RESOURCE_ID2 = URI.create("ivo://negotiation-test-site2");

    private static final URI CONSIST_RESOURCE_ID = URI.create("ivo://cadc.nrc.ca/minoc");

    static {
        Log4jInit.setLevel("org.opencadc.raven", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
    }

    ArtifactDAO artifactDAO;
    StorageSiteDAO siteDAO;

    public FilesTest() throws Exception {
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

    public void runGET(String queryString) throws Exception {
        StorageSite site1 = new StorageSite(RESOURCE_ID1, "site1", true, true);
        StorageSite site2 = new StorageSite(RESOURCE_ID2, "site2", true, true);

        URI artifactURI = URI.create("cadc:TEST/" + UUID.randomUUID() + ".fits");
        URI checksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
        Artifact artifact = new Artifact(artifactURI, checksum, new Date(), 1L);

        // override the anonURL and certURL
        RegistryClient regClient = new RegistryClient();
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        URL minocURL1 = regClient.getServiceURL(RESOURCE_ID1, Standards.SI_FILES, authMethod);
        URL ravenURL = regClient.getServiceURL(RAVEN_SERVICE_ID, Standards.SI_FILES, AuthMethod.ANON);
        if (authMethod == AuthMethod.CERT) {
            ravenURL = regClient.getServiceURL(RAVEN_SERVICE_ID, Standards.SI_FILES, AuthMethod.CERT);
        }
        try {
            siteDAO.put(site1);
            siteDAO.put(site2);

            final SiteLocation location1 = new SiteLocation(site1.getID());
            final SiteLocation location2 = new SiteLocation(site2.getID());

            artifactDAO.put(artifact);

            log.info("add: " + location1);
            artifactDAO.addSiteLocation(artifact, location1);
            artifact = artifactDAO.get(artifact.getID());
            String qs = "";
            if (queryString != null) {
                qs += "?" + queryString;
            }

            String au = artifactURI.toASCIIString();
            HttpGet filesGet = new HttpGet(new URL(ravenURL.toString() + "/" + au + qs), false);
            filesGet.run();
            Assert.assertEquals("files response", 303, filesGet.getResponseCode());
            // URL should be of form anonMinocURL1/token/artifactURI
            Assert.assertTrue("redirect URL start", filesGet.getRedirectURL().toString().startsWith(minocURL1.toString()));
            Assert.assertTrue("redirect URL end", filesGet.getRedirectURL().toString().endsWith(artifactURI.toASCIIString() + qs));

            log.info("add: " + location2);
            artifactDAO.addSiteLocation(artifact, location2);
            artifact = artifactDAO.get(artifact.getID());

            // for multiple locations, the service returns the first entry in the list
            // of URLs returned by transfer negotiation.
            Protocol proto = new Protocol(VOS.PROTOCOL_HTTPS_GET);
            proto.setSecurityMethod(Standards.SECURITY_METHOD_ANON);
            
            Transfer transferReq = new Transfer(artifactURI, Direction.pullFromVoSpace);
            transferReq.getProtocols().add(proto);
            transferReq.version = VOS.VOSPACE_21;
            Transfer transferResp = negotiate(transferReq);
            String expectedEndPoint = transferResp.getProtocols().get(0).getEndpoint();

            filesGet = new HttpGet(new URL(ravenURL.toString() + "/" + artifactURI.toASCIIString() + qs), false);
            filesGet.run();
            Assert.assertEquals("files response", 303, filesGet.getResponseCode());
            Assert.assertEquals("files URL", expectedEndPoint + qs, filesGet.getRedirectURL().toString());
    } finally {
            // cleanup sites
            siteDAO.delete(site1.getID());
            siteDAO.delete(site2.getID());
            artifactDAO.delete(artifact.getID());
        }
    }

    @Test
    public void testAnonGET() throws Exception {
        Subject.doAs(anonSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                runGET(null);
                return null;
            }
        });
    }

    @Test
    public void testCertGET() throws Exception {
        Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                runGET(null);
                return null;
            }
        });
    }

    @Test
    public void testParamsGET() throws Exception {
        Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                runGET("SUB=val1");
                runGET("SUB=val1&SUB=val2");
                // query string params map in ca.nrc.cadc.rest.SyncInput is ordered alphabetically by
                // keys (case insensitive) so the order below is maintained and can be tested in the redirect
                // to minoc URL
                runGET("RUNID=1234&SUB=val1&SUB=val2");
                return null;
            }
        });
    }

    @Test
    public void testHEAD() throws Exception {

        URI artifactURI = URI.create("cadc:TEST/" + UUID.randomUUID() + ".fits");
        URI checksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
        Artifact artifact = new Artifact(artifactURI, checksum, new Date(), 1L);
        artifact.contentType = "application/fits";
        try {
            artifactDAO.put(artifact);
            RegistryClient regClient = new RegistryClient();
            final URL anonRavenURL = regClient.getServiceURL(RAVEN_SERVICE_ID, Standards.SI_FILES, AuthMethod.ANON);
            // anon request
            Subject.doAs(anonSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    String au = artifactURI.toASCIIString();
                    URL anonArtifactURL = new URL(anonRavenURL.toString() + "/" + au);
                    HttpGet request = new HttpGet(anonArtifactURL, false);
                    request.setHeadOnly(true);
                    request.run();
                    checkHeadResult(request, artifactURI, 1L, checksum, artifact.contentType, null);
                    return null;
                }
            });
            // repeat for auth request
            final URL certRavenURL = regClient.getServiceURL(RAVEN_SERVICE_ID, Standards.SI_FILES, AuthMethod.CERT);
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    String au = artifactURI.toASCIIString();
                    URL certArtifactURL = new URL(certRavenURL.toString() + "/" + au);
                    HttpGet request = new HttpGet(certArtifactURL, false);
                    request.setHeadOnly(true);
                    request.run();
                    checkHeadResult(request, artifactURI, 1L, checksum, artifact.contentType, null);
                    return null;
                }
            });

            // test HEAD works for files available externally
            final URI externalURI = URI.create("mast::HST/product/iem13occq_trl.fits");
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    String au = externalURI.toASCIIString();
                    URL certArtifactURL = new URL(anonRavenURL.toString() + "/" + au);
                    HttpGet request = new HttpGet(certArtifactURL, false);
                    request.setHeadOnly(true);
                    request.run();
                    Assert.assertEquals("HEAD response code", 303, request.getResponseCode());
                    Assert.assertTrue("Location", request.getResponseHeader("Location").contains("stsci"));
                    return null;
                }
            });

        } finally {
            artifactDAO.delete(artifact.getID());
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
        URI artifactURI = URI.create("cadc:TEST/files-test.txt");
        URL artifactURL = new URL(filesURL.toString() + "/" + artifactURI.toString());

        String content = "abcdefghijklmnopqrstuvwxyz";
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
        final URL anonRavenURL = regClient.getServiceURL(RAVEN_SERVICE_ID, Standards.SI_FILES, AuthMethod.ANON);
        // anon request
        Subject.doAs(anonSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                String au = artifactURI.toASCIIString();
                URL anonArtifactURL = new URL(anonRavenURL.toString() + "/" + au);
                HttpGet head = new HttpGet(anonArtifactURL, false);
                head.setHeadOnly(true);
                head.run();
                checkHeadResult(head, artifactURI, content.length(), expectedChecksum, type, encoding);

                HttpGet get = new HttpGet(anonArtifactURL, true);
                get.run();
                checkHeadResult(get, artifactURI, content.length(), expectedChecksum, type, encoding);
                OutputStream out = new ByteArrayOutputStream();
                final byte[] buffer = new byte[64];
                int bytesRead;
                final InputStream inputStream = get.getInputStream();
                while ((bytesRead = inputStream.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
                Assert.assertEquals("File content", content, out.toString());
                return null;
            }
        });
        // repeat for auth request
        final URL certRavenURL = regClient.getServiceURL(RAVEN_SERVICE_ID, Standards.SI_FILES, AuthMethod.CERT);
        Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                String au = artifactURI.toASCIIString();
                URL certArtifactURL = new URL(certRavenURL.toString() + "/" + au);
                HttpGet head = new HttpGet(certArtifactURL, false);
                head.setHeadOnly(true);
                head.run();
                checkHeadResult(head, artifactURI, content.length(), expectedChecksum, type, encoding);

                HttpGet get = new HttpGet(certArtifactURL, true);
                get.run();
                checkHeadResult(get, artifactURI, content.length(), expectedChecksum, type, encoding);
                OutputStream out = new ByteArrayOutputStream();
                final byte[] buffer = new byte[64];
                int bytesRead;
                final InputStream inputStream = get.getInputStream();
                while ((bytesRead = inputStream.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
                Assert.assertEquals("File content", content, out.toString());
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

        String au = artifactURI.toASCIIString();
        URL anonArtifactURL = new URL(anonRavenURL.toString() + "/" + au);
        HttpGet globalHead = new HttpGet(anonArtifactURL, false);
        globalHead.setHeadOnly(true);
        globalHead.run();
        Assert.assertEquals("Not found expected", 404, globalHead.getResponseCode());

        HttpGet globalGet = new HttpGet(anonArtifactURL, true);
        globalGet.setHeadOnly(false);
        globalGet.run();
        Assert.assertEquals("Not found expected", 404, globalGet.getResponseCode());

    }

    public void checkHeadResult(HttpGet request, URI artifactURI, long size, URI checksum, String contentType, String contentEncoding) {
        Assert.assertEquals("HEAD response code", 200, request.getResponseCode());
        Assert.assertEquals("File length", size, Long.valueOf(request.getResponseHeader("Content-Length")).longValue());
        Assert.assertNotNull("File last-modified", request.getResponseHeader("Last-Modified"));
        Assert.assertEquals("File name", "attachment; filename=\"" + InventoryUtil.computeArtifactFilename(artifactURI) + "\"",
                request.getResponseHeader("Content-Disposition"));
        Assert.assertEquals("File digest", checksum, request.getDigest());
        Assert.assertEquals("File type", contentType, request.getResponseHeader("Content-Type"));
        Assert.assertEquals("File encoding", contentEncoding, request.getResponseHeader("Content-Encoding"));

    }

}


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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
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
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
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

            // also test prototype convenience suuport for default scheme
            //for (String au : new String[]{artifactURI.toASCIIString(), artifactURI.getSchemeSpecificPart()}) {
            {
                String au = artifactURI.toASCIIString();
                HttpGet filesGet = new HttpGet(new URL(ravenURL.toString() + "/" + au + qs), false);
                filesGet.run();
                Assert.assertEquals("files response", 303, filesGet.getResponseCode());
                // URL should be of form anonMinocURL1/token/artifactURI
                Assert.assertTrue("redirect URL start", filesGet.getRedirectURL().toString().startsWith(minocURL1.toString()));
                Assert.assertTrue("redirect URL end", filesGet.getRedirectURL().toString().endsWith(artifactURI.toASCIIString() + qs));
            }

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

            // should work for artifact uri with or without the scheme part
            for (String au : new String[]{artifactURI.toASCIIString(), artifactURI.getSchemeSpecificPart()}) {
                HttpGet filesGet = new HttpGet(new URL(ravenURL.toString() + "/" + artifactURI.toASCIIString() + qs), false);
                filesGet.run();
                Assert.assertEquals("files response", 303, filesGet.getResponseCode());
                Assert.assertEquals("files URL", expectedEndPoint + qs, filesGet.getRedirectURL().toString());
            }
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
                    // also test prototype convenience suuport for default scheme
                    //for (String au : new String[]{artifactURI.toASCIIString(), artifactURI.getSchemeSpecificPart()}) {
                    {
                        String au = artifactURI.toASCIIString();
                        URL anonArtifactURL = new URL(anonRavenURL.toString() + "/" + au);
                        HttpGet request = new HttpGet(anonArtifactURL, false);
                        request.setHeadOnly(true);
                        request.run();
                        checkHeadResult(request, artifactURI, checksum, artifact.contentType);
                    }
                    return null;
                }
            });
            // repeat for auth request
            final URL certRavenURL = regClient.getServiceURL(RAVEN_SERVICE_ID, Standards.SI_FILES, AuthMethod.CERT);
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    // also test prototype convenience suuport for default scheme
                    //for (String au : new String[]{artifactURI.toASCIIString(), artifactURI.getSchemeSpecificPart()}) {
                    {
                        String au = artifactURI.toASCIIString();
                        URL certArtifactURL = new URL(certRavenURL.toString() + "/" + au);
                        HttpGet request = new HttpGet(certArtifactURL, false);
                        request.setHeadOnly(true);
                        request.run();
                        checkHeadResult(request, artifactURI, checksum, artifact.contentType);
                    }
                    return null;
                }
            });
        } finally {
            artifactDAO.delete(artifact.getID());
        }
    }

    public void checkHeadResult(HttpGet request, URI artifactURI, URI checksum, String contentType) {
        Assert.assertEquals("HEAD response code", 200, request.getResponseCode());
        Assert.assertEquals("File length", 1L, Long.valueOf(request.getResponseHeader("Content-Length")).longValue());
        Assert.assertNotNull("File last-modified", request.getResponseHeader("Last-Modified"));
        Assert.assertEquals("File name", "attachment; filename=\"" + InventoryUtil.computeArtifactFilename(artifactURI) + "\"",
                request.getResponseHeader("Content-Disposition"));
        Assert.assertEquals("File digest", checksum, request.getDigest());
        Assert.assertEquals("File type", contentType, request.getResponseHeader("Content-Type"));
        Assert.assertNull("No file encoding", request.getResponseHeader("Content-Encoding"));

    }
}


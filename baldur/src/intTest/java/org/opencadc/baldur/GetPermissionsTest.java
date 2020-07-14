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

package org.opencadc.baldur;

import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.gms.GroupURI;
import org.opencadc.permissions.Grant;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.WriteGrant;
import org.opencadc.permissions.xml.GrantReader;

/**
 * Test HTTP GET to baldur
 * 
 * @author majorb
 */
public class GetPermissionsTest extends BaldurTest {
    
    private static final Logger log = Logger.getLogger(GetPermissionsTest.class);
    
    // This URI matches the single entry in baldur-permissions-config.pem
    URI assetID = URI.create("cadc:TEST/file.fits");
    
    static {
        Log4jInit.setLevel("org.opencadc.baldur", Level.INFO);
    }
    
    public GetPermissionsTest() {
        super();
    }

    @Test
    public void testAnonAccess() {
        try {
            log.info("start - testAnonAccess");
            String artifact = URLEncoder.encode(assetID.toString(), "UTF-8");
            URL getPermissionsURL = new URL(certURL.toString() + "?OP=read&ID=" + artifact);
            HttpGet httpGet = new HttpGet(getPermissionsURL, true);
            try {
                httpGet.prepare();
                Assert.fail("anon get should have thrown AccessControlException");
            }
            catch (AccessControlException expected) {
                Assert.assertEquals(403,httpGet.getResponseCode());
            }
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        } finally {
            log.info("end - testAnonAccess");
        }
    }

    @Test
    public void testForbiddenAccess() {
        try {
            log.info("start - testForbiddenAccess");
            Subject.doAs(noAuthSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    String artifact = URLEncoder.encode(assetID.toString(), "UTF-8");
                    URL getPermissionsURL = new URL(certURL.toString() + "?OP=read&ID=" + artifact);
                    HttpGet httpGet = new HttpGet(getPermissionsURL, true);
                    try {
                        httpGet.prepare();
                        Assert.fail("anon get should have thrown AccessControlException");
                    } catch (AccessControlException expected) {
                        Assert.assertEquals(403,httpGet.getResponseCode());
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        } finally {
            log.info("end - testForbiddenAccess");
        }
    }

    @Test
    public void testCorrectReadPermissions() {
        try {
            log.info("start - testCorrectReadPermissions");
            Subject.doAs(authSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    String artifact = URLEncoder.encode(assetID.toString(), "UTF-8");
                    URL getPermissionsURL = new URL(certURL.toString() + "?OP=read&ID=" + artifact);
                    HttpGet httpGet = new HttpGet(getPermissionsURL, true);
                    httpGet.prepare();
                    Assert.assertNull(httpGet.getThrowable());
                    Assert.assertEquals(200, httpGet.getResponseCode());
                    
                    GrantReader grantReader = new GrantReader();
                    Grant grant = grantReader.read(httpGet.getInputStream());
                    Assert.assertTrue(grant instanceof ReadGrant);
                    ReadGrant readGrant = (ReadGrant) grant;
                    Assert.assertEquals(assetID, readGrant.getAssetID());
                    Assert.assertTrue("isAnon=" + readGrant.isAnonymousAccess(), readGrant.isAnonymousAccess());
                    Assert.assertEquals(2, readGrant.getGroups().size());
                    GroupURI readGroup = new GroupURI("ivo://cadc.nrc.ca/gms?Test-Read");
                    GroupURI readWriteGroup = new GroupURI("ivo://cadc.nrc.ca/gms?Test-Write");
                    Assert.assertTrue(readGrant.getGroups().contains(readGroup));
                    Assert.assertTrue(readGrant.getGroups().contains(readWriteGroup));
                    return null;
                }
            });
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        } finally {
            log.info("end - testCorrectReadPermissions");
        }
    }

    @Test
    public void testCorrectWritePermissions() {
        try {
            log.info("start - testCorrectWritePermissions");
            Subject.doAs(authSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    String artifact = URLEncoder.encode(assetID.toString(), "UTF-8");
                    URL getPermissionsURL = new URL(certURL.toString() + "?OP=write&ID=" + artifact);
                    HttpGet httpGet = new HttpGet(getPermissionsURL, true);
                    httpGet.prepare();
                    Assert.assertNull(httpGet.getThrowable());
                    Assert.assertEquals(200, httpGet.getResponseCode());
                    
                    GrantReader grantReader = new GrantReader();
                    Grant grant = grantReader.read(httpGet.getInputStream());
                    Assert.assertTrue(grant instanceof WriteGrant);
                    WriteGrant writeGrant = (WriteGrant) grant;
                    Assert.assertEquals(assetID, writeGrant.getAssetID());
                    Assert.assertEquals(1, writeGrant.getGroups().size());
                    GroupURI readWriteGroup = new GroupURI("ivo://cadc.nrc.ca/gms?Test-Write");
                    Assert.assertEquals(readWriteGroup, writeGrant.getGroups().get(0));
                    return null;
                }
            });
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        } finally {
            log.info("end - testCorrectWritePermissions");
        }
    }

    @Test
    public void testNoMatchReadPermissions() {
        try {
            log.info("start - testNoMatchReadPermissions");
            String notFoundArtifactURL = "notfound:this.file";
            Subject.doAs(authSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    String artifact = URLEncoder.encode(notFoundArtifactURL, "UTF-8");
                    URL getPermissionsURL = new URL(certURL.toString() + "?OP=read&ID=" + artifact);
                    HttpGet httpGet = new HttpGet(getPermissionsURL, true);
                    try {
                        httpGet.prepare();
                        Assert.fail("should throw ResourceNotFoundException");
                    } catch (ResourceNotFoundException expected) {
                        Assert.assertEquals(404, httpGet.getResponseCode());
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        } finally {
            log.info("end - testNoMatchReadPermissions");
        }
    }

    @Test
    public void testNoMatchWritePermissions() {
        try {
            log.info("start - testNoMatchWritePermissions");
            String notFoundArtifactURL = "notfound:this.file";
            Subject.doAs(authSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    String artifact = URLEncoder.encode(notFoundArtifactURL, "UTF-8");
                    URL getPermissionsURL = new URL(certURL.toString() + "?OP=write&ID=" + artifact);
                    HttpGet httpGet = new HttpGet(getPermissionsURL, true);
                    try {
                        httpGet.prepare();
                        Assert.fail("should throw ResourceNotFoundException");
                    } catch (ResourceNotFoundException expected) {
                        Assert.assertEquals(404, httpGet.getResponseCode());
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        } finally {
            log.info("end - testNoMatchWritePermissions");
        }
    }

    @Test
    public void testInvalidOperation() {
        try {
            log.info("start - testInvalidOperation");
            Subject.doAs(authSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    String artifact = URLEncoder.encode(assetID.toString(), "UTF-8");
                    URL getPermissionsURL = new URL(certURL.toString() + "?OP=nonsense&ID=" + artifact);
                    HttpGet httpGet = new HttpGet(getPermissionsURL, true);
                    try {
                        httpGet.prepare();
                        Assert.fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException expected) {
                        Assert.assertEquals(400, httpGet.getResponseCode());
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        } finally {
            log.info("end - testInvalidOperation");
        }
    }

    @Test
    public void testMissingOperation() {
        try {
            log.info("start - testMissingOperation");
            Subject.doAs(authSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    String artifact = URLEncoder.encode(assetID.toString(), "UTF-8");
                    URL getPermissionsURL = new URL(certURL.toString() + "?ID=" + artifact);
                    HttpGet httpGet = new HttpGet(getPermissionsURL, true);
                    try {
                        httpGet.prepare();
                        Assert.fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException expected) {
                        Assert.assertEquals(400, httpGet.getResponseCode());
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        } finally {
            log.info("end - testMissingOperation");
        }
    }

    @Test
    public void testMissingURI() {
        try {
            log.info("start - testMissingURI");
            Subject.doAs(authSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    URL getPermissionsURL = new URL(certURL.toString() + "?op=read");
                    HttpGet httpGet = new HttpGet(getPermissionsURL, true);
                    try {
                        httpGet.prepare();
                        Assert.fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException expected) {
                        Assert.assertEquals(400, httpGet.getResponseCode());
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        } finally {
            log.info("end - testMissingURI");
        }
    }

    @Test
    public void testInvalidURI() {
        try {
            log.info("start - testInvalidURI");
            
            String invalidURI = "foo bar";
            Subject.doAs(authSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    String artifact = URLEncoder.encode(invalidURI, "UTF-8");
                    URL getPermissionsURL = new URL(certURL.toString() + "?OP=read&ID=" + artifact);
                    HttpGet httpGet = new HttpGet(getPermissionsURL, true);
                    try {
                        httpGet.prepare();
                        Assert.fail("should throw IllegalArgumentException");
                    } catch (IllegalArgumentException expected) {
                        Assert.assertEquals(400, httpGet.getResponseCode());
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        } finally {
            log.info("end - testInvalidURI");
        }
    }
    
}

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

package org.opencadc.minoc;

import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.Log4jInit;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Upload with wrong md5sum and/or length and ensure failure.
 * 
 * @author majorb
 */
public class IncorrectPutMetadataTest extends MinocTest {
    
    private static final Logger log = Logger.getLogger(IncorrectPutMetadataTest.class);
    
    static {
        Log4jInit.setLevel("org.opencadc.minoc", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.net", Level.INFO);
    }
    
    public IncorrectPutMetadataTest() {
        super();
    }
    
    @Test
    public void testUploadContentMD5_Mismatch() {
        try {
            
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
            
                    byte[] bytes = randomData(16384); // 16k
                    final String incorrectData = "incorrect artifact";
                    URI artifactURI = URI.create("cadc:TEST/testUploadContentMD5_Mismatch");
                    URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());
                    
                    // cleanup
                    HttpDelete del = new HttpDelete(artifactURL, false);
                    del.run();
                    // verify
                    HttpGet head = new HttpGet(artifactURL, false);
                    head.setHeadOnly(true);
                    try {
                        head.prepare();
                        Assert.fail("cleanup failed -- file exists: " + artifactURI);
                    } catch (ResourceNotFoundException ok) {
                        log.info("verify not found: " + artifactURL);
                    }
                    
                    // put file
                    InputStream in = new ByteArrayInputStream(bytes);
                    HttpUpload put = new HttpUpload(in, artifactURL);
                    put.setRequestProperty(HttpTransfer.CONTENT_MD5, computeMD5(incorrectData.getBytes()));
                    put.setRequestProperty(HttpTransfer.CONTENT_LENGTH, Long.toString(bytes.length));
                    put.run();
                    log.info("response code: " + put.getResponseCode() + " " + put.getThrowable());
                    //log.info("throwable", put.getThrowable());
                    Assert.assertEquals("should be 412, precondition failed", 412, put.getResponseCode());
            
                    // verify
                    head = new HttpGet(artifactURL, false);
                    head.setHeadOnly(true);
                    try {
                        head.prepare();
                        Assert.fail("put succeeded -- file exists: " + artifactURI);
                    } catch (ResourceNotFoundException ok) {
                        log.info("verify not found: " + artifactURL);
                    }
            
                    return null;
                }
            });
            
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        }
    }
    
    @Test
    public void testUploadContentLengthHeader_TooLarge() {
        try {
            
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
            
                    byte[] bytes = randomData(16384); // 16KiB
                    URI artifactURI = URI.create("cadc:TEST/testUploadContentLengthHeader_TooLarge");
                    URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());
                    
                    // cleanup
                    HttpDelete del = new HttpDelete(artifactURL, false);
                    del.run();
                    // verify
                    HttpGet head = new HttpGet(artifactURL, false);
                    head.setHeadOnly(true);
                    try {
                        head.prepare();
                        Assert.fail("cleanup failed -- file exists: " + artifactURI);
                    } catch (ResourceNotFoundException ok) {
                        log.info("verify not found: " + artifactURL);
                    }
                    
                    // put file
                    InputStream in = new ByteArrayInputStream(bytes);
                    HttpUpload put = new HttpUpload(in, artifactURL);
                    put.setRequestProperty("X-Test-Method", "testUploadContentLengthHeader_TooLarge");
                    put.setRequestProperty(HttpTransfer.CONTENT_LENGTH, Long.toString(bytes.length + 1L));
                    put.run();
                    log.info("response code: " + put.getResponseCode() + " " + put.getThrowable());
                    Assert.assertNotNull(put.getThrowable());
                    
                    // cannot verify the correct response code and exception because client side detects the
                    // failure when using setFixedLengthStreamingMode
                    //log.info("throwable", put.getThrowable());
                    //Assert.assertEquals("should be 412, precondition failed", 412, put.getResponseCode());
            
                    // verify
                    head = new HttpGet(artifactURL, false);
                    head.setHeadOnly(true);
                    try {
                        head.prepare();
                        Assert.fail("put succeeded -- file exists: " + artifactURI);
                    } catch (ResourceNotFoundException ok) {
                        log.info("verify not found: " + artifactURL);
                    }
                    
                    return null;
                }
            });
            
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        }
    }
    
    @Test
    public void testUploadContentLengthHeader_TooSmall() {
        try {
            
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
            
                    byte[] bytes = randomData(16386); // 16k
                    URI artifactURI = URI.create("cadc:TEST/testUploadContentLengthHeader_TooSmall");
                    URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());
                    
                    // cleanup
                    HttpDelete del = new HttpDelete(artifactURL, false);
                    del.run();
                    // verify
                    HttpGet head = new HttpGet(artifactURL, false);
                    head.setHeadOnly(true);
                    try {
                        head.prepare();
                        Assert.fail("cleanup failed -- file exists: " + artifactURI);
                    } catch (ResourceNotFoundException ok) {
                        log.info("verify not found: " + artifactURL);
                    }
                    
                    // put file
                    InputStream in = new ByteArrayInputStream(bytes);
                    HttpUpload put = new HttpUpload(in, artifactURL);
                    put.setRequestProperty(HttpTransfer.CONTENT_LENGTH, Long.toString(bytes.length + 1L));
                    put.setRequestProperty("X-Test-Method", "testUploadContentLengthHeader_TooSmall");
                    put.run();
                    log.info("response code: " + put.getResponseCode() + " " + put.getThrowable());
                    Assert.assertNotNull(put.getThrowable());
                    
                    // cannot verify the correct response code and exception because client side detects the
                    // failure when using setFixedLengthStreamingMode
                    //log.info("throwable", put.getThrowable());
                    //Assert.assertEquals("should be 412, precondition failed", 412, put.getResponseCode());
            
                    // verify
                    head = new HttpGet(artifactURL, false);
                    head.setHeadOnly(true);
                    try {
                        head.prepare();
                        Assert.fail("put succeeded -- file exists: " + artifactURI);
                    } catch (ResourceNotFoundException ok) {
                        log.info("verify not found: " + artifactURL);
                    }
            
                    return null;
                }
            });
            
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        }
    }
    
    @Test
    public void testUploadContentMD5_Correct_ContentLengthHeader_TooSmall() {
        try {
            
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
            
                    String data = "first artifact";
                    byte[] bytes = data.getBytes();
                    URI artifactURI = URI.create("cadc:TEST/testUploadContentMD5_Correct_ContentLengthHeader_TooSmall");
                    URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());
                    
                    // cleanup
                    HttpDelete del = new HttpDelete(artifactURL, false);
                    del.run();
                    // verify
                    HttpGet head = new HttpGet(artifactURL, false);
                    head.setHeadOnly(true);
                    try {
                        head.prepare();
                        Assert.fail("cleanup failed -- file exists: " + artifactURI);
                    } catch (ResourceNotFoundException ok) {
                        log.info("verify not found: " + artifactURL);
                    }
                    
                    // put file
                    InputStream in = new ByteArrayInputStream(bytes);
                    HttpUpload put = new HttpUpload(in, artifactURL);
                    put.setRequestProperty(HttpTransfer.CONTENT_MD5, computeMD5(bytes));
                    put.setRequestProperty(HttpTransfer.CONTENT_LENGTH, Long.toString((long) bytes.length - 1L));
                    put.run();
                    log.info("response code: " + put.getResponseCode() + " " + put.getThrowable());
                    Assert.assertNotNull(put.getThrowable());
                    
                    // cannot verify the correct response code and exception because client side detects the
                    // failure when using setFixedLengthStreamingMode
                    //log.info("throwable", put.getThrowable());
                    //Assert.assertEquals("should be 412, precondition failed", 412, put.getResponseCode());
            
                    // verify
                    head = new HttpGet(artifactURL, false);
                    head.setHeadOnly(true);
                    try {
                        head.prepare();
                        Assert.fail("put succeeded -- file exists: " + artifactURI);
                    } catch (ResourceNotFoundException ok) {
                        log.info("verify not found: " + artifactURL);
                    }
            
                    return null;
                }
            });
            
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        }
    }
    
    @Test
    public void testUpload_ContentMD5_ContentLength_Correct() {
        try {
            
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
            
                    String data = "first artifact";
                    byte[] bytes = data.getBytes();
                    URI artifactURI = URI.create("cadc:TEST/testUpload_ContentMD5_ContentLength_Correct");
                    URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());
                    
                    // cleanup
                    HttpDelete del = new HttpDelete(artifactURL, false);
                    del.run();
                    // verify
                    HttpGet head = new HttpGet(artifactURL, false);
                    head.setHeadOnly(true);
                    try {
                        head.prepare();
                        Assert.fail("cleanup failed -- file exists: " + artifactURI);
                    } catch (ResourceNotFoundException ok) {
                        log.info("verify not found: " + artifactURL);
                    }
                    
                    // put file
                    InputStream in = new ByteArrayInputStream(bytes);
                    HttpUpload put = new HttpUpload(in, artifactURL);
                    put.setRequestProperty(HttpTransfer.CONTENT_MD5, computeMD5(bytes));
                    put.setRequestProperty(HttpTransfer.CONTENT_LENGTH, Long.toString((long) bytes.length));

                    put.run();
                    log.info("response code: " + put.getResponseCode() + " " + put.getThrowable());
                    Assert.assertNull(put.getThrowable());
                    Assert.assertEquals("should be 200, ok", 200, put.getResponseCode());
                    
                    // verify
                    head = new HttpGet(artifactURL, false);
                    head.setHeadOnly(true);
                    try {
                        head.prepare();
                    } catch (Exception ok) {
                        Assert.fail("verify failed: " + artifactURI);
                    }
                    
                    return null;
                }
            });
            
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        }
    }
    
}

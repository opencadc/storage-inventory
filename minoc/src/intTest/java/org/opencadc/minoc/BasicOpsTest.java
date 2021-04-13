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

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.Log4jInit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test PUT, GET, DELETE, POST, HEAD and common errors
 * 
 * @author majorb
 */
public class BasicOpsTest extends MinocTest {
    
    private static final Logger log = Logger.getLogger(BasicOpsTest.class);
    
    static {
        Log4jInit.setLevel("org.opencadc.minoc", Level.INFO);
    }
    
    public BasicOpsTest() {
        super();
    }
    
    @Test
    public void testAllMethodsSimple() {
        try {
            URI artifactURI = URI.create("cadc:TEST/file.txt");
            URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());

            String content = "abcdefghijklmnopqrstuvwxyz";
            String encoding = "test-encoding";
            String type = "text/plain";
            byte[] data = content.getBytes();

            // put: no length or checksum
            InputStream in = new ByteArrayInputStream(data);
            HttpUpload put = new HttpUpload(in, artifactURL);
            put.setRequestProperty(HttpTransfer.CONTENT_TYPE, type);
            put.setRequestProperty(HttpTransfer.CONTENT_ENCODING, encoding);
            put.setDigest(computeChecksumURI(data));

            Subject.doAs(userSubject, new RunnableAction(put));
            Assert.assertNull(put.getThrowable());

            // get
            OutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(artifactURL, out);
            Subject.doAs(userSubject, new RunnableAction(get));
            Assert.assertNull(get.getThrowable());
            URI checksumURI = get.getDigest();
            long contentLength = get.getContentLength();
            String contentType = get.getContentType();
            String contentEncoding = get.getContentEncoding();
            Assert.assertEquals(computeChecksumURI(data), checksumURI);
            Assert.assertEquals(data.length, contentLength);
            Assert.assertEquals(type, contentType);
            Assert.assertEquals(encoding, contentEncoding);

            // update
            // TODO: add update to artifactURI when functionality available
            String newEncoding = "test-encoding-2";
            String newType = "application/x-text-message";
            Map<String,Object> params = new HashMap<String,Object>(2);
            params.put("contentEncoding", newEncoding);
            params.put("contentType", newType);
            HttpPost post = new HttpPost(artifactURL, params, false);
            post.setDigest(computeChecksumURI(data));
            Subject.doAs(userSubject, new RunnableAction(post));
            Assert.assertNull(post.getThrowable());

            // head
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpGet head = new HttpGet(artifactURL, bos);
            head.setHeadOnly(true);
            Subject.doAs(userSubject, new RunnableAction(head));
            log.warn("head output: " + bos.toString());
            Assert.assertNull(head.getThrowable());
            checksumURI = head.getDigest();
            contentLength = head.getContentLength();
            contentType = head.getContentType();
            contentEncoding = head.getContentEncoding();
            Assert.assertEquals(computeChecksumURI(data), checksumURI);
            Assert.assertEquals(data.length, contentLength);
            Assert.assertEquals(newType, contentType);
            Assert.assertEquals(newEncoding, contentEncoding);

            // delete
            HttpDelete delete = new HttpDelete(artifactURL, false);
            Subject.doAs(userSubject, new RunnableAction(delete));
            Assert.assertNull(delete.getThrowable());

            // get
            get = new HttpGet(artifactURL, out);
            Subject.doAs(userSubject, new RunnableAction(get));
            Throwable throwable = get.getThrowable();
            Assert.assertNotNull(throwable);
            Assert.assertTrue(throwable instanceof ResourceNotFoundException);
            
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        }
    }
    
    @Test
    public void testGetNotFound() {
        try {
            
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                
                    URI artifactURI = URI.create("cadc:TEST/testGetNotFound");
                    URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());
                    
                    // get
                    OutputStream out = new ByteArrayOutputStream();
                    HttpGet get = new HttpGet(artifactURL, out);
                    get.run();
                    Assert.assertNotNull(get.getThrowable());
                    Assert.assertEquals("should be 404, not found", 404, get.getResponseCode());
                    Assert.assertTrue(get.getThrowable() instanceof ResourceNotFoundException);
                
                    return null;
                }
            });
            
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        }
    }
    
    @Test
    public void testDeleteNotFound() {
        try {
            
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                
                    URI artifactURI = URI.create("cadc:TEST/testDeleteNotFound");
                    URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());
                    
                    // delete
                    HttpDelete delete = new HttpDelete(artifactURL, false);
                    delete.run();
                    Assert.assertNotNull(delete.getThrowable());
                    Assert.assertEquals("should be 404, not found", 404, delete.getResponseCode());
                    System.out.println(delete.getThrowable());
                    Assert.assertTrue(delete.getThrowable() instanceof ResourceNotFoundException);
                
                    return null;
                }
            });
            
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        }
    }
    
    @Test
    public void testZeroLengthFile() {
        try {
            
            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
            
                    String data = "";
                    URI artifactURI = URI.create("cadc:TEST/testZeroLengthFile");
                    URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());
                    String type = "text/plain";
                    
                    // put
                    InputStream in = new ByteArrayInputStream(data.getBytes());
                    HttpUpload put = new HttpUpload(in, artifactURL);
                    put.setRequestProperty(HttpTransfer.CONTENT_TYPE, type);
                    put.run();
                    Assert.assertEquals("should be 400, bad request", 400, put.getResponseCode());
                    
                    return null;
                }
            });
            
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        }
    }
}

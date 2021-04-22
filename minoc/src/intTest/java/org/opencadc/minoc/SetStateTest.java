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

package org.opencadc.minoc;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.XMLConstants;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vosi.Availability;
import ca.nrc.cadc.vosi.VOSI;
import ca.nrc.cadc.xml.XmlUtil;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test PUT, GET, DELETE, POST, HEAD and common errors
 * Note: This test the minoc-test.pem cert and requires minoc-availability.properties 
 *       to be in minoc/config/. The following entry needs to be in the 
 *       minoc-availability.properties file.
 *       users=CN=cadcauthtest1_24c, OU=cadc, O=hia, C=ca
 * 
 * @author yeunga
 */
public class SetStateTest extends MinocTest {
    
    private static final Logger log = Logger.getLogger(SetStateTest.class);
    private static final Map<String, String> AVAIL_SCHEMA_MAP = new TreeMap<>();
    
    static {
        Log4jInit.setLevel("org.opencadc.minoc", Level.INFO);
        AVAIL_SCHEMA_MAP.putAll(XMLConstants.SCHEMA_MAP);
	String localURL = XmlUtil.getResourceUrlString(VOSI.AVAILABILITY_SCHEMA, XMLConstants.class);
        AVAIL_SCHEMA_MAP.put(VOSI.AVAILABILITY_NS_URI.toString(), localURL);
    }
    
    public SetStateTest() {
        super();
    }
    
    @Test
    public void testSetOffline() throws Exception {
        String testState = RestAction.STATE_OFFLINE;
        String originalState = null;

        try {
            originalState = getState();
            if (!originalState.equals(testState)) {
                // set minoc state to 'Offline'
                setState(testState);
                Assert.assertEquals("Failed to set state to " + testState, testState, getState());
            }

            // test put, post, get, head, delete actions
            URI artifactURI = URI.create("cadc:TEST/testSetOffline.txt");
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
            Assert.assertNotNull("put to minoc in offline state should have failed", put.getThrowable());
            String exMsg = put.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from put", exMsg.contains("System is offline"));

            // get
            OutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(artifactURL, out);
            Subject.doAs(userSubject, new RunnableAction(get));
            Assert.assertNotNull("get from minoc in offline state should have failed", get.getThrowable());
            exMsg = put.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from get", exMsg.contains("System is offline"));

            // update
            // TODO: add update to artifactURI when functionality available
            String newEncoding = "test-encoding-2";
            String newType = "application/x-text-message";
            Map<String,Object> postParams = new HashMap<String,Object>(2);
            postParams.put("contentEncoding", newEncoding);
            postParams.put("contentType", newType);
            HttpPost post = new HttpPost(artifactURL, postParams, false);
            post.setDigest(computeChecksumURI(data));
            Subject.doAs(userSubject, new RunnableAction(post));
            Assert.assertNotNull("post to minoc in offline state should have failed", post.getThrowable());
            exMsg = post.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from post", exMsg.contains("System is offline"));

            // head
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpGet head = new HttpGet(artifactURL, bos);
            Subject.doAs(userSubject, new RunnableAction(head));
            Assert.assertNotNull("head from minoc in offline state should have failed", head.getThrowable());
            exMsg = head.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from head", exMsg.contains("System is offline"));

            // delete
            HttpDelete delete = new HttpDelete(artifactURL, false);
            Subject.doAs(userSubject, new RunnableAction(delete));
            Assert.assertNotNull("delete from minoc in offline state should have failed", delete.getThrowable());
            exMsg = delete.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from delete", exMsg.contains("System is offline"));
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        } finally {
	    if (!originalState.equals(testState)) {
	        // restore minoc to its original state
	        setState(originalState);
                Assert.assertEquals("Failed to set state to " + originalState, originalState, getState());
	    }
	}
    }
            
    @Test
    public void testSetReadOnly() throws Exception {
        String testState = RestAction.STATE_READ_ONLY;
        String originalState = null;

        try {
            originalState = getState();
            if (!originalState.equals(testState)) {
                // set minoc state to 'ReadOnly'
                setState(testState);
                Assert.assertEquals("Failed to set state to " + testState, testState, getState());
            }

            // set minoc state to 'ReadWrite' so that we can write a test file 
	    // to test the subsequent get action
            String state = RestAction.STATE_READ_WRITE;
	    setState(state);

            // test get action
            URI artifactURI = URI.create("cadc:TEST/testSetReadOnly.txt");
            URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());

            String content = "abcdefghijklmnopqrstuvwxyz";
            String encoding = "test-encoding";
            String type = "text/plain";
            byte[] data = content.getBytes();

            // put a file so that we can test get: no length or checksum
            InputStream in = new ByteArrayInputStream(data);
            HttpUpload put = new HttpUpload(in, artifactURL);
            put.setRequestProperty(HttpTransfer.CONTENT_TYPE, type);
            put.setRequestProperty(HttpTransfer.CONTENT_ENCODING, encoding);
            put.setDigest(computeChecksumURI(data));
            Subject.doAs(userSubject, new RunnableAction(put));
            Assert.assertNull(put.getThrowable());

            // get should be successful
            OutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(artifactURL, out);
            Subject.doAs(userSubject, new RunnableAction(get));
            Assert.assertNull(get.getThrowable());

            // set minoc state to 'ReadOnly'
            state = RestAction.STATE_READ_ONLY;
	    setState(state);

            // test put, post, head, delete actions
            // put: no length or checksum
            in = new ByteArrayInputStream(data);
            put = new HttpUpload(in, artifactURL);
            put.setRequestProperty(HttpTransfer.CONTENT_TYPE, type);
            put.setRequestProperty(HttpTransfer.CONTENT_ENCODING, encoding);
            put.setDigest(computeChecksumURI(data));
            Subject.doAs(userSubject, new RunnableAction(put));
            Assert.assertNotNull("put to minoc in read-only state should have failed", put.getThrowable());
            String exMsg = put.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from put", exMsg.contains("System is in read-only"));

            // update
            // TODO: add update to artifactURI when functionality available
            String newEncoding = "test-encoding-2";
            String newType = "application/x-text-message";
            Map<String,Object> postParams = new HashMap<String,Object>(2);
            postParams.put("contentEncoding", newEncoding);
            postParams.put("contentType", newType);
            HttpPost post = new HttpPost(artifactURL, postParams, false);
            post.setDigest(computeChecksumURI(data));
            Subject.doAs(userSubject, new RunnableAction(post));
            Assert.assertNotNull("post to minoc in read-only state should have failed", post.getThrowable());
            exMsg = post.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from post", exMsg.contains("System is in read-only"));

            // head
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpGet head = new HttpGet(artifactURL, bos);
            head.setHeadOnly(true);
            Subject.doAs(userSubject, new RunnableAction(head));
            log.warn("head output: " + bos.toString());
            Assert.assertNull(head.getThrowable());

            // delete
            HttpDelete delete = new HttpDelete(artifactURL, false);
            Subject.doAs(userSubject, new RunnableAction(delete));
            Assert.assertNotNull("delete from minoc in read-only state should have failed", delete.getThrowable());
            exMsg = delete.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from delete", exMsg.contains("System is in read-only"));
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        } finally {
	    if (!originalState.equals(testState)) {
	        // restore minoc to its original state
	        setState(originalState);
                Assert.assertEquals("Failed to set state to " + originalState, originalState, getState());
	    }
	}
    }
            
    @Test
    public void testSetReadWrite() throws Exception {
        String testState = RestAction.STATE_READ_WRITE;
        String originalState = null;

        try {
            originalState = getState();
            if (!originalState.equals(testState)) {
                // set minoc state to 'ReadWrite'
                setState(testState);
                Assert.assertEquals("Failed to set state to " + testState, testState, getState());
            }

            URI artifactURI = URI.create("cadc:TEST/testSetReadWrite.txt");
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

            // update
            // TODO: add update to artifactURI when functionality available
            String newEncoding = "test-encoding-2";
            Map<String,Object> postParams = new HashMap<String,Object>(2);
            postParams.put("contentEncoding", newEncoding);
            HttpPost post = new HttpPost(artifactURL, postParams, false);
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
        } finally {
	    if (!originalState.equals(testState)) {
	        // restore minoc to its original state
	        setState(originalState);
                Assert.assertEquals("Failed to set state to " + originalState, originalState, getState());
	    }
	}
    }
            
    private String getState() throws Exception {
        // get minoc state 
        RegistryClient regClient = new RegistryClient();
        URL availabilityURL = regClient.getServiceURL(MINOC_SERVICE_ID, Standards.VOSI_AVAILABILITY, AuthMethod.CERT);
        HttpGet getState = new HttpGet(availabilityURL, true);
        getState.prepare();

        Document xml = XmlUtil.buildDocument(getState.getInputStream(), AVAIL_SCHEMA_MAP);
        Availability availability = new Availability(xml);
	String note = availability.note;
        String state = RestAction.STATE_READ_WRITE;
        if (note.contains("offline")) {
            state = RestAction.STATE_OFFLINE;
        } else if (note.contains("read-only")) {
            state = RestAction.STATE_READ_ONLY;
        } else {
            Assert.assertTrue("unknown state: " + note, note.contains("accepting requests")); 
        }

	return state;
    }
            
    private void setState(String state) throws Exception {
        RegistryClient regClient = new RegistryClient();
        URL availabilityURL = regClient.getServiceURL(MINOC_SERVICE_ID, Standards.VOSI_AVAILABILITY, AuthMethod.CERT);

        // set minoc state 
        Map<String,Object> params = new TreeMap<>();
        params.put("state", state);
        HttpPost setState = new HttpPost(availabilityURL, params, false);
        Subject.doAs(userSubject, new RunnableAction(setState));
    }
}

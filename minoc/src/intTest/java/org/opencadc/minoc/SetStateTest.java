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
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.XMLConstants;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vosi.Availability;
import ca.nrc.cadc.vosi.VOSI;
import ca.nrc.cadc.xml.XmlUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
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
    private static final String INITIAL_STATE = RestAction.STATE_READ_WRITE;
    
    static {
        Log4jInit.setLevel("org.opencadc.minoc", Level.INFO);
        AVAIL_SCHEMA_MAP.putAll(XMLConstants.SCHEMA_MAP);
        String localURL = XmlUtil.getResourceUrlString(VOSI.AVAILABILITY_SCHEMA, XMLConstants.class);
        AVAIL_SCHEMA_MAP.put(VOSI.AVAILABILITY_NS_URI, localURL);
    }
    
    private URL availabilityURL;
    
    public SetStateTest() {
        super();
        RegistryClient regClient = new RegistryClient();
        this.availabilityURL = regClient.getServiceURL(MINOC_SERVICE_ID, Standards.VOSI_AVAILABILITY, AuthMethod.CERT);
        log.info("availabilityURL: " + availabilityURL);
    }
    
    @Test
    public void testSetOffline() throws Exception {
        String testMethod = "testSetOffline()";
        String testState = RestAction.STATE_OFFLINE;
        String expectedMsg = RestAction.STATE_OFFLINE_MSG;

        try {
            // start test with 'ReadWrite' state
            setState(INITIAL_STATE);
            Assert.assertEquals("Failed to set state to " + INITIAL_STATE, INITIAL_STATE, getState());

            // set minoc state to 'Offline'
            setState(testState);
            Assert.assertEquals("Failed to set state to " + testState, testState, getState());

            // test put, post, get, head, delete actions
            URI artifactURI = URI.create("cadc:TEST/testSetOffline.txt");
            URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());

            String content = "abcdefghijklmnopqrstuvwxyz";
            String encoding = "test-encoding";
            byte[] data = content.getBytes();

            // put: no length or checksum
            InputStream in = new ByteArrayInputStream(data);
            HttpUpload put = new HttpUpload(in, artifactURL);
            put.setRequestProperty(HttpTransfer.CONTENT_LENGTH, String.valueOf(content.length()));

            put.setRetry(1, 0, HttpTransfer.RetryReason.TRANSIENT);
            Subject.doAs(userSubject, new RunnableAction(put));
            int responseCode = put.getResponseCode();
            log.info(testMethod + " put response: " + responseCode + " " + put.getThrowable());
            Assert.assertEquals("incorrect response code", 503, responseCode);
            Assert.assertNotNull("put to minoc in offline state should have failed", put.getThrowable());
            String exMsg = put.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from put", exMsg.contains(expectedMsg));

            // get
            OutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(artifactURL, out);
            get.setRetry(1, 0, HttpTransfer.RetryReason.TRANSIENT);
            Subject.doAs(userSubject, new RunnableAction(get));
            responseCode = get.getResponseCode();
            log.info(testMethod + " get response: " + responseCode + " " + get.getThrowable());
            Assert.assertEquals("incorrect response code", 503, responseCode);
            Assert.assertNotNull("get from minoc in offline state should have failed", get.getThrowable());
            exMsg = get.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from get", exMsg.contains(expectedMsg));

            // update
            String newEncoding = "test-encoding-2";
            Map<String,Object> postParams = new HashMap<String,Object>(2);
            postParams.put("contentEncoding", newEncoding);
            HttpPost post = new HttpPost(artifactURL, postParams, false);
            post.setRetry(1, 0, HttpTransfer.RetryReason.TRANSIENT);
            Subject.doAs(userSubject, new RunnableAction(post));
            responseCode = post.getResponseCode();
            log.info(testMethod + " post response: " + responseCode + " " + post.getThrowable());
            Assert.assertEquals("incorrect response code", 503, responseCode);
            Assert.assertNotNull("post to minoc in offline state should have failed", post.getThrowable());
            exMsg = post.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from post", exMsg.contains(expectedMsg));

            // head
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpGet head = new HttpGet(artifactURL, bos);
            head.setRetry(1, 0, HttpTransfer.RetryReason.TRANSIENT);
            Subject.doAs(userSubject, new RunnableAction(head));
            responseCode = head.getResponseCode();
            log.info(testMethod + " head response: " + responseCode + " " + head.getThrowable());
            Assert.assertEquals("incorrect response code", 503, responseCode);
            Assert.assertNotNull("head from minoc in offline state should have failed", head.getThrowable());
            exMsg = head.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from head", exMsg.contains(expectedMsg));

            // delete
            HttpDelete delete = new HttpDelete(artifactURL, false);
            delete.setRetry(1, 0, HttpTransfer.RetryReason.TRANSIENT);
            Subject.doAs(userSubject, new RunnableAction(delete));
            responseCode = head.getResponseCode();
            log.info(testMethod + " delete response: " + responseCode + " " + delete.getThrowable());
            Assert.assertEquals("incorrect response code", 503, responseCode);
            Assert.assertNotNull("delete from minoc in offline state should have failed", delete.getThrowable());
            exMsg = delete.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from delete", exMsg.contains(expectedMsg));
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        } finally {
            // restore minoc to its intial state
            setState(INITIAL_STATE);
            Assert.assertEquals("Failed to set state to " + INITIAL_STATE, INITIAL_STATE, getState());
        }
    }
            
    @Test
    public void testSetReadOnly() throws Exception {
        String testMethod = "testSetReadOnly()";
        String testState = RestAction.STATE_READ_ONLY;
        String expectedMsg = RestAction.STATE_READ_ONLY_MSG;
        
        try {
            // start test with 'ReadWrite' state
            setState(INITIAL_STATE);
            Assert.assertEquals("Failed to set state to " + INITIAL_STATE, INITIAL_STATE, getState());

            // test get action
            URI artifactURI = URI.create("cadc:TEST/testSetReadOnly.txt");
            URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());

            String content = "abcdefghijklmnopqrstuvwxyz";
            String encoding = "test-encoding";
            byte[] data = content.getBytes();

            // put a file so that we can test get
            InputStream in = new ByteArrayInputStream(data);
            HttpUpload put = new HttpUpload(in, artifactURL);
            put.setRequestProperty(HttpTransfer.CONTENT_LENGTH, String.valueOf(content.length()));
            Subject.doAs(userSubject, new RunnableAction(put));
            int responseCode = put.getResponseCode();
            log.info(testMethod + " put response: " + responseCode + " " + put.getThrowable());
            Assert.assertEquals("incorrect response code", 200, responseCode);
            Assert.assertNull(put.getThrowable());

            // set minoc state to 'ReadOnly'
            setState(testState);
            Assert.assertEquals("Failed to set state to " + testState, testState, getState());

            // get should be successful
            OutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(artifactURL, out);
            Subject.doAs(userSubject, new RunnableAction(get));
            responseCode = get.getResponseCode();
            log.info(testMethod + " get response: " + responseCode + " " + get.getThrowable());
            Assert.assertEquals("incorrect response code", 200, responseCode);
            Assert.assertNull(get.getThrowable());

            // test put, post, head, delete actions
            // put: no length or checksum
            in = new ByteArrayInputStream(data);
            put = new HttpUpload(in, artifactURL);
            put.setRequestProperty(HttpTransfer.CONTENT_LENGTH, String.valueOf(content.length()));
	    put.setRetry(1, 0, HttpTransfer.RetryReason.TRANSIENT);
            Subject.doAs(userSubject, new RunnableAction(put));
            responseCode = put.getResponseCode();
            log.info(testMethod + " put response: " + responseCode + " " + put.getThrowable());
            Assert.assertEquals("incorrect response code", 503, responseCode);
            Assert.assertNotNull("put to minoc in read-only state should have failed", put.getThrowable());
            String exMsg = put.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from put", exMsg.contains(expectedMsg));

            // update
            String newEncoding = "test-encoding-2";
            Map<String,Object> postParams = new HashMap<String,Object>(2);
            postParams.put("contentEncoding", newEncoding);
            HttpPost post = new HttpPost(artifactURL, postParams, false);
            post.setRetry(1, 0, HttpTransfer.RetryReason.TRANSIENT);
            Subject.doAs(userSubject, new RunnableAction(post));
            responseCode = post.getResponseCode();
            log.info(testMethod + " post response: " + responseCode + " " + post.getThrowable());
            Assert.assertEquals("incorrect response code", 503, responseCode);
            Assert.assertNotNull("post to minoc in read-only state should have failed", post.getThrowable());
            exMsg = post.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from post", exMsg.contains(expectedMsg));

            // head
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpGet head = new HttpGet(artifactURL, bos);
            head.setHeadOnly(true);
            Subject.doAs(userSubject, new RunnableAction(head));
            responseCode = head.getResponseCode();
            log.info(testMethod + " head response: " + responseCode + " " + head.getThrowable());
            Assert.assertEquals("incorrect response code", 200, responseCode);
            Assert.assertNull(head.getThrowable());

            // delete
            HttpDelete delete = new HttpDelete(artifactURL, false);
            delete.setRetry(1, 0, HttpTransfer.RetryReason.TRANSIENT);
            Subject.doAs(userSubject, new RunnableAction(delete));
            responseCode = delete.getResponseCode();
            log.info(testMethod + " delete response: " + responseCode + " " + delete.getThrowable());
            Assert.assertEquals("incorrect response code", 503, responseCode);
            Assert.assertNotNull("delete from minoc in read-only state should have failed", delete.getThrowable());
            exMsg = delete.getThrowable().getMessage();
            Assert.assertTrue("incorrect exception message from delete", exMsg.contains(expectedMsg));
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
        } finally {
            // restore minoc to its intial state
            setState(INITIAL_STATE);
            Assert.assertEquals("Failed to set state to " + INITIAL_STATE, INITIAL_STATE, getState());
        }
    }
    
    @Test
    public void testStateSwitch() {
        try {
            String msg = "Test switching from read-write to itself and to other states";
            log.info(msg);
            switchState(RestAction.STATE_READ_WRITE, RestAction.STATE_READ_WRITE);
            switchState(RestAction.STATE_READ_WRITE, RestAction.STATE_READ_ONLY);
            switchState(RestAction.STATE_READ_WRITE, RestAction.STATE_OFFLINE);

            msg = "Test switch from read-only to itself and to other states";
            log.info(msg);
            switchState(RestAction.STATE_READ_ONLY, RestAction.STATE_READ_ONLY);
            switchState(RestAction.STATE_READ_ONLY, RestAction.STATE_READ_WRITE);
            switchState(RestAction.STATE_READ_ONLY, RestAction.STATE_OFFLINE);

            msg = "Test switch from offline to itself and to other states";
            log.info(msg);
            switchState(RestAction.STATE_OFFLINE, RestAction.STATE_OFFLINE);
            switchState(RestAction.STATE_OFFLINE, RestAction.STATE_READ_WRITE);
            switchState(RestAction.STATE_OFFLINE, RestAction.STATE_READ_ONLY);
        } catch (Exception t) {
            log.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
            
        }
    }
            
    private void switchState(String fromState, String toState) throws Exception {
        String testMethod = "switchState";
        // start with the fromState
        setState(fromState);
        Assert.assertEquals(testMethod + " failed to set state to " + fromState, fromState, getState());
 
        // switch from fromState to toState
        setState(toState);
        Assert.assertEquals(testMethod + " failed to switch from " + fromState + " to " + toState, toState, getState());
    }
    
    private String getState() throws Exception {
        // get minoc state 
        HttpGet getState = new HttpGet(availabilityURL, true);
        getState.prepare();

        int responseCode = getState.getResponseCode();
        Assert.assertEquals("incorrect response code", 200, responseCode);
        if (getState.getThrowable() != null) {
            log.info("getState() response throwable message is '" + getState.getThrowable().getMessage() + "'");
        }

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

        log.info("getState response code is " + responseCode + " and returns state " + state);
        return state;
    }
            
    private void setState(String state) throws Exception {
        // set minoc state 
        Map<String,Object> params = new TreeMap<>();
        params.put("state", state);
        HttpPost setState = new HttpPost(availabilityURL, params, false);
        Subject.doAs(userSubject, new RunnableAction(setState));

        int responseCode = setState.getResponseCode();
        log.info("setState() " + state + " response code is " + responseCode);
        Assert.assertEquals("incorrect response code", 302, responseCode);
        if (setState.getThrowable() != null) {
            log.info("setState() " + state + " response throwable message is '" + setState.getThrowable().getMessage() + "'");
        }
    }
}

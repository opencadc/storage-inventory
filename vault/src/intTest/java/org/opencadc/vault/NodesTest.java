/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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

package org.opencadc.vault;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.util.Random;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.conformance.vos.VOSTest;
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferParsingException;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;
import javax.security.auth.Subject;

/**
 * Test the nodes endpoint.
 * 
 * @author pdowler
 */
public class NodesTest extends org.opencadc.conformance.vos.NodesTest {
    private static final Logger log = Logger.getLogger(NodesTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.conformance.vos", Level.INFO);
        Log4jInit.setLevel("org.opencadc.vospace", Level.INFO);
        Log4jInit.setLevel("org.opencadc.vault", Level.INFO);
    }
    
    public NodesTest() {
        super(Constants.RESOURCE_ID, Constants.ADMIN_CERT);
        
        enablePermissionTests(Constants.ALT_GROUP, Constants.ALT_CERT);
        
        // vault does not check the actual groups in the permission props tests, hence they can be made up.
        enablePermissionPropsTest(new GroupURI(URI.create("ivo://myauth/gms?gr1")), new GroupURI(URI.create("ivo://myauth/gms?gr2")));
    }

    @Test
    public void testDataNodeProps() {
        try {
            // create a simple data node
            String name = "testDataNode";
            URL nodeURL = getNodeURL(nodesServiceURL, name);
            VOSURI nodeURI = getVOSURI(name);

            // cleanup
            delete(nodeURL, false);

            // PUT the node
            log.info("put: " + nodeURI + " -> " + nodeURL);
            DataNode testNode = new DataNode(name);
            put(nodeURL, nodeURI, testNode);

            // GET the new node
            NodeReader.NodeReaderResult result = get(nodeURL, 200, XML_CONTENT_TYPE);
            log.info("found: " + result.vosURI + " owner: " + result.node.ownerDisplay);
            Assert.assertTrue(result.node instanceof DataNode);
            DataNode persistedNode = (DataNode) result.node;
            for (NodeProperty np : persistedNode.getProperties()) {
                log.info("persisted prop: " + np.getKey() + " = " + np.getValue());
            }
            Assert.assertEquals(testNode, persistedNode);
            Assert.assertEquals(nodeURI, result.vosURI);
            Assert.assertNull(persistedNode.getProperty(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertNull(persistedNode.getProperty(VOS.PROPERTY_URI_CONTENTDATE));

            // Push some data to the node
            pushData(nodeURI.getURI());

            result = get(nodeURL, 200, XML_CONTENT_TYPE);
            log.info("found: " + result.vosURI + " owner: " + result.node.ownerDisplay);
            Assert.assertTrue(result.node instanceof DataNode);
            persistedNode = (DataNode) result.node;
            for (NodeProperty np : persistedNode.getProperties()) {
                log.info("persisted prop: " + np.getKey() + " = " + np.getValue());
            }
            Assert.assertEquals(testNode, persistedNode);
            Assert.assertEquals(nodeURI, result.vosURI);
            // expect these to be returned by the GET after the upload
            Assert.assertNotNull(persistedNode.getProperty(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertNotNull(persistedNode.getProperty(VOS.PROPERTY_URI_CONTENTDATE));

            if (cleanupOnSuccess) {
                delete(nodeURL);
            }
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    private void pushData(URI testURI) throws Exception {
        // Create a push-to-vospace Transfer for the node
        Transfer pushTransfer = new Transfer(testURI, Direction.pushToVoSpace);
        pushTransfer.version = VOS.VOSPACE_21;
        pushTransfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTPS_PUT)); // anon, preauth
        Protocol putWithCert = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
        putWithCert.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        pushTransfer.getProtocols().add(putWithCert);

        // negotiate the transfer
        Transfer details = doTransfer(pushTransfer);
        Assert.assertEquals("expected transfer direction = " + Direction.pushToVoSpace,
                Direction.pushToVoSpace, details.getDirection());
        Assert.assertNotNull(details.getProtocols());
        log.info(pushTransfer.getDirection() + " results: " + details.getProtocols().size());
        URL putURL = null;
        for (Protocol p : details.getProtocols()) {
            String endpoint = p.getEndpoint();
            try {

                URL u = new URL(endpoint);
                if (putURL == null) {
                    putURL = u; // first
                }
            } catch (MalformedURLException e) {
                Assert.fail(String.format("invalid protocol endpoint: %s because %s", endpoint, e.getMessage()));
            }
        }
        Assert.assertNotNull(putURL);

        // put the bytes
        Random rnd = new Random();
        byte[] data = new byte[1024];
        rnd.nextBytes(data);
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(data);

        // put data
        md5.reset();
        md5.update(data);
        URI digestURI = URI.create("md5:" + HexUtil.toHex(md5.digest()));
        FileContent content = new FileContent(data, "application/octet-stream");
        HttpUpload put = new HttpUpload(content, putURL);
        put.setDigest(digestURI);
        put.run();
        log.info("put accept: " + put.getResponseCode() + " " + put.getThrowable() + " " + put.getDigest());
        Assert.assertEquals(201, put.getResponseCode());
        Assert.assertNull(put.getThrowable());
        Assert.assertEquals(digestURI, put.getDigest());

    }

    protected Transfer doTransfer(Transfer transfer) throws IOException, TransferParsingException {
        // Write a transfer document
        TransferWriter transferWriter = new TransferWriter();
        StringWriter sw = new StringWriter();
        transferWriter.write(transfer, sw);
        log.debug("POST Transfer XML: " + sw);

        // POST the transfer document
        FileContent fileContent = new FileContent(sw.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
        HttpPost post = new HttpPost(synctransServiceURL, fileContent, false);
        Subject.doAs(authSubject, new RunnableAction(post));
        Assert.assertEquals("expected POST response code = 303",303, post.getResponseCode());
        Assert.assertNull("expected POST throwable == null", post.getThrowable());

        // Get the updated transfer
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(post.getRedirectURL(), out);
        log.debug("GET: " + post.getRedirectURL());
        Subject.doAs(authSubject, new RunnableAction(get));
        log.debug("GET responseCode: " + get.getResponseCode());
        Assert.assertEquals("expected GET response code = 200", 200, get.getResponseCode());
        Assert.assertNull("expected GET throwable == null", get.getThrowable());
        Assert.assertTrue("expected GET Content-Type starts with " + VOSTest.XML_CONTENT_TYPE,
                get.getContentType().startsWith(VOSTest.XML_CONTENT_TYPE));

        // Read the transfer
        log.debug("GET Transfer XML: " + out);
        TransferReader transferReader = new TransferReader();
        return transferReader.read(out.toString(), "vos");
    }

}

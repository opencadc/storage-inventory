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
 * Test heartbeat under different service states.
 * 
 * @author yeunga
 */
public class HeartbeatTest extends MinocTest {
    
    private static final Logger log = Logger.getLogger(HeartbeatTest.class);
    private static final Map<String, String> AVAIL_SCHEMA_MAP = new TreeMap<>();
    private static final String OFFLINE_STATE = RestAction.STATE_OFFLINE;
    private static final String READ_WRITE_STATE = RestAction.STATE_READ_WRITE;
    
    static {
        Log4jInit.setLevel("org.opencadc.minoc", Level.INFO);
        AVAIL_SCHEMA_MAP.putAll(XMLConstants.SCHEMA_MAP);
        String localURL = XmlUtil.getResourceUrlString(VOSI.AVAILABILITY_SCHEMA, XMLConstants.class);
        AVAIL_SCHEMA_MAP.put(VOSI.AVAILABILITY_NS_URI, localURL);
    }
    
    private URL availabilityURL;
    
    public HeartbeatTest() {
        super();
        RegistryClient regClient = new RegistryClient();
        this.availabilityURL = regClient.getServiceURL(MINOC_SERVICE_ID, Standards.VOSI_AVAILABILITY, AuthMethod.CERT);
        log.info("availabilityURL: " + availabilityURL);
    }
    
    @Test
    public void testHeartbeat() throws Exception {
        String testMethod = "testSetOffline()";
        String testState = RestAction.STATE_OFFLINE;
        String expectedMsg = RestAction.STATE_OFFLINE_MSG;
        int offlineCode = 503;

        try {
            // set minoc state to 'Offline'
            setState(OFFLINE_STATE);
            Assert.assertEquals("Failed to set state to " + OFFLINE_STATE, OFFLINE_STATE, getState());
            boolean heartbeat = getHeartbeat(offlineCode);
            Assert.assertEquals("Incorrect heartbeat " + heartbeat, false, heartbeat);
        } finally {
            // restore minoc to its intial state
            setState(RestAction.STATE_READ_WRITE);
            Assert.assertEquals("Failed to set state to " + RestAction.STATE_READ_WRITE, RestAction.STATE_READ_WRITE, getState());
        }

    }
            
    private boolean getHeartbeat(int expectedCode) throws Exception {
        // get heartbeat 
        boolean isOk = false;
        URL heartbeatURL = new URL(availabilityURL.toExternalForm() + "?detail=min");
        HttpGet getHeartbeat = new HttpGet(heartbeatURL, true);
        getHeartbeat.run();

        int responseCode = getHeartbeat.getResponseCode();
        Assert.assertEquals("incorrect response code", expectedCode, responseCode);
        log.info("getHeartbeat response code is " + responseCode);
        isOk = true;
        if (getHeartbeat.getThrowable() != null) {
            isOk = false;
            log.info("getHeartbeat() response throwable message is '" + getHeartbeat.getThrowable().getMessage() + "'");
        }

        return isOk;
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

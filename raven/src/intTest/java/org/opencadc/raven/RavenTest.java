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

package org.opencadc.raven;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferParsingException;
import ca.nrc.cadc.vos.TransferReader;
import ca.nrc.cadc.vos.TransferWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.TreeMap;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.opencadc.inventory.db.SQLGenerator;

/**
 * Abstract integration test class with general setup and test support.
 * 
 * @author majorb
 */
public abstract class RavenTest {
    
    private static final Logger log = Logger.getLogger(NegotiationTest.class);
    public static final URI RAVEN_SERVICE_ID = URI.create("ivo://cadc.nrc.ca/raven");
    // TODO: Move this to Standards.java
    private static final URI RAVEN_STANDARD_ID = URI.create("vos://cadc.nrc.ca~vospace/CADC/std/inventory#locate-1.0");
    
    static String SERVER = "INVENTORY_TEST";
    static String DATABASE = "content";
    static String SCHEMA = "inventory";

    protected URL anonURL;
    protected URL certURL;
    protected Subject anonSubject;
    protected Subject userSubject;
    
    Map<String,Object> config;
    
    static {
        Log4jInit.setLevel("org.opencadc.raven", Level.INFO);
    }
    
    public RavenTest() throws Exception {
        RegistryClient regClient = new RegistryClient();
        anonURL = regClient.getServiceURL(RAVEN_SERVICE_ID, RAVEN_STANDARD_ID, AuthMethod.ANON);
        log.info("anonURL: " + anonURL);
        certURL = regClient.getServiceURL(RAVEN_SERVICE_ID, RAVEN_STANDARD_ID, AuthMethod.CERT);
        log.info("certURL: " + certURL);
        anonSubject = AuthenticationUtil.getAnonSubject();
        File cert = FileUtil.getFileFromResource("raven-test.pem", RavenTest.class);
        log.info("userSubject: " + userSubject);
        userSubject = SSLUtil.createSubject(cert);
        log.info("userSubject: " + userSubject);
        
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(SERVER, DATABASE);
            DBUtil.createJNDIDataSource("jdbc/inventory", cc);

            config = new TreeMap<String,Object>();
            config.put(SQLGenerator.class.getName(), SQLGenerator.class);
            config.put("jndiDataSourceName", "jdbc/inventory");
            config.put("schema", SCHEMA);

        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw ex;
        }
    }
    
    protected Transfer negotiate(Transfer request) throws IOException, TransferParsingException, PrivilegedActionException {
        TransferWriter writer = new TransferWriter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.write(request, out);
        FileContent content = new FileContent(out.toByteArray(), "text/xml");
        URL negotiateURL = new URL(certURL.toString() + "/locate");
        HttpPost post = new HttpPost(negotiateURL, content, false);
        post.run();
        if (post.getThrowable() != null && post.getThrowable() instanceof FileNotFoundException) {
            throw (FileNotFoundException) post.getThrowable();
        }
        Assert.assertNull(post.getThrowable());
        String response = post.getResponseBody();
        TransferReader reader = new TransferReader();
        return reader.read(response, null);
    }

}

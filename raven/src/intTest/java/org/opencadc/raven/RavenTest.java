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

package org.opencadc.raven;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedActionException;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferParsingException;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;

/**
 * Abstract integration test class with general setup and test support.
 * 
 * @author majorb
 */
public abstract class RavenTest {
    private static final Logger log = Logger.getLogger(NegotiationTest.class);

    static final URI RAVEN_SERVICE_ID = URI.create("ivo://opencadc.org/raven");
    static final URI CONSIST_RESOURCE_ID = URI.create("ivo://opencadc.org/minoc");
    
    static String SERVER = "INVENTORY_TEST";
    static String DATABASE = "cadctest";
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
        anonURL = regClient.getServiceURL(RAVEN_SERVICE_ID, Standards.SI_LOCATE, AuthMethod.ANON);
        log.info("anonURL: " + anonURL);
        certURL = regClient.getServiceURL(RAVEN_SERVICE_ID, Standards.SI_LOCATE, AuthMethod.CERT);
        log.info("certURL: " + certURL);
        anonSubject = AuthenticationUtil.getAnonSubject();
        File cert = FileUtil.getFileFromResource("raven-test.pem", RavenTest.class);
        userSubject = SSLUtil.createSubject(cert);
        log.info("userSubject: " + userSubject);
        
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(SERVER, DATABASE);
            DBUtil.createJNDIDataSource("jdbc/inventory", cc);

            config = new TreeMap<String,Object>();
            config.put(SQLGenerator.class.getName(), SQLGenerator.class);
            config.put("jndiDataSourceName", "jdbc/inventory");
            config.put("invSchema", SCHEMA);
            //config.put("genSchema", SCHEMA);

        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw ex;
        }
    }
    
    protected Transfer negotiate(Transfer request)
        throws InterruptedException, IOException, TransferParsingException, PrivilegedActionException, ResourceNotFoundException {
        TransferWriter writer = new TransferWriter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.write(request, out);
        FileContent content = new FileContent(out.toByteArray(), "text/xml");
        HttpPost post = new HttpPost(certURL, content, false);
        try {
            post.prepare();
            TransferReader reader = new TransferReader();
            Transfer t = reader.read(post.getInputStream(),  null);
            log.debug("Response transfer: " + t);
            return t;
        } catch (ResourceAlreadyExistsException  ex) {
            throw new RuntimeException("unexpected exception", ex);
        }
    }

}

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

package org.opencadc.inventory.db;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.KeyPair;
import org.opencadc.inventory.db.version.InitDatabase;

/**
 *
 * @author pdowler
 */
public class KeyPairDAOTest {
    private static final Logger log = Logger.getLogger(KeyPairDAOTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.DEBUG);
    }

    KeyPairDAO dao = new KeyPairDAO();
    
    public KeyPairDAOTest()throws Exception {
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
        DBUtil.createJNDIDataSource("jdbc/KeyPairDAOTest", cc);
        
        Map<String,Object> config = new TreeMap<String,Object>();
        config.put(SQLGenerator.class.getName(), SQLGenerator.class);
        config.put("jndiDataSourceName", "jdbc/KeyPairDAOTest");
        config.put("database", TestUtil.DATABASE);
        config.put("schema", TestUtil.SCHEMA);
        dao.setConfig(config);
    }
    
    @Before
    public void setup()
        throws Exception
    {
        log.info("init database...");
        InitDatabase init = new InitDatabase(dao.getDataSource(), TestUtil.DATABASE, TestUtil.SCHEMA);
        init.doInit();
        log.info("init database... OK");
        
        log.info("clearing old content...");
        SQLGenerator gen = dao.getSQLGenerator();
        DataSource ds = dao.getDataSource();
        String sql = "delete from " + gen.getTable(KeyPair.class);
        log.info("pre-test cleanup: " + sql);
        ds.getConnection().createStatement().execute(sql);
        log.info("clearing old content... OK");
    }
    
    @Test
    public void testPutGetUpdateDelete() {
        String name = "testPutGetUpdateDelete";
        Random rnd = new Random();
        byte[] publicKey = new byte[128];
        rnd.nextBytes(publicKey);
        byte[] privateKey = new byte[512];
        rnd.nextBytes(privateKey);
        

        try {
            KeyPair expected = new KeyPair(name, publicKey, privateKey);

            KeyPair notFound = dao.get(expected.getID());
            Assert.assertNull(notFound);
        
            dao.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            URI mcs = expected.getMetaChecksum();
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs, mcs0);
            
            // get by ID
            KeyPair fid = dao.get(expected.getID());
            Assert.assertNotNull(fid);
            Assert.assertEquals(expected.getName(), fid.getName());
            Assert.assertEquals(expected.getPublicKey().length, fid.getPublicKey().length);
            Assert.assertEquals(expected.getPrivateKey().length, fid.getPrivateKey().length);
            URI mcs1 = fid.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", mcs, mcs1);
            
            // get by name
            fid = dao.get(name);
            Assert.assertNotNull(fid);
            Assert.assertEquals(expected.getName(), fid.getName());
            Assert.assertEquals(expected.getPublicKey().length, fid.getPublicKey().length);
            Assert.assertEquals(expected.getPrivateKey().length, fid.getPrivateKey().length);
            URI mcs2 = fid.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", mcs, mcs2);
            
            // TODO: udpate
            
            // list
            Set<KeyPair> keys = dao.list();
            Assert.assertNotNull(keys);
            Assert.assertEquals(1, keys.size());
            Iterator<KeyPair> iter = keys.iterator();
            Assert.assertTrue(iter.hasNext());
            KeyPair actual = iter.next();
            Assert.assertEquals(expected.getPublicKey().length, fid.getPublicKey().length);
            Assert.assertEquals(expected.getPrivateKey().length, fid.getPrivateKey().length);
            URI mcs3 = fid.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", mcs, mcs3);
            
            // delete
            dao.delete(expected.getID());
            KeyPair deleted = dao.get(expected.getID());
            Assert.assertNull(deleted);
        
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}

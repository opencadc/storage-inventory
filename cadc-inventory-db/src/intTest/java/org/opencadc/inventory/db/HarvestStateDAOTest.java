/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.db.version.InitDatabase;

/**
 *
 * @author pdowler
 */
public class HarvestStateDAOTest {
    private static final Logger log = Logger.getLogger(HarvestStateDAOTest.class);

    static final URI RID = URI.create("ivo://cadc.nrc.ca/no-lookup-luskan");
    
    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
    }
    
    HarvestStateDAO dao = new HarvestStateDAO();
    
    public HarvestStateDAOTest() throws Exception {
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
            DBUtil.createJNDIDataSource("jdbc/HarvestStateDAOTest", cc);

            Map<String,Object> config = new TreeMap<String,Object>();
            config.put(SQLGenerator.class.getName(), SQLGenerator.class);
            config.put("jndiDataSourceName", "jdbc/HarvestStateDAOTest");
            config.put("database", TestUtil.DATABASE);
            config.put("schema", TestUtil.SCHEMA);
            dao.setConfig(config);
        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw ex;
        }
    }
    
    @Before
    public void setup() throws Exception {
        log.info("init database...");
        InitDatabase init = new InitDatabase(dao.getDataSource(), TestUtil.DATABASE, TestUtil.SCHEMA);
        init.doInit();
        log.info("init database... OK");
        
        log.info("clearing old content...");
        SQLGenerator gen = dao.getSQLGenerator();
        DataSource ds = dao.getDataSource();
        String sql = "delete from " + gen.getTable(HarvestState.class);
        log.info("pre-test cleanup: " + sql);
        ds.getConnection().createStatement().execute(sql);
        log.info("clearing old content... OK");
    }
    
    @Test
    public void testPutGetUpdateDelete() {
        try {
            HarvestState expected = new HarvestState(HarvestStateDAOTest.class.getSimpleName(), RID);
            log.info("expected: " + expected);
            
            
            HarvestState notFound = dao.get(HarvestState.class, expected.getID());
            Assert.assertNull(notFound);
            
            // curently null curLastModified
            dao.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            // get by ID
            HarvestState hs1 = dao.get(expected.getID());
            log.info("found: " + hs1);
            
            Assert.assertNotNull("find by uuid", hs1);
            Assert.assertEquals(expected.getLastModified(), hs1.getLastModified());
            Assert.assertEquals(expected.getMetaChecksum(), hs1.getMetaChecksum());
            URI mcs1 = hs1.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", expected.getMetaChecksum(), mcs1);
            
            //TransactionManager txn = dao.getTransactionManager();
            //txn.startTransaction();
            
            // update
            hs1.curLastModified = new Date();
            hs1.curID = UUID.randomUUID();
            dao.put(hs1);
            
            //log.warn("SLEEPING for lock diagnostics: 20 sec");
            //Thread.sleep(20000L);
            //txn.commitTransaction();
            
            HarvestState hs2 = dao.get(hs1.getID());
            log.info("found: " + hs2);
            
            Assert.assertNotNull("find by uuid", hs1);
            Assert.assertNotEquals(expected.getLastModified(), hs2.getLastModified());
            Assert.assertNotEquals(expected.getMetaChecksum(), hs2.getMetaChecksum());
            Assert.assertEquals("round trip metachecksum", hs1.getMetaChecksum(), hs2.getMetaChecksum());
            Assert.assertEquals("curLastModified", hs1.curLastModified.getTime(), hs2.curLastModified.getTime());
            Assert.assertEquals("curID", hs1.curID, hs2.curID);
            
            // clear tracking state
            hs1.curLastModified = null;
            hs1.curID = null;
            dao.put(hs1);
            HarvestState hs3 = dao.get(hs1.getID());
            log.info("found: " + hs3);
            Assert.assertNotNull(hs3);
            Assert.assertEquals("round trip metachecksum", hs1.getMetaChecksum(), hs3.getMetaChecksum());
            Assert.assertNotEquals("checksum changed", hs2.getMetaChecksum(), hs3.getMetaChecksum());
            Assert.assertNull("curLastModified", hs3.curLastModified);
            Assert.assertNull("curID", hs3.curID);
            
            dao.delete(expected.getID());
            HarvestState deleted = dao.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetBySource() {
        try {
            HarvestState expected = new HarvestState(HarvestStateDAOTest.class.getSimpleName(), RID);
            log.info("expected: " + expected);
            
            dao.put(expected);
            
            // get by ID
            HarvestState fid = dao.get(expected.getID());
            Assert.assertNotNull("find by uuid", fid);
            
            // get by location
            
            HarvestState hs1 = dao.get(expected.getName(), expected.getResourceID());
            log.info("found: " + hs1);
            Assert.assertNotNull("find by source", hs1);
            Assert.assertEquals(expected.getName(), hs1.getName());
            Assert.assertEquals(expected.getResourceID(), hs1.getResourceID());
            
            dao.delete(expected.getID());
            HarvestState deleted = dao.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testRenameSelf() {
        try {
            // orig: fully qualified class name
            HarvestState orig = new HarvestState(HarvestStateDAOTest.class.getName(), RID);
            log.info("expected: " + orig);
            
            HarvestState notFound = dao.get(HarvestState.class, orig.getID());
            Assert.assertNull(notFound);
            
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);
            
            // get by ID
            HarvestState hs1 = dao.get(orig.getID());
            log.info("found by ID: " + hs1);
            
            // get by source,cname 
            HarvestState hs2 = dao.get(orig.getName(), orig.getResourceID());
            log.info("found by name: " + hs2);
            
            log.info("update: rename to simple class name");
            hs2.setName(HarvestStateDAOTest.class.getSimpleName());
            dao.put(hs2);
            
            HarvestState hs3 = dao.get(hs2.getName(), hs2.getResourceID());
            log.info("found by name: " + hs3);
            Assert.assertNotNull(hs3);
            Assert.assertEquals(orig.getID(), hs3.getID());
            Assert.assertEquals(orig.curID, hs3.curID);
            
            dao.delete(hs3.getID());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testBufferedUpdates() {
        try {
            dao.setUpdateBufferCount(2); // buffer 2, write 3rd
            
            // orig: fully qualified class name
            HarvestState orig = new HarvestState(HarvestStateDAOTest.class.getName(), RID);
            log.info("expected: " + orig);
            
            HarvestState hs = dao.get(HarvestState.class, orig.getID());
            Assert.assertNull(hs);
            
            // put 1
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);
            hs = dao.get(HarvestState.class, orig.getID());
            Assert.assertNull(hs);

            // put 2
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);
            hs = dao.get(HarvestState.class, orig.getID());
            Assert.assertNull(hs);

            // put 3
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);
            
            // get by ID
            HarvestState save1 = dao.get(orig.getID());
            log.info("found by ID after put x3: " + save1);
            Assert.assertNotNull(save1);
            Assert.assertEquals(orig.curID, save1.curID);

            // put 1
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);
            hs = dao.get(HarvestState.class, orig.getID());
            Assert.assertNotNull(hs);
            Assert.assertEquals(save1.curID, hs.curID);

            // put 2
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);
            hs = dao.get(HarvestState.class, orig.getID());
            Assert.assertNotNull(hs);
            Assert.assertEquals(save1.curID, hs.curID);
            
            dao.flushBufferedState();
            HarvestState save2 = dao.get(orig.getID());
            log.info("found by ID after flush: " + save2);
            Assert.assertNotNull(save2);
            Assert.assertEquals(orig.curID, save2.curID);
            
            dao.delete(save1.getID());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            dao.setUpdateBufferCount(0);
        }
    }
    
    @Test
    public void testPeriodicMaintenance() {
        try {
            // orig: fully qualified class name
            HarvestState orig = new HarvestState(HarvestStateDAOTest.class.getName(), RID);
            log.info("expected: " + orig);
            
            HarvestState hs = dao.get(HarvestState.class, orig.getID());
            Assert.assertNull(hs);

            // create a bunch of dead tuples
            for (int i = 0; i < 101; i++) {
                orig.curID = UUID.randomUUID();
                orig.curLastModified = new Date();
                dao.put(orig);
            }
            
            // enable frequnt maintenance
            dao.setMaintCount(2);
            
            // put 1
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);
            
            // put 2
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);
            
            // put 3 -- first maint
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);
            
            // put 4
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);

            // put 5
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);
            
            // put 6 -- second maint
            orig.curID = UUID.randomUUID();
            orig.curLastModified = new Date();
            dao.put(orig);
            
            //dao.delete(orig.getID());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            dao.setMaintCount(-1);
        }
    }
}

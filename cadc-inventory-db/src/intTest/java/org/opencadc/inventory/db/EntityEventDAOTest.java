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
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocationEvent;
import org.opencadc.inventory.db.version.InitDatabase;

/**
 *
 * @author pdowler
 */
public class EntityEventDAOTest {
    private static final Logger log = Logger.getLogger(EntityEventDAOTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.INFO);
    }
    
    DeletedArtifactEventDAO daeDAO = new DeletedArtifactEventDAO();
    DeletedStorageLocationEventDAO dslDAO = new DeletedStorageLocationEventDAO();
    StorageLocationEventDAO slDAO = new StorageLocationEventDAO();
    
    public EntityEventDAOTest() throws Exception {
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
        DBUtil.createJNDIDataSource("jdbc/EntityEventDAOTest", cc);
        
        Map<String,Object> config = new TreeMap<String,Object>();
        config.put(SQLGenerator.class.getName(), SQLGenerator.class);
        config.put("jndiDataSourceName", "jdbc/EntityEventDAOTest");
        config.put("database", TestUtil.DATABASE);
        config.put("schema", TestUtil.SCHEMA);
        daeDAO.setConfig(config);
        dslDAO.setConfig(config);
        slDAO.setConfig(config);
    }
    
    @Before
    public void setup()
        throws Exception {
        log.info("init database...");
        InitDatabase init = new InitDatabase(daeDAO.getDataSource(), TestUtil.DATABASE, TestUtil.SCHEMA);
        init.doInit();
        log.info("init database... OK");
        
        log.info("clearing old content...");
        SQLGenerator gen = daeDAO.getSQLGenerator();
        DataSource ds = daeDAO.getDataSource();
        for (Class c : new Class[] { DeletedArtifactEvent.class, DeletedStorageLocationEvent.class, StorageLocationEvent.class }) {
            String sql = "delete from " + gen.getTable(c);
            log.info("pre-test cleanup: " + sql);
            ds.getConnection().createStatement().execute(sql);
        }
        log.info("clearing old content... OK");
    }
    
    @Test
    public void testPutGetDeletedArtifactEvent() {
        try {
            DeletedArtifactEvent expected = new DeletedArtifactEvent(UUID.randomUUID());
            log.info("expected: " + expected);
            
            DeletedArtifactEvent notFound = daeDAO.get(expected.getID());
            Assert.assertNull(notFound);
            
            daeDAO.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            // get by ID
            DeletedArtifactEvent fid = (DeletedArtifactEvent) daeDAO.get(expected.getID());
            Assert.assertNotNull(fid);
            
            Assert.assertEquals("lastModified", expected.getLastModified(), fid.getLastModified());
            
            // idempotent put: create new instance with same state
            DeletedArtifactEvent dupe = new DeletedArtifactEvent(expected.getID());
            Thread.sleep(10L);
            daeDAO.put(dupe);
            
            DeletedArtifactEvent fid2 = (DeletedArtifactEvent) daeDAO.get(expected.getID());
            Assert.assertNotNull(fid2);
            // idempotent includes not updating timestamp
            Assert.assertEquals("lastModified", expected.getLastModified(), fid2.getLastModified());
            
            // no delete
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPutDeletedArtifactEvent_LastModifiedUpdate() {
        try {
            DeletedArtifactEvent expected = new DeletedArtifactEvent(UUID.randomUUID());
            log.info("expected: " + expected);
            
            DeletedArtifactEvent notFound = daeDAO.get(expected.getID());
            Assert.assertNull(notFound);
            
            Date now = new Date();
            InventoryUtil.assignLastModified(expected, now);
            URI mcs = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            InventoryUtil.assignMetaChecksum(expected, mcs);
            
            Thread.sleep(10L);
            
            daeDAO.put(expected, true);

            Assert.assertNotNull(expected.getMetaChecksum());
            Assert.assertEquals(mcs, expected.getMetaChecksum()); // no delta
                        
            // get by ID
            DeletedArtifactEvent fid = (DeletedArtifactEvent) daeDAO.get(expected.getID());
            Assert.assertNotNull(fid);
            
            Assert.assertTrue("lastModified", fid.getLastModified().after(now));
            
            // idempotent put: create new instance with same state
            Thread.sleep(10L);
            daeDAO.put(fid, true);
            
            DeletedArtifactEvent fid2 = (DeletedArtifactEvent) daeDAO.get(expected.getID());
            Assert.assertNotNull(fid2);
            Assert.assertEquals("lastModified", fid.getLastModified(), fid2.getLastModified());
            
            // no delete
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPutGetDeletedStorageLocationEvent() {
        try {
            DeletedStorageLocationEvent expected = new DeletedStorageLocationEvent(UUID.randomUUID());
            log.info("expected: " + expected);
            
            DeletedStorageLocationEvent notFound = dslDAO.get(expected.getID());
            Assert.assertNull(notFound);
            
            dslDAO.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            // get by ID
            DeletedStorageLocationEvent fid = dslDAO.get(expected.getID());
            Assert.assertNotNull(fid);

            // idempotent put: create new instance with same state            
            DeletedStorageLocationEvent dupe = new DeletedStorageLocationEvent(expected.getID());
            Thread.sleep(10L);
            dslDAO.put(dupe);
            
            DeletedStorageLocationEvent fid2 = dslDAO.get(expected.getID());
            Assert.assertNotNull(fid2);
            // idempotent includes not updating timestamp
            Assert.assertEquals("lastModified", expected.getLastModified(), fid2.getLastModified());
            
            // no delete
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPutGetStorageLocationEvent() {
        try {
            StorageLocationEvent expected = new StorageLocationEvent(UUID.randomUUID());
            log.info("expected: " + expected);
            
            StorageLocationEvent notFound = slDAO.get(expected.getID());
            Assert.assertNull(notFound);
            
            slDAO.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            // get by ID
            StorageLocationEvent fid = slDAO.get(expected.getID());
            Assert.assertNotNull(fid);

            // idempotent put: create new instance with same state            
            StorageLocationEvent dupe = new StorageLocationEvent(expected.getID());
            Thread.sleep(10L);
            slDAO.put(dupe);
            
            StorageLocationEvent fid2 = slDAO.get(expected.getID());
            Assert.assertNotNull(fid2);
            // idempotent includes not updating timestamp
            Assert.assertEquals("lastModified", expected.getLastModified(), fid2.getLastModified());
            
            // no delete
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCopyConstructorDAE() {
        try {
            DeletedArtifactEvent expected = new DeletedArtifactEvent(UUID.randomUUID());
            log.info("expected: " + expected);
            
            DeletedArtifactEvent notFound = daeDAO.get(expected.getID());
            Assert.assertNull(notFound);
            
            daeDAO.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            // get by ID
            DeletedArtifactEventDAO cp = new DeletedArtifactEventDAO(daeDAO);
            DeletedArtifactEvent fid = (DeletedArtifactEvent) cp.get(DeletedArtifactEvent.class, expected.getID());
            Assert.assertNotNull(fid);
            
            // no delete
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCopyConstructorDSL() {
        try {
            DeletedStorageLocationEvent expected = new DeletedStorageLocationEvent(UUID.randomUUID());
            log.info("expected: " + expected);
            
            DeletedStorageLocationEvent notFound = dslDAO.get(expected.getID());
            Assert.assertNull(notFound);
            
            dslDAO.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            // get by ID
            DeletedStorageLocationEventDAO cp = new DeletedStorageLocationEventDAO(daeDAO);
            DeletedStorageLocationEvent fid = cp.get(expected.getID());
            Assert.assertNotNull(fid);
            
            // no delete
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}

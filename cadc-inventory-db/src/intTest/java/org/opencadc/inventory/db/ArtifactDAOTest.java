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

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.StoredArtifactComparator;
import org.opencadc.inventory.db.version.InitDatabaseSI;

/**
 *
 * @author pdowler
 */
public class ArtifactDAOTest {
    private static final Logger log = Logger.getLogger(ArtifactDAOTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
    }
    
    ArtifactDAO originDAO;
    ArtifactDAO nonOriginDAO;
    ArtifactDAO altDAO = new ArtifactDAO();
    
    public ArtifactDAOTest() throws Exception {
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
            DBUtil.createJNDIDataSource("jdbc/ArtifactDAOTest", cc);

            Map<String,Object> config = new TreeMap<String,Object>();
            config.put(SQLGenerator.class.getName(), SQLGenerator.class);
            config.put("jndiDataSourceName", "jdbc/ArtifactDAOTest");
            config.put("database", TestUtil.DATABASE);
            config.put("invSchema", TestUtil.SCHEMA);
            config.put("genSchema", TestUtil.SCHEMA);

            originDAO = new ArtifactDAO();
            originDAO.setConfig(config);

            nonOriginDAO = new ArtifactDAO(false);
            nonOriginDAO.setConfig(config);

            DBUtil.createJNDIDataSource("jdbc/ArtifactDAOTest-alt", cc);
            Map<String,Object> altConfig = new TreeMap<String,Object>();
            altConfig.put(SQLGenerator.class.getName(), SQLGenerator.class);
            altConfig.put("jndiDataSourceName", "jdbc/ArtifactDAOTest-alt");
            altConfig.put("database", TestUtil.DATABASE);
            altConfig.put("invSchema", TestUtil.SCHEMA);
            altConfig.put("genSchema", TestUtil.SCHEMA);
            altDAO.setConfig(altConfig);
        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw ex;
        }
    }
    
    @Before
    public void init_cleanup() throws Exception {
        log.info("init database...");
        InitDatabaseSI init = new InitDatabaseSI(originDAO.getDataSource(), TestUtil.DATABASE, TestUtil.SCHEMA);
        init.doInit();
        log.info("init database... OK");
        
        log.info("clearing old content...");
        SQLGenerator gen = originDAO.getSQLGenerator();
        DataSource ds = originDAO.getDataSource();
        String sql = "delete from " + gen.getTable(Artifact.class);
        log.info("pre-test cleanup: " + sql);
        ds.getConnection().createStatement().execute(sql);
        log.info("clearing old content... OK");
    }
    
    @Test
    public void testGetPutDelete() {
        long t1 = System.currentTimeMillis();
        try {
            Artifact expected = new Artifact(
                    URI.create("cadc:ARCHIVE/filename"),
                    URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                    new Date(),
                    666L);
            expected.contentType = "application/octet-stream";
            expected.contentEncoding = "gzip";
            log.info("expected: " + expected);
            
            
            Artifact notFound = originDAO.get(expected.getURI());
            Assert.assertNull(notFound);
            
            originDAO.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            // get by ID
            Artifact fid = originDAO.get(expected.getID());
            Assert.assertNotNull(fid);
            Assert.assertEquals(expected.getURI(), fid.getURI());
            Assert.assertEquals(expected.getLastModified(), fid.getLastModified());
            Assert.assertEquals(expected.getMetaChecksum(), fid.getMetaChecksum());
            URI mcs1 = fid.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", expected.getMetaChecksum(), mcs1);
            
            // get by URI
            Artifact furi = originDAO.get(expected.getURI());
            Assert.assertNotNull(furi);
            Assert.assertEquals(expected.getID(), furi.getID());
            Assert.assertEquals(expected.getLastModified(), furi.getLastModified());
            Assert.assertEquals(expected.getMetaChecksum(), furi.getMetaChecksum());
            URI mcs2 = furi.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", expected.getMetaChecksum(), mcs2);
            
            originDAO.delete(expected.getID());
            Artifact deleted = originDAO.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            long t2 = System.currentTimeMillis();
            long dt = t2 - t1;
            log.info("testGetPutDelete: " + dt + "ms");
        }
    }
    
    @Test
    public void testGetWithLock() {
        // to verify locking  and release by getWithLocks
        // - set this to an amount of time in milliseconds so the test sleeps before and after rollback
        // - run the query in sql/pg-locks.sql manually to check for locks
        // note: assumes this test is the only user of the database
        final long sleep = 0L;
        
        TransactionManager txn = originDAO.getTransactionManager();
        
        try {
            Artifact expected = new Artifact(
                    URI.create("cadc:ARCHIVE/filename"),
                    URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                    new Date(),
                    666L);
            expected.contentType = "application/octet-stream";
            expected.contentEncoding = "gzip";
            log.info("  orig: " + expected);
            
            
            Artifact notFound = originDAO.get(expected.getURI());
            Assert.assertNull(notFound);
            
            originDAO.put(expected);
            log.info("   put: " + expected);
            
            txn.startTransaction();
            
            Artifact fid = originDAO.lock(expected);
            Assert.assertNotNull(fid);
            
            log.info("lock(Artifact): " + fid + " -- sleeping for " + sleep);
            Thread.sleep(sleep);
            
            Artifact fid2 = originDAO.lock(expected.getID());
            Assert.assertNotNull(fid2);
            
            log.info("lock(UUID): " + fid2 + " -- sleeping for " + sleep);
            Thread.sleep(sleep);
            
            txn.rollbackTransaction();
            log.info("rollback: -- sleeping for " + sleep);
            Thread.sleep(sleep);
            
            originDAO.delete(expected.getID());
            Artifact deleted = originDAO.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCopyConstructor() {
        try {
            Artifact expected = new Artifact(
                    URI.create("cadc:ARCHIVE/filename"),
                    URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                    new Date(),
                    666L);
            expected.contentType = "application/octet-stream";
            expected.contentEncoding = "gzip";
            log.info("expected: " + expected);
            
            
            Artifact notFound = originDAO.get(expected.getURI());
            Assert.assertNull(notFound);
            
            originDAO.put(expected);
            
            // get by ID
            Artifact fid = originDAO.get(expected.getID());
            Assert.assertNotNull(fid);
            Assert.assertEquals(expected.getURI(), fid.getURI());
            
            ArtifactDAO cp = new ArtifactDAO(originDAO);
            cp.delete(expected.getID());
            
            Artifact deleted = originDAO.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetPutDeleteStorageLocation() {
        try {
            Artifact expected = new Artifact(
                    URI.create("cadc:ARCHIVE/filename"),
                    URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                    new Date(),
                    666L);
            log.info("expected: " + expected);
            
            
            Artifact notFound = originDAO.get(expected.getURI());
            Assert.assertNull(notFound);
            
            originDAO.put(expected);
            Artifact a1 = originDAO.get(expected.getID());
            Assert.assertNotNull(a1);
            Assert.assertNull("no storageLocation", a1.storageLocation);
            Thread.sleep(20L);
            
            StorageLocation loc = new StorageLocation(URI.create("ceph:" + UUID.randomUUID()));
            originDAO.setStorageLocation(expected, loc);
            Artifact a2 = originDAO.get(expected.getID());
            Assert.assertNotNull(a2);
            URI mcs2 = a2.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs2);
            Assert.assertTrue("lastModified not incremented", a1.getLastModified().equals(a2.getLastModified()));
            Assert.assertNotNull("set storageLocation", a2.storageLocation);
            Assert.assertEquals(expected.storageLocation.getStorageID(), a2.storageLocation.getStorageID());
            Assert.assertNull(a2.storageLocation.storageBucket);
            Thread.sleep(20L);
            
            loc = new StorageLocation(URI.create("ceph:"  + UUID.randomUUID()));
            loc.storageBucket = "abc";
            originDAO.setStorageLocation(expected, loc);
            Artifact a3 = originDAO.get(expected.getID());
            Assert.assertNotNull(a3);
            URI mcs3 = a3.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs3);
            Assert.assertTrue("lastModified not incremented", a2.getLastModified().equals(a3.getLastModified()));
            Assert.assertNotNull("update storageLocation", a3.storageLocation);
            Assert.assertEquals(expected.storageLocation.getStorageID(), a3.storageLocation.getStorageID());
            Assert.assertEquals(expected.storageLocation.storageBucket, a3.storageLocation.storageBucket);
            Thread.sleep(20L);
            
            originDAO.setStorageLocation(expected, null);
            Artifact a4 = originDAO.get(expected.getID());
            Assert.assertNotNull(a4);
            URI mcs4 = a4.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs4);
            Assert.assertTrue("lastModified unchanged", a3.getLastModified().equals(a4.getLastModified()));
            Assert.assertNull("clear storageLocation", a4.storageLocation);
            
            originDAO.delete(expected.getID());
            Artifact deleted = originDAO.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetPutDeleteSiteLocations() {
        long t1 = System.currentTimeMillis();
        final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        try {
            Artifact expected = new Artifact(
                    URI.create("cadc:ARCHIVE/filename"),
                    URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                    new Date(),
                    666L);
            log.info("expected: " + expected);
            
            
            Artifact notFound = originDAO.get(expected.getURI());
            Assert.assertNull(notFound);
            
            originDAO.put(expected);
            Thread.sleep(20L);
            
            final SiteLocation loc1 = new SiteLocation(UUID.randomUUID());
            final SiteLocation loc2 = new SiteLocation(UUID.randomUUID());
            final SiteLocation loc3 = new SiteLocation(UUID.randomUUID());
            
            Artifact a1 = originDAO.get(expected.getID());
            Assert.assertNotNull(a1);
            URI mcs1 = a1.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs1);
            Assert.assertTrue("no siteLocations", a1.siteLocations.isEmpty());
            
            log.info("adding " + loc1);
            nonOriginDAO.addSiteLocation(expected, loc1);
            Artifact a2 = nonOriginDAO.get(expected.getID());
            Assert.assertNotNull(a2);
            URI mcs2 = a2.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs2);
            log.info("lastModified: " + df.format(a1.getLastModified()) + " vs " + df.format(a2.getLastModified()));
            Assert.assertTrue("lastModified changed", a1.getLastModified().before(a2.getLastModified()));
            Assert.assertEquals(1, a2.siteLocations.size());
            Thread.sleep(20L);
            
            log.info("adding " + loc2);
            nonOriginDAO.addSiteLocation(a2, loc2);
            log.info("adding " + loc3);
            nonOriginDAO.addSiteLocation(a2, loc3);
            Artifact a3 = nonOriginDAO.get(expected.getID());
            Assert.assertNotNull(a3);
            URI mcs3 = a3.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs3);
            log.info("lastModified: " + df.format(a2.getLastModified()) + " vs " + df.format(a3.getLastModified()));
            Assert.assertTrue("lastModified not changed", a2.getLastModified().equals(a3.getLastModified()));
            Assert.assertEquals(3, a3.siteLocations.size());
            Thread.sleep(20L);
            
            log.info("adding again: " + loc1);
            nonOriginDAO.addSiteLocation(expected, loc1);
            Artifact a4 = nonOriginDAO.get(expected.getID());
            Assert.assertNotNull(a4);
            URI mcs4 = a4.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs4);
            log.info("lastModified: " + a3.getLastModified() + " vs " + a4.getLastModified());
            Assert.assertTrue("lastModified unchanged", a3.getLastModified().equals(a4.getLastModified()));
            Assert.assertEquals(3, a4.siteLocations.size());
            Thread.sleep(20L);
            
            // must remove from the persisted artifact that contains them
            nonOriginDAO.removeSiteLocation(a3, loc3);
            Assert.assertEquals("removed", 2, originDAO.get(expected.getID()).siteLocations.size());
            nonOriginDAO.removeSiteLocation(a3, loc1);
            Assert.assertEquals("removed", 1, originDAO.get(expected.getID()).siteLocations.size());
            Artifact a5 = nonOriginDAO.get(expected.getID());
            Assert.assertNotNull(a5);
            URI mcs5 = a5.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs5);
            Assert.assertTrue("lastModified unchanged", a3.getLastModified().equals(a5.getLastModified()));
            Assert.assertEquals(1, a5.siteLocations.size());
            
            //try {
            nonOriginDAO.removeSiteLocation(a3, loc2);
            //    Assert.fail("remove last SiteLocation - expected IllegalStateException");
            //} catch (IllegalStateException iex) {
            //    log.info("caught expected: " + iex);
            //}
            
            originDAO.delete(expected.getID());
            Artifact deleted = originDAO.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            long t2 = System.currentTimeMillis();
            long dt = t2 - t1;
            log.info("testGetPutDeleteSiteLocations: " + dt + "ms");
        }
    }
    
    @Test
    public void testMetadataSyncSequenceNew() {

        ArtifactDAO dao = nonOriginDAO;
        final TransactionManager transactionManager = dao.getTransactionManager();
        
        
        try {
            Artifact expected = new Artifact(
                    URI.create("cadc:ARCHIVE/filename"),
                    URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                    new Date(),
                    666L);
            expected.contentType = "application/octet-stream";
            log.info("expected: " + expected);
            
            final long t1 = System.currentTimeMillis();
            
            // get by uri
            Artifact notFound = originDAO.get(expected.getURI());
            Assert.assertNull(notFound);
            
            transactionManager.startTransaction();

            Artifact cur = dao.lock(expected);
            Assert.assertNull(cur);
            
            dao.put(expected);
            dao.addSiteLocation(expected, new SiteLocation(UUID.randomUUID()));
            
            transactionManager.commitTransaction();
            
            long t2 = System.currentTimeMillis();
            long dt = t2 - t1;
            log.info("testMetadataSyncSequenceNew: " + dt + "ms");
            
            originDAO.delete(expected.getID());
            Artifact deleted = originDAO.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            if (transactionManager != null && transactionManager.isOpen()) {
                transactionManager.rollbackTransaction();
            }
            
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testMetadataSyncSequenceMerge() {

        ArtifactDAO dao = nonOriginDAO;
        final TransactionManager transactionManager = dao.getTransactionManager();
        
        try {
            Artifact expected = new Artifact(
                    URI.create("cadc:ARCHIVE/filename"),
                    URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                    new Date(),
                    666L);
            expected.contentType = "application/octet-stream";
            log.info("expected: " + expected);
            
            dao.put(expected);
            dao.addSiteLocation(expected, new SiteLocation(UUID.randomUUID()));
            
            long t1 = System.currentTimeMillis();
            
            // get by uri
            Artifact collider = originDAO.get(expected.getURI());
            Assert.assertNotNull(collider);
            // not a collider, so ignore
            
            transactionManager.startTransaction();
            
            Artifact cur = dao.lock(expected);
            Assert.assertNotNull(cur);
            
            expected.siteLocations.addAll(cur.siteLocations);
            dao.put(expected);
            dao.addSiteLocation(expected, new SiteLocation(UUID.randomUUID()));
            
            transactionManager.commitTransaction();
            
            long t2 = System.currentTimeMillis();
            long dt = t2 - t1;
            log.info("testMetadataSyncSequenceMerge: " + dt + "ms");
            
            originDAO.delete(expected.getID());
            Artifact deleted = originDAO.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            if (transactionManager != null && transactionManager.isOpen()) {
                transactionManager.rollbackTransaction();
            }
            
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testEmptyStoredIterator() {
        try {
            for (int i = 0; i < 3; i++) {
                Artifact a = new Artifact(
                        URI.create("cadc:ARCHIVE/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                a.contentType = "application/octet-stream";
                a.contentEncoding = "gzip";
                // no storage location
                originDAO.put(a);
                log.info("expected: " + a);
            }
            
            ResourceIterator<Artifact> iter = originDAO.storedIterator(null);
            Assert.assertFalse("no results", iter.hasNext());
            
            iter.close();
            Assert.assertFalse("no more results", iter.hasNext());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testStoredIterator() {
        int num = 10;
        try {
            int numArtifacts = 0;
            SortedSet<Artifact> eset = new TreeSet<>(new StoredArtifactComparator());
            for (int i = 0; i < num; i++) {
                Artifact a = new Artifact(
                        URI.create("cadc:ARCHIVE/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                a.storageLocation = new StorageLocation(URI.create("foo:" + UUID.randomUUID()));
                a.storageLocation.storageBucket = InventoryUtil.computeBucket(a.storageLocation.getStorageID(), 3);
                originDAO.put(a);
                log.debug("put: " + a);
                eset.add(a);
                numArtifacts++;
            }
            // add some artifacts without the optional bucket to check database vs comparator ordering
            for (int i = num; i < 2 * num; i++) {
                Artifact a = new Artifact(
                        URI.create("cadc:ARCHIVE/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                a.storageLocation = new StorageLocation(URI.create("foo:" + UUID.randomUUID()));
                // no bucket
                originDAO.put(a);
                log.debug("put: " + a);
                eset.add(a);
                numArtifacts++;
            }
            // add some artifacts with no storageLocation
            for (int i = 2 * num; i < 3 * num; i++) {
                Artifact a = new Artifact(
                        URI.create("cadc:ARCHIVE/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                // no storageLocation
                originDAO.put(a);
                log.debug("put: " + a);
                //not in eset
                numArtifacts++;
            }
            log.info("added: " + numArtifacts);
            
            ResourceIterator<Artifact> iter = originDAO.storedIterator(null);
            Iterator<Artifact> ei = eset.iterator();
            Artifact prev = null;
            int count = 0;
            while (ei.hasNext()) {
                Artifact expected = ei.next();
                Artifact actual = iter.next();
                count++;
                
                log.info("compare: " + expected.getURI() + " vs " + actual.getURI() + "\n\t"
                        + expected.storageLocation + " vs " + actual.storageLocation);
                Assert.assertEquals("order", expected.storageLocation, actual.storageLocation);
                Assert.assertEquals(expected.getID(), actual.getID());
                Assert.assertEquals(expected.getURI(), actual.getURI());
                Assert.assertEquals(expected.getLastModified(), actual.getLastModified());
                Assert.assertEquals(expected.getMetaChecksum(), actual.getMetaChecksum());
                URI mcs2 = actual.computeMetaChecksum(MessageDigest.getInstance("MD5"));
                Assert.assertEquals("round trip metachecksum", expected.getMetaChecksum(), mcs2);
                
                // check if iterator is holding a read lock that prevents update
                if (prev != null) {
                    Artifact a = altDAO.get(prev.getID());
                    log.info("alt.get: " + a);
                    a.contentType = "text/plain";
                    altDAO.put(a);
                    log.info("log.put: " + a + " OK");
                }
                prev = actual;
                
                // one time, part way through: pause so developer can examine database locks held
                //if (count == numArtifacts / 4) {
                //    log.warn("count == 100: sleeping for 30 sec...");
                //    Thread.sleep(30000L);
                //    log.warn("count == 100: sleeping for 30 sec... DONE");
                //}
            }
            Assert.assertFalse("database iterator exhausted", iter.hasNext());
            Assert.assertEquals("count", eset.size(), count);
            
            iter.close();
            Assert.assertFalse("no more results", iter.hasNext());
            
            int found = 0;
            for (byte b = 0; b < 16; b++) {
                String hex =  HexUtil.toHex(b);
                String bp = hex.substring(1);
                ResourceIterator<Artifact> i = originDAO.storedIterator(bp);
                while (i.hasNext()) {
                    Artifact a = i.next();
                    log.info("found: " + bp + " contains " + a.storageLocation);
                    Assert.assertNotNull(a.storageLocation);
                    Assert.assertNotNull(a.storageLocation.storageBucket);
                    Assert.assertTrue("prefix match", a.storageLocation.storageBucket.startsWith(bp));
                    found++;
                }
            }
            Assert.assertEquals("found with bucketPrefix", num, found); // num is number with a bucket
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testEmptyUnstoredIterator() {
        try {
            for (int i = 0; i < 3; i++) {
                Artifact a = new Artifact(
                        URI.create("cadc:ARCHIVE/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                a.contentType = "application/octet-stream";
                a.contentEncoding = "gzip";
                a.storageLocation = new StorageLocation(URI.create("foo:" + UUID.randomUUID()));
                originDAO.put(a);
                log.info("expected: " + a);
            }
            
            ResourceIterator<Artifact> iter = originDAO.unstoredIterator(null);
            Assert.assertFalse("no results", iter.hasNext());
            
            iter.close();
            Assert.assertFalse("no more results", iter.hasNext());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testUnstoredIterator() {
        int num = 10;
        try {
            int numArtifacts = 0;
            SortedSet<Artifact> eset = new TreeSet<>(new LastModifiedComparator());
            for (int i = 0; i < num; i++) {
                Artifact a = new Artifact(
                        URI.create("cadc:ARCHIVE/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                a.storageLocation = new StorageLocation(URI.create("foo:" + UUID.randomUUID()));
                a.storageLocation.storageBucket = InventoryUtil.computeBucket(a.storageLocation.getStorageID(), 3);
                originDAO.put(a);
                log.info("expected: " + a);
                // not in eset
                Thread.sleep(1L);
                numArtifacts++;
            }
            // add some artifacts without the optional bucket to check database vs comparator ordering
            for (int i = num; i < 2 * num; i++) {
                Artifact a = new Artifact(
                        URI.create("cadc:ARCHIVE/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                a.storageLocation = new StorageLocation(URI.create("foo:" + UUID.randomUUID()));
                // no bucket
                originDAO.put(a);
                log.info("expected: " + a);
                // not in eset
                Thread.sleep(1L);
                numArtifacts++;
            }
            // add some artifacts with no storageLocation
            for (int i = 2 * num; i < 3 * num; i++) {
                Artifact a = new Artifact(
                        URI.create("cadc:ARCHIVE/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                // no storageLocation
                originDAO.put(a);
                log.info("expected: " + a);
                eset.add(a);
                Thread.sleep(1L);
                numArtifacts++;
            }
            log.info("added: " + numArtifacts);
            
            ResourceIterator<Artifact> iter = originDAO.unstoredIterator(null);
            Iterator<Artifact> ei = eset.iterator();
            int count = 0;
            while (ei.hasNext()) {
                Artifact expected = ei.next();
                Artifact actual = iter.next();
                log.info("compare: " + expected.getURI() + " vs " + actual.getURI() + "\n\t"
                        + expected.storageLocation + " vs " + actual.storageLocation);
                Assert.assertEquals("order", expected.getLastModified(), actual.getLastModified());
                count++;
                Assert.assertEquals(expected.getID(), actual.getID());
                Assert.assertEquals(expected.getURI(), actual.getURI());
                Assert.assertEquals(expected.getLastModified(), actual.getLastModified());
                Assert.assertEquals(expected.getMetaChecksum(), actual.getMetaChecksum());
                URI mcs2 = actual.computeMetaChecksum(MessageDigest.getInstance("MD5"));
                Assert.assertEquals("round trip metachecksum", expected.getMetaChecksum(), mcs2);
            }
            Assert.assertEquals("count", eset.size(), count);
            
            iter.close();
            Assert.assertFalse("no more results", iter.hasNext());
            
            // check uriBucket prefix usage
            int found = 0;
            for (byte b = 0; b < 16; b++) {
                String urib = HexUtil.toHex(b).substring(1);
                log.info("bucket prefix: " + urib);
                ResourceIterator<Artifact> i = originDAO.unstoredIterator(urib);
                while (i.hasNext()) {
                    Artifact a = i.next();
                    log.info("found: " + urib + " contains " + a.getBucket());
                    Assert.assertTrue("prefix match", a.getBucket().startsWith(urib));
                    found++;
                }
            }
            Assert.assertFalse("database iterator exhausted", iter.hasNext());
            Assert.assertEquals("found with bucketPrefix", num, found); // num unstored
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testIteratorDelete() {
        int num = 10;
        long sleep = 20000L;
        try {
            log.info("testIteratorDelete: START");
            int numArtifacts = 0;
            SortedSet<Artifact> eset = new TreeSet<>(new LastModifiedComparator());
            for (int i = 0; i < num; i++) {
                Artifact a = new Artifact(
                        URI.create("cadc:ARCHIVE/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                originDAO.put(a);
                log.info("expected: " + a);
                Thread.sleep(1L);
                numArtifacts++;
            }
            
            log.info("created: " + numArtifacts);
            ResourceIterator<Artifact> iter = originDAO.unstoredIterator(null);
            
            int count = 0;
            while (iter.hasNext()) {
                Artifact actual = iter.next();
                originDAO.delete(actual.getID());
                log.info("deleted: " + actual.getID() + " aka " + actual.getURI());
                count++;
                
                // one time, part way through: pause so developer can examine database locks held
                //if (count == numArtifacts / 2) {
                //    log.warn("iter/delete: sleeping for " + sleep + " ms...");
                //    Thread.sleep(sleep);
                //}
            }
            
            iter.close();
            Assert.assertFalse("no more results", iter.hasNext());
            
            iter = originDAO.unstoredIterator(null);
            Assert.assertFalse("all deleted", iter.hasNext());
            log.info("testIteratorDelete: OK");
            
            iter.close();
            Assert.assertFalse("no more results", iter.hasNext());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }        
    
    @Test
    public void testIteratorClose() {
        // to verify locking  and release of locks by ArtifactIterator.close():
        // - set this to an amount of time in milliseconds so the test sleeps before and after close
        // - run the query in sql/pg-locks.sql manually to check for locks
        // note: assumes this test is the only user of the database
        final long sleep = 0L;
        
        int num = 10;
        int numArtifacts = 0;
        try {
            for (int i = 2 * num; i < 3 * num; i++) {
                Artifact a = new Artifact(
                        URI.create("cadc:ARCHIVE/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                // no storageLocation
                originDAO.put(a);
                log.info("expected: " + a);
                numArtifacts++;
            }
            log.info("added: " + numArtifacts);
            
            ResourceIterator<Artifact> iter = originDAO.unstoredIterator(null);
            int count = 0;
            while (iter.hasNext()) {
                Artifact actual = iter.next();
                originDAO.delete(actual.getID()); // delete to create locks in txn
                log.info("deleted: " + actual.getURI() + " aka " + actual.getID());
                count++;
                if (count == numArtifacts / 2) {
                    break;
                }
            }
            Assert.assertTrue("more results", iter.hasNext());
            
            if (sleep > 0L) {
                log.info("testIteratorClose: before close() -- should be locks in DB (sleep: " + sleep + ")");
                Thread.sleep(sleep);
                log.info("testIteratorClose: sleep DONE");
            }
            
            iter.close();
            Assert.assertFalse("no more results", iter.hasNext());
            
            if (sleep > 0L) {
                log.info("testIteratorClose: after close() -- should be *NO* locks in DB (sleep: " + sleep + ")");
                Thread.sleep(sleep);
                log.info("testIteratorClose: sleep DONE");
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testArtifactIterator() {
        int num = 10;
        try {
            int numArtifacts = 0;
            int numStuffExpected = 0;
            // artifacts with storageLocation
            String collection = "STUFF";
            for (int i = 0; i < num; i++) {
                if (i == num / 2) {
                    collection = "NONSENSE";
                }
                Artifact a = new Artifact(
                        URI.create("cadc:" + collection + "/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                a.storageLocation = new StorageLocation(URI.create("foo:" + UUID.randomUUID()));
                a.storageLocation.storageBucket = InventoryUtil.computeBucket(a.storageLocation.getStorageID(), 3);
                originDAO.put(a);
                log.debug("put: " + a);
                numArtifacts++;
                if (collection.equals("STUFF")) {
                    numStuffExpected++;
                }
            }
            // some artifacts with no storageLocation
            collection = "STUFF";
            for (int i = num; i < 2 * num; i++) {
                if (i == num + num / 2) {
                    collection = "NONSENSE";
                }
                Artifact a = new Artifact(
                        URI.create("cadc:" + collection + "/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                // no storageLocation
                originDAO.put(a);
                log.debug("put: " + a);
                numArtifacts++;
                if (collection.equals("STUFF")) {
                    numStuffExpected++;
                }
            }
            // some artifacts with siteLocations
            UUID siteID = UUID.randomUUID();
            int numSiteExpected = 0;
            collection = "STUFF";
            for (int i = 2 * num; i < 3 * num; i++) {
                if (i == num + num / 2) {
                    collection = "NONSENSE";
                }
                Artifact a = new Artifact(
                        URI.create("cadc:" + collection + "/filename" + i),
                        URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                        new Date(),
                        666L);
                a.siteLocations.add(new SiteLocation(siteID));
                originDAO.put(a);
                log.debug("put: " + a);
                numArtifacts++;
                numSiteExpected++;
                if (collection.equals("STUFF")) {
                    numStuffExpected++;
                }
            }
            log.info("added: " + numArtifacts);
            
            log.info("count all...");
            int count = 0;
            try (ResourceIterator<Artifact> iter = originDAO.iterator(null, false)) {
                while (iter.hasNext()) {
                    Artifact actual = iter.next();
                    count++;
                    log.info("found: " + actual.getURI());
                }
            }
            Assert.assertEquals("count", numArtifacts, count);
            
            log.info("count with criteria...");
            count = 0;
            try (ResourceIterator<Artifact> iter = originDAO.iterator("uri like 'cadc:STUFF/%'", null, false)) {
                while (iter.hasNext()) {
                    Artifact actual = iter.next();
                    count++;
                    log.info("found: " + actual.getURI());
                    Assert.assertTrue("STUFF", actual.getURI().toASCIIString().startsWith("cadc:STUFF/"));
                }
            }
            Assert.assertEquals("count", numStuffExpected, count);
            
            log.info("count vs siteID...");
            count = 0;
            try (ResourceIterator<Artifact> iter = originDAO.iterator(siteID, null, false)) {
                while (iter.hasNext()) {
                    Artifact actual = iter.next();
                    count++;
                    log.info("found: " + actual.getURI());
                    Assert.assertFalse("siteID", actual.siteLocations.isEmpty());
                    Assert.assertEquals("siteID", siteID, actual.siteLocations.iterator().next().getSiteID());
                }
            }
            Assert.assertEquals("count", numSiteExpected, count);
            
            log.info("count in buckets...");
            count = 0;
            for (byte b = 0; b < 16; b++) {
                String bpre = HexUtil.toHex(b).substring(1);
                log.debug("bucket prefix: " + bpre);
                try (ResourceIterator<Artifact> iter = originDAO.iterator(bpre, false)) {
                    while (iter.hasNext()) {
                        Artifact actual = iter.next();
                        count++;
                        log.info("found: " + actual.getURI());
                    }
                }
            }
            Assert.assertEquals("count", numArtifacts, count);
            
            log.info("count with criteria + in buckets...");
            count = 0;
            for (byte b = 0; b < 16; b++) {
                String bpre = HexUtil.toHex(b).substring(1);
                log.debug("bucket prefix: " + bpre);
                try (ResourceIterator<Artifact> iter = originDAO.iterator("uri like 'cadc:STUFF/%'", bpre, false)) {
                    while (iter.hasNext()) {
                        Artifact actual = iter.next();
                        count++;
                        log.info("found: " + actual.getURI());
                        Assert.assertTrue("STUFF", actual.getURI().toASCIIString().startsWith("cadc:STUFF/"));
                    }
                }
            }
            Assert.assertEquals("count", numStuffExpected, count);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    private class LastModifiedComparator implements Comparator<Artifact> {

        @Override
        public int compare(Artifact lhs, Artifact rhs) {
            return lhs.getLastModified().compareTo(rhs.getLastModified());
        }
    }
}

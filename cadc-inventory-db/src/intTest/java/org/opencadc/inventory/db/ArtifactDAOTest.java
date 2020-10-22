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

package org.opencadc.inventory.db;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;

import java.net.URI;
import java.security.MessageDigest;
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
import org.opencadc.inventory.db.version.InitDatabase;

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
            config.put("schema", TestUtil.SCHEMA);
            
            originDAO = new ArtifactDAO();
            originDAO.setConfig(config);
            
            nonOriginDAO = new ArtifactDAO(false);
            nonOriginDAO.setConfig(config);
            
            DBUtil.createJNDIDataSource("jdbc/ArtifactDAOTest-alt", cc);
            Map<String,Object> altConfig = new TreeMap<String,Object>();
            altConfig.put(SQLGenerator.class.getName(), SQLGenerator.class);
            altConfig.put("jndiDataSourceName", "jdbc/ArtifactDAOTest-alt");
            altConfig.put("database", TestUtil.DATABASE);
            altConfig.put("schema", TestUtil.SCHEMA);
            altDAO.setConfig(altConfig);
            
        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw ex;
        }
    }
    
    @Before
    public void init_cleanup() throws Exception {
        log.info("init database...");
        InitDatabase init = new InitDatabase(originDAO.getDataSource(), TestUtil.DATABASE, TestUtil.SCHEMA);
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
        try {
            Artifact expected = new Artifact(
                    URI.create("cadc:ARCHIVE/filename"),
                    URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                    new Date(),
                    new Long(666L));
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
        }
    }
    
    @Test
    public void testCopyConstructor() {
        try {
            Artifact expected = new Artifact(
                    URI.create("cadc:ARCHIVE/filename"),
                    URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                    new Date(),
                    new Long(666L));
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
                    new Long(666L));
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
            Assert.assertTrue("lastModified incremented", a1.getLastModified().before(a2.getLastModified()));
            Assert.assertNotNull("set storageLocation", a2.storageLocation);
            Assert.assertEquals(expected.storageLocation.getStorageID(), a2.storageLocation.getStorageID());
            Assert.assertNull(a2.storageLocation.storageBucket);
            Thread.sleep(20L);
            
            loc = new StorageLocation(URI.create("ceph:"  + UUID.randomUUID()));
            loc.storageBucket = "abc";
            originDAO.setStorageLocation(expected, loc);
            Artifact a3 = originDAO.get(expected.getID());
            Assert.assertNotNull(a2);
            URI mcs3 = a3.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs3);
            Assert.assertTrue(a2.getLastModified().before(a3.getLastModified()));
            Assert.assertNotNull("update storageLocation", a3.storageLocation);
            Assert.assertEquals(expected.storageLocation.getStorageID(), a3.storageLocation.getStorageID());
            Assert.assertEquals(expected.storageLocation.storageBucket, a3.storageLocation.storageBucket);
            Thread.sleep(20L);
            
            originDAO.setStorageLocation(expected, null);
            Artifact a4 = originDAO.get(expected.getID());
            Assert.assertNotNull(a2);
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
        try {
            Artifact expected = new Artifact(
                    URI.create("cadc:ARCHIVE/filename"),
                    URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                    new Date(),
                    new Long(666L));
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
            
            nonOriginDAO.addSiteLocation(expected, loc1);
            Artifact a2 = nonOriginDAO.get(expected.getID());
            Assert.assertNotNull(a2);
            URI mcs2 = a2.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs2);
            Assert.assertTrue("lastModified unchanged", a1.getLastModified().equals(a2.getLastModified()));
            Assert.assertEquals(1, a2.siteLocations.size());
            Thread.sleep(20L);
            
            nonOriginDAO.addSiteLocation(expected, loc2);
            nonOriginDAO.addSiteLocation(expected, loc3);
            Artifact a3 = nonOriginDAO.get(expected.getID());
            Assert.assertNotNull(a3);
            URI mcs3 = a3.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs3);
            Assert.assertTrue("lastModified unchanged", a1.getLastModified().equals(a3.getLastModified()));
            Assert.assertEquals(3, a3.siteLocations.size());
            Thread.sleep(20L);
            
            // must remove from the persisted artifact that contains them
            nonOriginDAO.removeSiteLocation(a3, loc3);
            Assert.assertEquals("removed", 2, originDAO.get(expected.getID()).siteLocations.size());
            nonOriginDAO.removeSiteLocation(a3, loc1);
            Assert.assertEquals("removed", 1, originDAO.get(expected.getID()).siteLocations.size());
            nonOriginDAO.removeSiteLocation(a3, loc2);
            Artifact a4 = nonOriginDAO.get(expected.getID());
            Assert.assertNotNull(a1);
            URI mcs4 = a4.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs3);
            Assert.assertTrue(a1.getLastModified().equals(a4.getLastModified()));
            
            originDAO.delete(expected.getID());
            Artifact deleted = originDAO.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
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
                        new Long(666L));
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
                        new Long(666L));
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
                        new Long(666L));
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
                        new Long(666L));
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
                        new Long(666L));
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
                        new Long(666L));
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
                        new Long(666L));
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
                        new Long(666L));
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
                        new Long(666L));
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
                        new Long(666L));
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
    
    private class LastModifiedComparator implements Comparator<Artifact> {

        @Override
        public int compare(Artifact lhs, Artifact rhs) {
            return lhs.getLastModified().compareTo(rhs.getLastModified());
        }
    }
}

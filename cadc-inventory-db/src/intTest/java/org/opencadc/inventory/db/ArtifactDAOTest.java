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
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.version.InitDatabase;

/**
 *
 * @author pdowler
 */
public class ArtifactDAOTest {
    private static final Logger log = Logger.getLogger(ArtifactDAOTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.DEBUG);
    }
    
    ArtifactDAO dao = new ArtifactDAO();
    
    public ArtifactDAOTest() throws Exception {
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
        DBUtil.createJNDIDataSource("jdbc/ArtifactDAOTest", cc);
        
        Map<String,Object> config = new TreeMap<String,Object>();
        config.put(SQLGenerator.class.getName(), SQLGenerator.class);
        config.put("jndiDataSourceName", "jdbc/ArtifactDAOTest");
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
            
            
            Artifact notFound = dao.get(expected.getURI());
            Assert.assertNull(notFound);
            
            dao.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            // get by ID
            Artifact fid = dao.get(expected.getID());
            Assert.assertNotNull(fid);
            Assert.assertEquals(expected.getURI(), fid.getURI());
            Assert.assertEquals(expected.getLastModified(), fid.getLastModified());
            Assert.assertEquals(expected.getMetaChecksum(), fid.getMetaChecksum());
            URI mcs1 = fid.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", expected.getMetaChecksum(), mcs1);
            
            // get by URI
            Artifact furi = dao.get(expected.getURI());
            Assert.assertNotNull(furi);
            Assert.assertEquals(expected.getID(), furi.getID());
            Assert.assertEquals(expected.getLastModified(), furi.getLastModified());
            Assert.assertEquals(expected.getMetaChecksum(), furi.getMetaChecksum());
            URI mcs2 = furi.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", expected.getMetaChecksum(), mcs2);
            
            dao.delete(expected.getID());
            Artifact deleted = dao.get(expected.getID());
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
            expected.contentType = "application/octet-stream";
            expected.contentEncoding = "gzip";
            log.info("expected: " + expected);
            
            
            Artifact notFound = dao.get(expected.getURI());
            Assert.assertNull(notFound);
            
            dao.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            expected.storageLocation = new StorageLocation(URI.create("ceph:" + UUID.randomUUID()));
            
            dao.put(expected);
            
            Artifact a1 = dao.get(expected.getID());
            Assert.assertNotNull(a1);
            URI mcs1 = a1.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs1);
            Assert.assertEquals(expected.getLastModified(), a1.getLastModified());
            Assert.assertNull("did-not-force-update", a1.storageLocation);
            
            dao.put(expected, true);
            Artifact a2 = dao.get(expected.getID());
            Assert.assertNotNull(a2);
            URI mcs2 = a2.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs2);
            Assert.assertEquals(expected.getLastModified(), a2.getLastModified());
            Assert.assertNotNull("force-update", a2.storageLocation);
            Assert.assertEquals(expected.storageLocation.getStorageID(), a2.storageLocation.getStorageID());
            
            expected.storageLocation = new StorageLocation(URI.create("ceph:"  + UUID.randomUUID()));
            dao.put(expected, true);
            Artifact a3 = dao.get(expected.getID());
            Assert.assertNotNull(a2);
            URI mcs3 = a3.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs3);
            Assert.assertEquals(expected.getLastModified(), a3.getLastModified());
            Assert.assertNotNull("updated", a3.storageLocation);
            Assert.assertEquals(expected.storageLocation.getStorageID(), a3.storageLocation.getStorageID());
            
            expected.storageLocation = null;
            dao.put(expected, true);
            Artifact a4 = dao.get(expected.getID());
            Assert.assertNotNull(a2);
            URI mcs4 = a4.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs4);
            Assert.assertEquals(expected.getLastModified(), a4.getLastModified());
            Assert.assertNull("cleared", a4.storageLocation);
            
            dao.delete(expected.getID());
            Artifact deleted = dao.get(expected.getID());
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
            expected.contentType = "application/octet-stream";
            expected.contentEncoding = "gzip";
            log.info("expected: " + expected);
            
            
            Artifact notFound = dao.get(expected.getURI());
            Assert.assertNull(notFound);
            
            dao.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            UUID s1 = UUID.randomUUID();
            UUID s2 = UUID.randomUUID();
            UUID s3 = UUID.randomUUID();
            expected.siteLocations.add(new SiteLocation(s1));
            expected.siteLocations.add(new SiteLocation(s2));
            expected.siteLocations.add(new SiteLocation(s3));
            
            dao.put(expected);
            
            Artifact a1 = dao.get(expected.getID());
            Assert.assertNotNull(a1);
            URI mcs1 = a1.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs1);
            Assert.assertEquals(expected.getLastModified(), a1.getLastModified());
            Assert.assertTrue("did-not-force-update", a1.siteLocations.isEmpty());
            
            dao.put(expected, true);
            Artifact a2 = dao.get(expected.getID());
            Assert.assertNotNull(a2);
            URI mcs2 = a2.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs2);
            Assert.assertEquals(expected.getLastModified(), a2.getLastModified());
            Assert.assertFalse("force-update", a2.siteLocations.isEmpty());
            Assert.assertEquals(3, a2.siteLocations.size());
            
            expected.siteLocations.clear();
            dao.put(expected, true);
            Artifact a3 = dao.get(expected.getID());
            Assert.assertNotNull(a1);
            URI mcs3 = a3.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum unchanged", expected.getMetaChecksum(), mcs3);
            Assert.assertEquals(expected.getLastModified(), a3.getLastModified());
            Assert.assertTrue("cleaned", a3.siteLocations.isEmpty());
            
            dao.delete(expected.getID());
            Artifact deleted = dao.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}

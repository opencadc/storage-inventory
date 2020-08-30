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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.version.InitDatabase;

/**
 *
 * @author pdowler
 */
public class StorageSiteDAOTest {
    private static final Logger log = Logger.getLogger(StorageSiteDAOTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.DEBUG);
    }
    
    StorageSiteDAO dao = new StorageSiteDAO();
    
    public StorageSiteDAOTest() throws Exception {
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
        DBUtil.createJNDIDataSource("jdbc/StorageSiteDAOTest", cc);
        
        Map<String,Object> config = new TreeMap<String,Object>();
        config.put(SQLGenerator.class.getName(), SQLGenerator.class);
        config.put("jndiDataSourceName", "jdbc/StorageSiteDAOTest");
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
        String sql = "delete from " + gen.getTable(StorageSite.class);
        log.info("pre-test cleanup: " + sql);
        ds.getConnection().createStatement().execute(sql);
        log.info("clearing old content... OK");
    }
    
    @Test
    public void testPutGetUpdateDelete() {
        try {
            StorageSite expected = new StorageSite(URI.create("ivo://cadc.nrc.ca/site1"), "Site-1", true, false);
            log.info("expected: " + expected);
            
            StorageSite notFound = dao.get(expected.getID());
            Assert.assertNull(notFound);
            
            dao.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            // get by ID
            StorageSite fid = dao.get(expected.getID());
            Assert.assertNotNull(fid);
            Assert.assertEquals(expected.getResourceID(), fid.getResourceID());
            Assert.assertEquals(expected.getName(), fid.getName());
            Assert.assertEquals(expected.getAllowRead(), fid.getAllowRead());
            Assert.assertEquals(expected.getAllowWrite(), fid.getAllowWrite());
            URI mcs1 = fid.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", fid.getMetaChecksum(), mcs1);
            
            // update
            fid.setResourceID(URI.create("ivo://cadc.nrc.ca/area51"));
            fid.setName("Area 51");
            fid.setAllowRead(false);
            fid.setAllowWrite(true);
            dao.put(fid);
            
            StorageSite a51 = dao.get(expected.getID());
            Assert.assertNotNull(fid);
            Assert.assertEquals(fid.getResourceID(), a51.getResourceID());
            Assert.assertEquals(fid.getName(), a51.getName());
            Assert.assertEquals(fid.getAllowRead(), a51.getAllowRead());
            Assert.assertEquals(fid.getAllowWrite(), a51.getAllowWrite());
            URI mcs2 = a51.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", a51.getMetaChecksum(), mcs2);
            
            // delete
            dao.delete(expected.getID());
            StorageSite deleted = dao.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCopyConstructor() {
        try {
            StorageSite expected = new StorageSite(URI.create("ivo://cadc.nrc.ca/site1"), "Site-1", false, false);
            log.info("expected: " + expected);
            
            StorageSite notFound = dao.get(expected.getID());
            Assert.assertNull(notFound);
            
            dao.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            // get by ID
            StorageSite fid = dao.get(expected.getID());
            Assert.assertNotNull(fid);
            Assert.assertEquals(expected.getResourceID(), fid.getResourceID());
            Assert.assertEquals(expected.getName(), fid.getName());
            URI mcs1 = fid.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", expected.getMetaChecksum(), mcs1);
            
            StorageSiteDAO cp = new StorageSiteDAO(dao);
            cp.delete(expected.getID());
            
            StorageSite deleted = dao.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testList() {
        try {
            StorageSite s1 = new StorageSite(URI.create("ivo://cadc.nrc.ca/site1"), "Site-1", false, false);
            StorageSite s2 = new StorageSite(URI.create("ivo://cadc.nrc.ca/site2"), "Site-2", true, false);
            StorageSite s3 = new StorageSite(URI.create("ivo://cadc.nrc.ca/site3"), "Site-3", false, true);
            StorageSite s4 = new StorageSite(URI.create("ivo://cadc.nrc.ca/site4"), "Site-4", true, true);
            
            Set<StorageSite> sites = dao.list();
            Assert.assertNotNull(sites);
            Assert.assertTrue("empty", sites.isEmpty());
            
            dao.put(s1);
            dao.put(s2);
            dao.put(s3);
            dao.put(s4);
             
            sites = dao.list();
            Assert.assertNotNull(sites);
            Assert.assertTrue("not-empty", !sites.isEmpty());
            Assert.assertEquals(4, sites.size());
            
            StorageSite as1 = InventoryUtil.findSite(s1.getID(), sites);
            Assert.assertNotNull(as1);
            StorageSite as2 = InventoryUtil.findSite(s2.getID(), sites);
            Assert.assertNotNull(as2);
            StorageSite as3 = InventoryUtil.findSite(s3.getID(), sites);
            Assert.assertNotNull(as3);
            StorageSite as4 = InventoryUtil.findSite(s4.getID(), sites);
            Assert.assertNotNull(as4);
            
            as1 = InventoryUtil.findSite(s1.getResourceID(), sites);
            Assert.assertNotNull(as1);
            as2 = InventoryUtil.findSite(s2.getResourceID(), sites);
            Assert.assertNotNull(as2);
            as3 = InventoryUtil.findSite(s3.getResourceID(), sites);
            Assert.assertNotNull(as3);
            as4 = InventoryUtil.findSite(s4.getResourceID(), sites);
            Assert.assertNotNull(as4);
            
            dao.delete(s1.getID());
            dao.delete(s2.getID());
            dao.delete(s3.getID());
            dao.delete(s4.getID());
            sites = dao.list();
            Assert.assertNotNull(sites);
            Assert.assertTrue("deleted", sites.isEmpty());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}

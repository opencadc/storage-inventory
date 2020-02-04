/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020                            (c) 2020.
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
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.version.InitDatabase;

/**
 *
 * @author pdowler
 */
public class ObsoleteStorageLocationDAOTest {
    private static final Logger log = Logger.getLogger(ObsoleteStorageLocationDAOTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.DEBUG);
    }
    
    ObsoleteStorageLocationDAO dao = new ObsoleteStorageLocationDAO();
    
    public ObsoleteStorageLocationDAOTest() throws Exception {
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
            DBUtil.createJNDIDataSource("jdbc/ArtifactDAOTest", cc);

            Map<String,Object> config = new TreeMap<String,Object>();
            config.put(SQLGenerator.class.getName(), SQLGenerator.class);
            config.put("jndiDataSourceName", "jdbc/ArtifactDAOTest");
            config.put("database", TestUtil.DATABASE);
            config.put("schema", TestUtil.SCHEMA);
            dao.setConfig(config);
        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw ex;
        }
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
        String sql = "delete from " + gen.getTable(ObsoleteStorageLocation.class);
        log.info("pre-test cleanup: " + sql);
        ds.getConnection().createStatement().execute(sql);
        log.info("clearing old content... OK");
    }
    
    @Test
    public void testGetPutDelete() {
        try {
            ObsoleteStorageLocation expected = new ObsoleteStorageLocation(
                new StorageLocation(URI.create("uuid:" + UUID.randomUUID()))
            );
            log.info("expected: " + expected);
            
            
            ObsoleteStorageLocation notFound = dao.get(ObsoleteStorageLocation.class, expected.getID());
            Assert.assertNull(notFound);
            
            dao.put(expected);
            
            // persistence assigns entity state before put
            Assert.assertNotNull(expected.getLastModified());
            Assert.assertNotNull(expected.getMetaChecksum());
            
            URI mcs0 = expected.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("put metachecksum", mcs0, expected.getMetaChecksum());
            
            // get by ID
            ObsoleteStorageLocation fid = dao.get(expected.getID());
            Assert.assertNotNull("find by uuid", fid);
            Assert.assertEquals(expected.getLastModified(), fid.getLastModified());
            Assert.assertEquals(expected.getMetaChecksum(), fid.getMetaChecksum());
            URI mcs1 = fid.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("round trip metachecksum", expected.getMetaChecksum(), mcs1);
            
            dao.delete(expected.getID());
            ObsoleteStorageLocation deleted = dao.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetByLocation() {
        try {
            ObsoleteStorageLocation expected = new ObsoleteStorageLocation(
                new StorageLocation(URI.create("uuid:" + UUID.randomUUID()))
            );
            log.info("expected: " + expected);
            
            dao.put(expected);
            
            // get by ID
            ObsoleteStorageLocation fid = dao.get(expected.getID());
            Assert.assertNotNull("find by uuid", fid);
            
            // get by location
            StorageLocation loc = new StorageLocation(expected.getLocation().getStorageID());
            loc.storageBucket = expected.getLocation().storageBucket;
            ObsoleteStorageLocation gloc = dao.get(expected.getLocation());
            Assert.assertNotNull("find by location", gloc);
            Assert.assertEquals(expected.getLocation(), gloc.getLocation());
            
            dao.delete(expected.getID());
            ObsoleteStorageLocation deleted = dao.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetByLocationFull() {
        try {
            StorageLocation loc = new StorageLocation(URI.create("uuid:" + UUID.randomUUID()));
            loc.storageBucket = "flibble";
            
            ObsoleteStorageLocation expected = new ObsoleteStorageLocation(loc);
            log.info("expected: " + expected);
            
            dao.put(expected);
            
            // get by location
            StorageLocation arg = new StorageLocation(expected.getLocation().getStorageID());
            arg.storageBucket = expected.getLocation().storageBucket;
            
            ObsoleteStorageLocation gloc = dao.get(arg);
            Assert.assertNotNull("find by location", gloc);
            Assert.assertEquals(expected.getLocation(), gloc.getLocation());
            
            dao.delete(expected.getID());
            ObsoleteStorageLocation deleted = dao.get(expected.getID());
            Assert.assertNull(deleted);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}

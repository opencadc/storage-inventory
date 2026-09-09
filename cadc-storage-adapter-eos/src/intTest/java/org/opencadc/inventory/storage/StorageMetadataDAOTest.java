/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2026.                            (c) 2026.
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

package org.opencadc.inventory.storage;

import org.opencadc.inventory.storage.StorageMetadataDAO;
import org.opencadc.inventory.storage.InitDatabaseSM;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 *
 * @author pdowler
 */
public class StorageMetadataDAOTest {
    private static final Logger log = Logger.getLogger(StorageMetadataDAOTest.class);
    
    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
    }

    StorageMetadataDAO dao = new StorageMetadataDAO();

    public StorageMetadataDAOTest() throws Exception {
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig("INVENTORY_TEST", "cadctest");
            DBUtil.createJNDIDataSource("jdbc/StorageMetadataDAOTest", cc);

            Map<String,Object> config = new TreeMap<>();
            //config.put(SQLGenerator.class.getName(), SQLGenerator.class);
            config.put("jndiDataSourceName", "jdbc/StorageMetadataDAOTest");
            config.put("schema", "eos"); // hard coded for now
            dao.setConfig(config);
        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw ex;
        }
    }

    @Before
    public void setup() throws Exception {
        log.info("init database...");
        InitDatabaseSM init = new InitDatabaseSM(dao.getDataSource(), "eos");
        init.doInit();
        log.info("init database... OK");
        
        log.info("clearing old content...");
        DataSource ds = dao.getDataSource();
        String sql = "delete from eos.StorageMetadata";
        log.info("pre-test cleanup: " + sql);
        ds.getConnection().createStatement().execute(sql);
        log.info("clearing old content... OK");
    }

    @Test
    public void testNoop() {
        
    }

    @Test
    public void testPutGetDelete() {
        try {
            String relPath = "collection/path/file1";
            StorageLocation sloc = new StorageLocation(URI.create(relPath));
            sloc.storageBucket = InventoryUtil.computeBucket(sloc.getStorageID(), 3);

            StorageMetadata expected = new StorageMetadata(sloc, 
                    URI.create("eos:" + relPath),
                    URI.create("md5:aea82f3d7d786a6db8dc811b4b16e957"),
                    123456L,
                    new Date());

            dao.put(expected);

            StorageMetadata actual = dao.get(sloc);
            Assert.assertNotNull(actual);
            Assert.assertEquals(expected.getStorageLocation(), actual.getStorageLocation());
            Assert.assertEquals(expected.getArtifactURI(), actual.getArtifactURI());
            Assert.assertEquals(expected.getContentChecksum(), actual.getContentChecksum());
            Assert.assertEquals(expected.getContentLastModified(), actual.getContentLastModified());
            Assert.assertEquals(expected.getContentLength(), actual.getContentLength());

            // prevent duplicate artifacts
            try {
                String dupePath = "collection/path/dupel";
                StorageMetadata dupe = new StorageMetadata(sloc, 
                    URI.create("eos:" + relPath), // duplicate artifact URI
                    URI.create("md5:8410bd62b2bd06bc64f3241ad8a6a0dc"),
                    123L,
                    new Date());
                dao.put(dupe);
                Assert.fail("tried to insert duplicate, expected Exception was not thrown");
            } catch (IllegalArgumentException ex) {
                log.info("caught expected: " + ex);
            }

            dao.delete(sloc);
            StorageMetadata deleted = dao.get(sloc);
            Assert.assertNull(deleted);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testIterator() {
        
    }
    
    @Test
    public void testBucketIterator() {
        
    }
}

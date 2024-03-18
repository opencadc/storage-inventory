/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

package org.opencadc.vospace.db;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.sql.Connection;
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
import org.opencadc.vospace.db.ArtifactSyncWorker;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.inventory.db.HarvestStateDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.TestUtil;
import org.opencadc.inventory.db.version.InitDatabaseSI;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.db.InitDatabaseVOS;
import org.opencadc.vospace.db.NodeDAO;

/**
 *
 * @author adriand
 */
public class ArtifactSyncWorkerTest {
    private static final Logger log = Logger.getLogger(ArtifactSyncWorkerTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.vospace", Level.INFO);
        Log4jInit.setLevel("org.opencadc.vospace.db", Level.INFO);
    }

    HarvestStateDAO harvestStateDAO;
    NodeDAO nodeDAO;
    ArtifactDAO artifactDAO;


    public ArtifactSyncWorkerTest() throws Exception {
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
        DBUtil.PoolConfig pool = new DBUtil.PoolConfig(cc, 1, 6000L, "select 123");
        DBUtil.createJNDIDataSource("jdbc/ArtifactSyncWorkerTest-node", pool);

        Map<String,Object> config = new TreeMap<>();
        config.put(SQLGenerator.class.getName(), SQLGenerator.class);
        config.put("jndiDataSourceName", "jdbc/ArtifactSyncWorkerTest-node");
        config.put("database", TestUtil.DATABASE);
        config.put("invSchema", TestUtil.SCHEMA);
        config.put("genSchema", TestUtil.SCHEMA);
        config.put("vosSchema", TestUtil.VOS_SCHEMA);

        this.harvestStateDAO = new HarvestStateDAO();
        harvestStateDAO.setConfig(config);
        this.nodeDAO = new NodeDAO();
        nodeDAO.setConfig(config);

        pool = new DBUtil.PoolConfig(cc, 1, 6000L, "select 123");
        DBUtil.createJNDIDataSource("jdbc/ArtifactSyncWorkerTest-artifact", pool);

        config.put("jndiDataSourceName", "jdbc/ArtifactSyncWorkerTest-artifact");

        this.artifactDAO = new ArtifactDAO();
        artifactDAO.setConfig(config);
    }
    
    @Before
    public void init_cleanup() throws Exception {
        log.info("init database...");
        InitDatabaseSI initSI = new InitDatabaseSI(artifactDAO.getDataSource(), TestUtil.DATABASE, TestUtil.SCHEMA);
        initSI.doInit();
        log.info("init SI database... OK");
        InitDatabaseVOS initVOS = new InitDatabaseVOS(nodeDAO.getDataSource(), TestUtil.DATABASE, TestUtil.VOS_SCHEMA);
        initVOS.doInit();
        log.info("init VOS database... OK");

        log.info("clearing old content...");
        // src DB
        SQLGenerator gen = artifactDAO.getSQLGenerator();
        DataSource ds = artifactDAO.getDataSource();
        String sql = "delete from " + gen.getTable(Artifact.class);
        Connection con = ds.getConnection();
        con.createStatement().execute(sql);
        con.close();

        gen = harvestStateDAO.getSQLGenerator();
        ds = harvestStateDAO.getDataSource();
        sql = "delete from " + gen.getTable(HarvestState.class);
        con = ds.getConnection();
        con.createStatement().execute(sql);

        gen = nodeDAO.getSQLGenerator();
        sql = "delete from " + gen.getTable(ContainerNode.class);
        log.info("pre-test cleanup: " + sql);
        con.createStatement().execute(sql);
        con.close();

        log.info("clearing old content... OK");
    }
    
    @Test
    public void testSyncArtifact() throws Exception {
        UUID rootID = new UUID(0L, 0L);
        ContainerNode root = new ContainerNode(rootID, "root");

        // create the data node
        Namespace siNamespace = new Namespace("myorg:VOS/");
        URI artifactURI = URI.create(siNamespace.getNamespace() + UUID.randomUUID());
        DataNode orig = new DataNode(UUID.randomUUID(), "data-test", artifactURI);
        orig.parentID = root.getID();
        orig.ownerID = "the-owner";
        orig.isPublic = true;
        orig.isLocked = false;
        nodeDAO.put(orig);

        // get-by-id
        DataNode actual = (DataNode)nodeDAO.get(orig.getID());
        Assert.assertNotNull(actual);
        log.info("found: "  + actual.getID() + " aka " + actual);
        Assert.assertNull(orig.bytesUsed);

        // create the corresponding artifact
        Artifact expected = new Artifact(
                artifactURI,
                URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"),
                new Date(),
                666L);
        log.info("expected: " + expected);

        artifactDAO.put(expected);
        Artifact actualArtifact = artifactDAO.get(expected.getID());
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.getContentLength(), actualArtifact.getContentLength());

        String hsName = "ArtifactSize";
        URI resourceID = URI.create("ivo://myorg.org/vospace");
        HarvestState hs = new HarvestState(hsName, resourceID);
        harvestStateDAO.put(hs);
        hs = harvestStateDAO.get(hsName, resourceID);

        ArtifactSyncWorker asWorker = new ArtifactSyncWorker(harvestStateDAO, hs, artifactDAO, siNamespace);
        asWorker.run();

        actual = (DataNode)nodeDAO.get(orig.getID());
        Assert.assertNotNull(actual);
        log.info("found: "  + actual.getID() + " aka " + actual);
        Assert.assertEquals(expected.getContentLength(), actual.bytesUsed);

        // update the artifact only
        artifactDAO.delete(actualArtifact.getID());
        expected = new Artifact(expected.getURI(), expected.getMetaChecksum(), new Date(), 333L);
        artifactDAO.put(expected);
        actual = (DataNode)nodeDAO.get(orig.getID());
        Assert.assertNotEquals(expected.getContentLength(), actual.bytesUsed);

        // run the update
        asWorker.run();
        actual = (DataNode)nodeDAO.get(orig.getID());
        Assert.assertEquals(expected.getContentLength(), actual.bytesUsed);

    }

}

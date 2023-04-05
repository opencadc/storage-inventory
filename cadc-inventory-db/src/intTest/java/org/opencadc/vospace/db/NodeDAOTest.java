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

package org.opencadc.vospace.db;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.TestUtil;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;

/**
 *
 * @author pdowler
 */
public class NodeDAOTest {
    private static final Logger log = Logger.getLogger(NodeDAOTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.vospace", Level.INFO);
        Log4jInit.setLevel("org.opencadc.vospace.db", Level.DEBUG);
    }
    
    NodeDAO nodeDAO;
    
    public NodeDAOTest() throws Exception {
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
            DBUtil.createJNDIDataSource("jdbc/ArtifactDAOTest", cc);

            Map<String,Object> config = new TreeMap<String,Object>();
            config.put(SQLGenerator.class.getName(), SQLGenerator.class);
            config.put("jndiDataSourceName", "jdbc/ArtifactDAOTest");
            config.put("database", TestUtil.DATABASE);
            config.put("schema", TestUtil.SCHEMA);
            config.put("vosSchema", TestUtil.VOS_SCHEMA);
            
            this.nodeDAO = new NodeDAO();
            nodeDAO.setConfig(config);
            
        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw ex;
        }
    }
    
    @Before
    public void init_cleanup() throws Exception {
        log.info("init database...");
        InitDatabaseVOS init = new InitDatabaseVOS(nodeDAO.getDataSource(), TestUtil.DATABASE, TestUtil.VOS_SCHEMA);
        init.doInit();
        log.info("init database... OK");
        
        log.info("clearing old content...");
        SQLGenerator gen = nodeDAO.getSQLGenerator();
        DataSource ds = nodeDAO.getDataSource();
        String sql = "delete from " + gen.getTable(Node.class);
        log.info("pre-test cleanup: " + sql);
        ds.getConnection().createStatement().execute(sql);
        log.info("clearing old content... OK");
    }
    
    @Test
    public void testGetByID() {
        UUID id = UUID.randomUUID();
        Node a = nodeDAO.get(id);
        Assert.assertNull(a);
    }
    
    //@Test
    public void testPutGetDeleteContainerNode() {
        ContainerNode root = new ContainerNode("root", false);
        
        ContainerNode n = new ContainerNode("container-test", false);
        n.parent = root;
        nodeDAO.put(n);
        
        Node a = nodeDAO.get(n.getID());
        Assert.assertNotNull(a);
        Assert.assertTrue(a instanceof ContainerNode);
        ContainerNode c = (ContainerNode) a;
        Assert.assertEquals(n.getName(), a.getName());
        // these are set in put
        Assert.assertEquals(n.getMetaChecksum(), a.getMetaChecksum());
        Assert.assertEquals(n.getLastModified(), a.getLastModified());
        ContainerNode ac = nodeDAO.getChildren(c);
        Assert.assertNotNull(ac);
        Assert.assertTrue(ac.nodes.isEmpty());
        
        // add children
        ContainerNode cont = new ContainerNode("container1", false);
        cont.parent = c;
        DataNode data = new DataNode("data1");
        data.parent = c;
        LinkNode link = new LinkNode("link1", URI.create("cadc:ARCHIVE/data"));
        link.parent = c;
        nodeDAO.put(cont);
        nodeDAO.put(data);
        nodeDAO.put(link);
        
        ac = nodeDAO.getChildren(c);
        Assert.assertNotNull(ac);
        Assert.assertFalse(ac.nodes.isEmpty());
        Assert.assertEquals(3, ac.nodes.size());
    }
    
    @Test
    public void testPutGetDeleteDataNode() {
        log.info("TODO");
    }
    
    @Test
    public void testPutGetDeleteLinkNode() {
        log.info("TODO");
    }
}

/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2020.                            (c) 2020.
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
 *
 ************************************************************************
 */

package org.opencadc.fenwick;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import java.util.Map;
import java.util.TreeMap;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.DeletedStorageLocationEventDAO;
import org.opencadc.inventory.db.HarvestStateDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.db.version.InitDatabase;
import org.springframework.jdbc.core.JdbcTemplate;



public class InventoryEnvironment {
    final StorageSiteDAO storageSiteDAO = new StorageSiteDAO();
    final ArtifactDAO artifactDAO = new ArtifactDAO();
    final DeletedArtifactEventDAO deletedArtifactEventDAO = new DeletedArtifactEventDAO();
    final DeletedStorageLocationEventDAO deletedStorageLocationEventDAO = new DeletedStorageLocationEventDAO();
    final HarvestStateDAO harvestStateDAO = new HarvestStateDAO();
    final Map<String, Object> daoConfig = new TreeMap<>();
    final ConnectionConfig connectionConfig;
    
    private final String jndiPath = "jdbc/InventoryEnvironment";

    public InventoryEnvironment() throws Exception {
        final DBConfig dbrc = new DBConfig();
        connectionConfig = dbrc.getConnectionConfig(TestUtil.INVENTORY_SERVER, TestUtil.INVENTORY_DATABASE);
        daoConfig.put(SQLGenerator.class.getName(), SQLGenerator.class);
        daoConfig.put("database", TestUtil.INVENTORY_DATABASE);
        daoConfig.put("schema", TestUtil.INVENTORY_SCHEMA);
        // connectionCOnfig and daoConfig used by InventoryHarvester to create it's own datasource
        
        Map<String, Object> testConfig = new TreeMap<>();
        
        // single direct connection for test setup/verification
        DBUtil.createJNDIDataSource(jndiPath, connectionConfig);
        
        testConfig.put(SQLGenerator.class.getName(), SQLGenerator.class);
        testConfig.put("jndiDataSourceName", jndiPath);
        testConfig.put("database", TestUtil.INVENTORY_DATABASE);
        testConfig.put("schema", TestUtil.INVENTORY_SCHEMA);

        storageSiteDAO.setConfig(testConfig);
        artifactDAO.setConfig(testConfig);
        deletedArtifactEventDAO.setConfig(testConfig);
        deletedStorageLocationEventDAO.setConfig(testConfig);
        harvestStateDAO.setConfig(testConfig);

        new InitDatabase(DBUtil.findJNDIDataSource(jndiPath),
                         (String) daoConfig.get("database"),
                         (String) daoConfig.get("schema")).doInit();
    }

    void cleanTestEnvironment() throws Exception {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate(DBUtil.findJNDIDataSource(jndiPath));
        jdbcTemplate.execute("TRUNCATE TABLE " + TestUtil.INVENTORY_SCHEMA + ".deletedArtifactEvent");
        jdbcTemplate.execute("TRUNCATE TABLE " + TestUtil.INVENTORY_SCHEMA + ".deletedStorageLocationEvent");
        jdbcTemplate.execute("TRUNCATE TABLE " + TestUtil.INVENTORY_SCHEMA + ".storageSite");
        jdbcTemplate.execute("TRUNCATE TABLE " + TestUtil.INVENTORY_SCHEMA + ".harvestState");
        jdbcTemplate.execute("TRUNCATE TABLE " + TestUtil.INVENTORY_SCHEMA + ".Artifact");
    }
}

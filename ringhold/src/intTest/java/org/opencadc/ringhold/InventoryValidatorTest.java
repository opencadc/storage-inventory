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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.ringhold;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedStorageLocationEventDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.version.InitDatabase;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Various versions of:
 * Insert artifacts more than uri pattern
 * Run tool with one uri deselector
 * Confirm delete storage location event creation and absence of artifacts in inventory
 */
public class InventoryValidatorTest {

    private static final Logger log = Logger.getLogger(InventoryValidatorTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.ringhold", Level.INFO);
    }

    static String INVENTORY_SERVER = "RINGHOLD_TEST";
    static String INVENTORY_DATABASE = "cadctest";
    static String INVENTORY_SCHEMA = "inventory";
    static String TMP_DIR = "build/tmp";
    static String USER_HOME = System.getProperty("user.home");

    static {
        try {
            File opt = FileUtil.getFileFromResource("intTest.properties", InventoryValidatorTest.class);
            if (opt.exists()) {
                Properties props = new Properties();
                props.load(new FileReader(opt));

                if (props.containsKey("inventoryServer")) {
                    INVENTORY_SERVER = props.getProperty("inventoryServer").trim();
                }
                if (props.containsKey("inventoryDatabase")) {
                    INVENTORY_DATABASE = props.getProperty("inventoryDatabase").trim();
                }
                if (props.containsKey("inventorySchema")) {
                    INVENTORY_SCHEMA = props.getProperty("inventorySchema").trim();
                }
            }
        } catch (MissingResourceException | FileNotFoundException noFileException) {
            log.debug("No intTest.properties supplied.  Using defaults.");
        } catch (IOException oops) {
            throw new RuntimeException(oops.getMessage(), oops);
        }

        log.debug("intTest database config: " + INVENTORY_SERVER + " " + INVENTORY_DATABASE + " " + INVENTORY_SCHEMA);
    }

    final ArtifactDAO artifactDAO;
    final DeletedStorageLocationEventDAO deletedStorageLocationEventDAO;
    final Map<String, Object> daoConfig = new TreeMap<>();
    final String jndiPath = "jdbc/InventoryEnvironment";

    public InventoryValidatorTest() throws Exception {
        artifactDAO = new ArtifactDAO();
        deletedStorageLocationEventDAO = new DeletedStorageLocationEventDAO();

        final DBConfig dbrc = new DBConfig();
        final ConnectionConfig connectionConfig = dbrc.getConnectionConfig(INVENTORY_SERVER, INVENTORY_DATABASE);
        DBUtil.createJNDIDataSource(jndiPath, connectionConfig);

        try {
            DataSource dataSource = DBUtil.findJNDIDataSource(jndiPath);
            InitDatabase init = new InitDatabase(dataSource, INVENTORY_DATABASE, INVENTORY_SCHEMA);
            init.doInit();
            log.debug("initDatabase: " + jndiPath + " " + INVENTORY_SCHEMA + " OK");
        } catch (Exception ex) {
            throw new IllegalStateException("check/init database failed", ex);
        }

        daoConfig.put(SQLGenerator.class.getName(), SQLGenerator.class);
        daoConfig.put("jndiDataSourceName", jndiPath);
        daoConfig.put("database", INVENTORY_DATABASE);
        daoConfig.put("schema", INVENTORY_SCHEMA);

        artifactDAO.setConfig(daoConfig);
        deletedStorageLocationEventDAO.setConfig(daoConfig);
    }

    @Before
    public void setup() throws Exception {
        writeConfig();
        truncateTables();
    }

    @Test
    public void noArtifactsMatchFilter() throws Exception {
        Artifact a1 = getTestArtifact("cadc:TEST/one.txt");
        this.artifactDAO.put(a1);
        Artifact a2 = getTestArtifact("cadc:INT/two.txt");
        this.artifactDAO.put(a2);
        Artifact a3 = getTestArtifact("cadc:CADC/three.txt");
        this.artifactDAO.put(a3);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.daoConfig, this.daoConfig);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        a1 = this.artifactDAO.get(a1.getID());
        Assert.assertNotNull(a1);
        a2 = this.artifactDAO.get(a2.getID());
        Assert.assertNotNull(a2);
        a3 = this.artifactDAO.get(a3.getID());
        Assert.assertNotNull(a3);

        DeletedStorageLocationEvent dsle1 = this.deletedStorageLocationEventDAO.get(a1.getID());
        Assert.assertNull(dsle1);
        DeletedStorageLocationEvent dsle2 = this.deletedStorageLocationEventDAO.get(a2.getID());
        Assert.assertNull(dsle2);
        DeletedStorageLocationEvent dsle3 = this.deletedStorageLocationEventDAO.get(a3.getID());
        Assert.assertNull(dsle3);
    }

    @Test
    public void someArtifactsMatchFilter() throws Exception {
        Artifact b1 = getTestArtifact("cadc:INT/one.txt");
        this.artifactDAO.put(b1);
        Artifact b2 = getTestArtifact("cadc:INT_TEST/two.txt");
        this.artifactDAO.put(b2);
        Artifact a1 = getTestArtifact("cadc:INTTEST/three.txt");
        this.artifactDAO.put(a1);
        Artifact a2 = getTestArtifact("cadc:INTTEST/four.txt");
        this.artifactDAO.put(a2);
        Artifact a3 = getTestArtifact("cadc:INTTEST/five.txt");
        this.artifactDAO.put(a3);
        Artifact b3 = getTestArtifact("cadc:TEST/six.txt");
        this.artifactDAO.put(b3);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.daoConfig, this.daoConfig);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        DeletedStorageLocationEvent a_dsle1 = this.deletedStorageLocationEventDAO.get(a1.getID());
        Assert.assertNotNull(a_dsle1);
        DeletedStorageLocationEvent a_dsle2 = this.deletedStorageLocationEventDAO.get(a2.getID());
        Assert.assertNotNull(a_dsle2);
        DeletedStorageLocationEvent a_dsle3 = this.deletedStorageLocationEventDAO.get(a3.getID());
        Assert.assertNotNull(a_dsle3);

        DeletedStorageLocationEvent b_dsle1 = this.deletedStorageLocationEventDAO.get(b1.getID());
        Assert.assertNull(b_dsle1);
        DeletedStorageLocationEvent b_dsle2 = this.deletedStorageLocationEventDAO.get(b2.getID());
        Assert.assertNull(b_dsle2);
        DeletedStorageLocationEvent b_dsle3 = this.deletedStorageLocationEventDAO.get(b3.getID());
        Assert.assertNull(b_dsle3);

        a1 = this.artifactDAO.get(a1.getID());
        Assert.assertNull(a1);
        a2 = this.artifactDAO.get(a2.getID());
        Assert.assertNull(a2);
        a3 = this.artifactDAO.get(a3.getID());
        Assert.assertNull(a3);

        b1 = this.artifactDAO.get(b1.getID());
        Assert.assertNotNull(b1);
        b2 = this.artifactDAO.get(b2.getID());
        Assert.assertNotNull(b2);
        b3 = this.artifactDAO.get(b3.getID());
        Assert.assertNotNull(b3);
    }

    @Test
    public void allArtifactsMatchFilter() throws Exception {
        Artifact a1 = getTestArtifact("cadc:INTTEST/one.txt");
        this.artifactDAO.put(a1);
        Artifact a2 = getTestArtifact("cadc:INTTEST/two.txt");
        this.artifactDAO.put(a2);
        Artifact a3 = getTestArtifact("cadc:INTTEST/three.txt");
        this.artifactDAO.put(a3);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.daoConfig, this.daoConfig);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        DeletedStorageLocationEvent a_dsle1 = this.deletedStorageLocationEventDAO.get(a1.getID());
        Assert.assertNotNull(a_dsle1);
        DeletedStorageLocationEvent a_dsle2 = this.deletedStorageLocationEventDAO.get(a2.getID());
        Assert.assertNotNull(a_dsle2);
        DeletedStorageLocationEvent a_dsle3 = this.deletedStorageLocationEventDAO.get(a3.getID());
        Assert.assertNotNull(a_dsle3);
    }

    private void writeConfig() throws IOException {
        final Path includePath = new File(TMP_DIR + "/config").toPath();
        Files.createDirectories(includePath);
        final File includeFile = new File(includePath.toFile(), "artifact-deselector.sql");

        final FileWriter fileWriter = new FileWriter(includeFile);
        fileWriter.write("WHERE uri LIKE 'cadc:INTTEST/%'");
        fileWriter.flush();
        fileWriter.close();
    }

    private Artifact getTestArtifact(final String uri) {
        UUID uuid = UUID.randomUUID();
        URI checkSum =  URI.create("md5:" + HexUtil.toHex(uuid.getMostSignificantBits()) + HexUtil.toHex(uuid.getLeastSignificantBits()));
        return new Artifact(URI.create(uri), checkSum, new Date(), 512L);
    }


    private void truncateTables() throws Exception {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate(DBUtil.findJNDIDataSource(jndiPath));
        jdbcTemplate.execute("TRUNCATE TABLE " + INVENTORY_SCHEMA + ".deletedArtifactEvent");
        jdbcTemplate.execute("TRUNCATE TABLE " + INVENTORY_SCHEMA + ".deletedStorageLocationEvent");
        jdbcTemplate.execute("TRUNCATE TABLE " + INVENTORY_SCHEMA + ".storageSite");
        jdbcTemplate.execute("TRUNCATE TABLE " + INVENTORY_SCHEMA + ".harvestState");
        jdbcTemplate.execute("TRUNCATE TABLE " + INVENTORY_SCHEMA + ".Artifact");
    }

}

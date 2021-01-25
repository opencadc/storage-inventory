/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2021.                            (c) 2021.
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

package org.opencadc.ratik;

import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.MissingResourceException;
import java.util.Properties;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.util.IncludeArtifacts;

public class InventoryValidatorIteratorsTest {
    private static final Logger log = Logger.getLogger(InventoryValidatorIteratorsTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.ratik", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
    }

    static String INVENTORY_SERVER = "RATIK_TEST";
    static String INVENTORY_DATABASE = "cadctest";
    static String INVENTORY_SCHEMA = "inventory";
    static String TMP_DIR = "build/tmp";
    static String USER_HOME = System.getProperty("user.home");
    static URI REMOTE_STORAGE_SITE = URI.create("ivo://cadc.nrc.ca/minoc");

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

    private final InventoryEnvironment localEnvironment = new InventoryEnvironment();
    private final LuskanEnvironment remoteEnvironment = new LuskanEnvironment();

    public InventoryValidatorIteratorsTest() throws Exception {

    }

    @Before
    public void beforeTest() throws Exception {
        writeConfig();
        this.localEnvironment.cleanTestEnvironment();
        this.remoteEnvironment.cleanTestEnvironment();
    }

    private void writeConfig() throws IOException {
        final Path includePath = new File(TMP_DIR + "/config").toPath();
        Files.createDirectories(includePath);
        final File includeFile = new File(includePath.toFile(), "artifact-filter.sql");

        final FileWriter fileWriter = new FileWriter(includeFile);
        fileWriter.write("WHERE uri LIKE 'cadc:INTTEST/%'");
        fileWriter.flush();
        fileWriter.close();
    }

    private void logValidate(Artifact localArtifact, Artifact remoteArtifact) {
        String local = (localArtifact == null ? "null" : localArtifact.getURI().toString());
        String remote = (remoteArtifact == null ? "null" : remoteArtifact.getURI().toString());
        log.info(String.format("local=%s remote=%s", local, remote));
    }
    /**
     * 2 Artifacts in both local and remote.
     * 2 iterator loops should validate the same Artifact.
     */
    @Test
    public void artifactsEqual() throws Exception {
        URI uri1 = URI.create("cadc:INTTEST/file1.ext");
        Artifact artifact1 = new Artifact(uri1, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact1);
        this.remoteEnvironment.artifactDAO.put(artifact1);

        URI uri2 = URI.create("cadc:INTTEST/file2.ext");
        Artifact artifact2 = new Artifact(uri2, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact2);
        this.remoteEnvironment.artifactDAO.put(artifact2);

        List<Artifact> artifacts = new ArrayList<>();
        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig,
                                                                    TestUtil.LUSKAN_URI, new IncludeArtifacts(),
                                                                    null, false) {
                @Override
                void validate(Artifact localArtifact, Artifact remoteArtifact) {
                    artifacts.add(localArtifact);
                    artifacts.add(remoteArtifact);
                    logValidate(localArtifact, remoteArtifact);
                }
            };
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }
        Assert.assertEquals(4, artifacts.size());

        // 1st iterator loop
        Artifact local = artifacts.get(0);
        Artifact remote = artifacts.get(1);
        Assert.assertNotNull(local);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact1, local);
        Assert.assertEquals(artifact1, remote);

        // 2nd iterator loop
        local = artifacts.get(2);
        remote = artifacts.get(3);
        Assert.assertNotNull(local);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact2, local);
        Assert.assertEquals(artifact2, remote);
    }

    /**
     * 2 Artifacts in local, no Artifacts in remote.
     * 2 iterator loops should validate a local Artifact and a null remote Artifact.
     */
    @Test
    public void localOnly() throws Exception {
        URI uri1 = URI.create("cadc:INTTEST/file1.ext");
        Artifact localArtifact1 = new Artifact(uri1, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(localArtifact1);

        URI uri2 = URI.create("cadc:INTTEST/file2.ext");
        Artifact localArtifact2 = new Artifact(uri2, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(localArtifact2);

        List<Artifact> artifacts = new ArrayList<>();
        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig,
                                                                    TestUtil.LUSKAN_URI, new IncludeArtifacts(),
                                                                    null, false) {
                @Override
                void validate(Artifact localArtifact, Artifact remoteArtifact) {
                    artifacts.add(localArtifact);
                    artifacts.add(remoteArtifact);
                    logValidate(localArtifact, remoteArtifact);
                }
            };
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }
        Assert.assertEquals(4, artifacts.size());

        // 1st iterator loop
        Artifact local = artifacts.get(0);
        Artifact remote = artifacts.get(1);
        Assert.assertNotNull(local);
        Assert.assertNull(remote);
        Assert.assertEquals(localArtifact1, local);

        // 2nd iterator loop
        local = artifacts.get(2);
        remote = artifacts.get(3);
        Assert.assertNotNull(local);
        Assert.assertNull(remote);
        Assert.assertEquals(localArtifact2, local);
    }

    /**
     * 2 Artifacts in remote, no Artifacts in local.
     * Each iterator loop should validate a remote Artifact and a null local Artifact.
     */
    @Test
    public void remoteOnly() throws Exception {
        URI uri1 = URI.create("cadc:INTTEST/file1.ext");
        Artifact remoteArtifact1 = new Artifact(uri1, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.remoteEnvironment.artifactDAO.put(remoteArtifact1);

        URI uri2 = URI.create("cadc:INTTEST/file2.ext");
        Artifact remoteArtifact2 = new Artifact(uri2, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.remoteEnvironment.artifactDAO.put(remoteArtifact2);

        List<Artifact> artifacts = new ArrayList<>();
        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig,
                                                                    TestUtil.LUSKAN_URI, new IncludeArtifacts(),
                                                                    null, false) {
                @Override
                void validate(Artifact localArtifact, Artifact remoteArtifact) {
                    artifacts.add(localArtifact);
                    artifacts.add(remoteArtifact);
                    logValidate(localArtifact, remoteArtifact);
                }
            };
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }
        Assert.assertEquals(4, artifacts.size());

        // 1st iterator loop
        Artifact local = artifacts.get(0);
        Artifact remote = artifacts.get(1);
        Assert.assertNull(local);
        Assert.assertNotNull(remote);
        Assert.assertEquals(remoteArtifact1, remote);

        // 2nd iterator loop
        local = artifacts.get(2);
        remote = artifacts.get(3);
        Assert.assertNull(local);
        Assert.assertNotNull(remote);
        Assert.assertEquals(remoteArtifact2, remote);
    }

    /**
     * 2 Artifacts in local, 1 Artifact in remote.
     * The 2 local Artifacts compare greater than the single remote Artifact.
     * The first 2 loops should validate a local Artifact and a null remote Artifact.
     * The 3rd loop should validate a null local Artifact and the remote Artifact.
     */
    @Test
    public void localGreaterThanRemote() throws Exception {
        URI uri1 = URI.create("cadc:INTTEST/file1.ext");
        Artifact localArtifact1 = new Artifact(uri1, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(localArtifact1);

        URI uri2 = URI.create("cadc:INTTEST/file2.ext");
        Artifact localArtifact2 = new Artifact(uri2, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(localArtifact2);

        URI uri3 = URI.create("cadc:INTTEST/file3.ext");
        Artifact remoteArtifact = new Artifact(uri3, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.remoteEnvironment.artifactDAO.put(remoteArtifact);

        List<Artifact> artifacts = new ArrayList<>();
        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig,
                                                                    TestUtil.LUSKAN_URI, new IncludeArtifacts(),
                                                                    null, false) {
                @Override
                void validate(Artifact localArtifact, Artifact remoteArtifact) {
                    artifacts.add(localArtifact);
                    artifacts.add(remoteArtifact);
                    logValidate(localArtifact, remoteArtifact);
                }
            };
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }
        Assert.assertEquals(6, artifacts.size());

        // 1st iterator loop
        Artifact local = artifacts.get(0);
        Artifact remote = artifacts.get(1);
        Assert.assertNotNull(local);
        Assert.assertNull(remote);
        Assert.assertEquals(localArtifact1, local);

        // 2nd iterator loop
        local = artifacts.get(2);
        remote = artifacts.get(3);
        Assert.assertNotNull(local);
        Assert.assertNull(remote);
        Assert.assertEquals(localArtifact2, local);

        // 3rd iterator loop
        local = artifacts.get(4);
        remote = artifacts.get(5);
        Assert.assertNull(local);
        Assert.assertNotNull(remote);
        Assert.assertEquals(remoteArtifact, remote);
    }

    /**
     * 2 Artifacts in remote, 1 Artifact in local.
     * The 2 remote Artifacts compare greater than the single local Artifact.
     * The first 2 loops should validate a null local Artifact and a remote Artifact.
     * The 3rd loop should validate a local Artifact and a null remote Artifact.
     */
    @Test
    public void remoteGreaterThanLocal() throws Exception {
        URI uri1 = URI.create("cadc:INTTEST/file3.ext");
        Artifact localArtifact = new Artifact(uri1, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(localArtifact);

        URI uri2 = URI.create("cadc:INTTEST/file1.ext");
        Artifact remoteArtifact1 = new Artifact(uri2, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.remoteEnvironment.artifactDAO.put(remoteArtifact1);

        URI uri3 = URI.create("cadc:INTTEST/file2.ext");
        Artifact remoteArtifact2 = new Artifact(uri3, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.remoteEnvironment.artifactDAO.put(remoteArtifact2);

        List<Artifact> artifacts = new ArrayList<>();
        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig,
                                                                    TestUtil.LUSKAN_URI, new IncludeArtifacts(),
                                                                    null, false) {
                @Override
                void validate(Artifact localArtifact, Artifact remoteArtifact) {
                    artifacts.add(localArtifact);
                    artifacts.add(remoteArtifact);
                    logValidate(localArtifact, remoteArtifact);
                }
            };
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }
        Assert.assertEquals(6, artifacts.size());

        // 1st iterator loop
        Artifact local = artifacts.get(0);
        Artifact remote = artifacts.get(1);
        Assert.assertNull(local);
        Assert.assertNotNull(remote);
        Assert.assertEquals(remoteArtifact1, remote);

        // 2nd iterator loop
        local = artifacts.get(2);
        remote = artifacts.get(3);
        Assert.assertNull(local);
        Assert.assertNotNull(remote);
        Assert.assertEquals(remoteArtifact2, remote);

        // 3rd iterator loop
        local = artifacts.get(4);
        remote = artifacts.get(5);
        Assert.assertNotNull(local);
        Assert.assertNull(remote);
        Assert.assertEquals(localArtifact, local);
    }

    /**
     * 2 Artifacts in remote, 2 Artifacts in local.
     * 1st local Artifact > 1st remote Artifact > 2nd local Artifact > 2nd remote Artifact.
     * 4 iterator loops should validate alternating local and remote Artifacts with a null Artifact.
     */
    @Test
    public void alternatingArtifacts() throws Exception {
        URI uri1 = URI.create("cadc:INTTEST/file1.ext");
        Artifact localArtifact1 = new Artifact(uri1, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(localArtifact1);

        URI uri2 = URI.create("cadc:INTTEST/file2.ext");
        Artifact remoteArtifact1 = new Artifact(uri2, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.remoteEnvironment.artifactDAO.put(remoteArtifact1);

        URI uri3 = URI.create("cadc:INTTEST/file3.ext");
        Artifact localArtifact2 = new Artifact(uri3, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(localArtifact2);

        URI uri4 = URI.create("cadc:INTTEST/file4.ext");
        Artifact remoteArtifact2 = new Artifact(uri4, TestUtil.getRandomMD5(), new Date(), 1024L);
        this.remoteEnvironment.artifactDAO.put(remoteArtifact2);

        List<Artifact> artifacts = new ArrayList<>();
        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig,
                                                                    TestUtil.LUSKAN_URI, new IncludeArtifacts(),
                                                                    null, false) {
                @Override
                void validate(Artifact localArtifact, Artifact remoteArtifact) {
                    artifacts.add(localArtifact);
                    artifacts.add(remoteArtifact);
                    logValidate(localArtifact, remoteArtifact);
                }
            };
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }
        Assert.assertEquals(8, artifacts.size());

        // 1st iterator loop
        Artifact local = artifacts.get(0);
        Artifact remote = artifacts.get(1);
        Assert.assertNotNull(local);
        Assert.assertNull(remote);
        Assert.assertEquals(localArtifact1, local);

        // 2nd iterator loop
        local = artifacts.get(2);
        remote = artifacts.get(3);
        Assert.assertNull(local);
        Assert.assertNotNull(remote);
        Assert.assertEquals(remoteArtifact1, remote);

        // 3rd iterator loop
        local = artifacts.get(4);
        remote = artifacts.get(5);
        Assert.assertNotNull(local);
        Assert.assertNull(remote);
        Assert.assertEquals(localArtifact2, local);

        // 4th iterator loop
        local = artifacts.get(6);
        remote = artifacts.get(7);
        Assert.assertNull(local);
        Assert.assertNotNull(remote);
        Assert.assertEquals(remoteArtifact2, remote);
    }
}

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

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.util.BucketSelector;
import org.opencadc.inventory.util.IncludeArtifacts;

public class InventoryValidatorIteratorsTest {
    private static final Logger log = Logger.getLogger(InventoryValidatorIteratorsTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.ratik", Level.DEBUG);
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
    }

    static String TMP_DIR = "build/tmp";
    static String USER_HOME = System.getProperty("user.home");

    private final InventoryEnvironment localEnvironment = new InventoryEnvironment();
    private final LuskanEnvironment remoteEnvironment = new LuskanEnvironment();

    public InventoryValidatorIteratorsTest() throws Exception {}

    @Before
    public void beforeTest() throws Exception {
        writeConfig();
        this.localEnvironment.cleanTestEnvironment();
        this.remoteEnvironment.cleanTestEnvironment();
    }

    private void writeConfig() throws IOException {
        // write artifact filter
        final Path includePath = new File(TMP_DIR + "/config").toPath();
        Files.createDirectories(includePath);
        final File includeFile = new File(includePath.toFile(), "artifact-filter.sql");

        final FileWriter fileWriter = new FileWriter(includeFile);
        fileWriter.write("WHERE uri LIKE 'cadc:INTTEST/%'");
        fileWriter.flush();
        fileWriter.close();

        // copy proxy cert to tmp dir
        final Path tmpCertDir = Paths.get(TMP_DIR + "/.ssl");
        Files.createDirectories(tmpCertDir);
        Path sourcePath = Paths.get(InventoryValidator.CERTIFICATE_FILE_LOCATION);
        Path destPath = Paths.get(tmpCertDir + "/cadcproxy.pem");
        Files.copy(sourcePath, destPath, REPLACE_EXISTING);
    }

    @Test
    public void foo() throws Exception {
        Artifact artifact1 = buildArtifact("cadc:INTTEST/file1.ext");
        Artifact artifact2 = buildArtifact("cadc:INTTEST/file2.ext");
        Artifact artifact3 = buildArtifact("cadc:INTTEST/file3.ext");

        this.localEnvironment.artifactDAO.put(artifact1);
        this.localEnvironment.artifactDAO.put(artifact2);
        this.localEnvironment.artifactDAO.put(artifact3);
        this.remoteEnvironment.artifactDAO.put(artifact1);
        this.remoteEnvironment.artifactDAO.put(artifact2);
        this.remoteEnvironment.artifactDAO.put(artifact3);

        logArtifact(artifact1, "local ");
        logArtifact(artifact2, "local ");
        logArtifact(artifact3, "local ");
        logArtifact(artifact1, "remote");
        logArtifact(artifact2, "remote");
        logArtifact(artifact3, "remote");

        String bucket2 = artifact2.getBucket().substring(0, 1);

        List<Artifact> ordered = new ArrayList<>();
        orderArtifacts(ordered, bucket2);
        Assert.assertEquals(2, ordered.size());

        // 1st iterator loop
        Artifact local = ordered.get(0);
        Assert.assertNotNull(local);
        Assert.assertEquals(artifact2, local);

        Artifact remote = ordered.get(1);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact2, remote);
    }

    /**
     * 2 Artifacts in both local and remote.
     * 2 iterator loops should validate the same Artifact.
     */
    @Test
    public void artifactsEqual() throws Exception {
        Artifact artifact1 = buildArtifact("cadc:INTTEST/file1.ext");
        Artifact artifact2 = buildArtifact("cadc:INTTEST/file2.ext");

        this.localEnvironment.artifactDAO.put(artifact1);
        this.localEnvironment.artifactDAO.put(artifact2);
        this.remoteEnvironment.artifactDAO.put(artifact1);
        this.remoteEnvironment.artifactDAO.put(artifact2);

        logArtifact(artifact1, "local ");
        logArtifact(artifact2, "local ");
        logArtifact(artifact1, "remote");
        logArtifact(artifact2, "remote");

        List<Artifact> ordered = new ArrayList<>();
        orderArtifacts(ordered, null);
        Assert.assertEquals(4, ordered.size());

        // 1st iterator loop [artifact1, artifact1]
        Artifact local = ordered.get(0);
        Assert.assertNotNull(local);
        Assert.assertEquals(artifact1, local);

        Artifact remote = ordered.get(1);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact1, remote);

        // 2nd iterator loop [artifact2, artifact2]
        local = ordered.get(2);
        Assert.assertNotNull(local);
        Assert.assertEquals(artifact2, local);

        remote = ordered.get(3);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact2, remote);
    }

    /**
     * 2 Artifacts in local, no Artifacts in remote.
     * 2 iterator loops should validate a local Artifact and a null remote Artifact.
     */
    @Test
    public void localOnly() throws Exception {
        Artifact artifact1 = buildArtifact("cadc:INTTEST/file1.ext");
        Artifact artifact2 = buildArtifact("cadc:INTTEST/file2.ext");
        this.localEnvironment.artifactDAO.put(artifact1);
        this.localEnvironment.artifactDAO.put(artifact2);
        logArtifact(artifact1, "local ");
        logArtifact(artifact2, "local ");

        List<Artifact> ordered = new ArrayList<>();
        orderArtifacts(ordered, null);
        Assert.assertEquals(4, ordered.size());

        // 1st iterator loop [artifact1, null]
        Artifact local = ordered.get(0);
        Assert.assertNotNull(local);
        Assert.assertEquals(artifact1, local);

        Artifact remote = ordered.get(1);
        Assert.assertNull(remote);

        // 2nd iterator loop [artifact2, null]
        local = ordered.get(2);
        Assert.assertNotNull(local);
        Assert.assertEquals(artifact2, local);

        remote = ordered.get(3);
        Assert.assertNull(remote);
    }

    /**
     * 2 Artifacts in remote, no Artifacts in local.
     * 2 iterator loops should validate a null local Artifact and a remote Artifact.
     */
    @Test
    public void remoteOnly() throws Exception {
        Artifact artifact1 = buildArtifact("cadc:INTTEST/file1.ext");
        Artifact artifact2 = buildArtifact("cadc:INTTEST/file2.ext");
        this.remoteEnvironment.artifactDAO.put(artifact1);
        this.remoteEnvironment.artifactDAO.put(artifact2);
        logArtifact(artifact1, "remote");
        logArtifact(artifact2, "remote");

        List<Artifact> ordered = new ArrayList<>();
        orderArtifacts(ordered, null);
        Assert.assertEquals(4, ordered.size());

        // 1st iterator loop [null, artifact1]
        Artifact local = ordered.get(0);
        Assert.assertNull(local);

        Artifact remote = ordered.get(1);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact1, remote);

        // 2nd iterator loop [null, artifact2]
        local = ordered.get(2);
        Assert.assertNull(local);

        remote = ordered.get(3);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact2, remote);
    }

    /**
     * 2 Artifacts in local, 1 Artifact in remote.
     * The 2 local Artifacts order before the the single remote Artifact.
     * The first 2 loops should validate a local Artifact and a null remote Artifact.
     * The 3rd loop should validate a null local Artifact and the remote Artifact.
     */
    @Test
    public void localBeforeRemote() throws Exception {
        Artifact artifact1 = buildArtifact("cadc:INTTEST/file1.ext");
        Artifact artifact2 = buildArtifact("cadc:INTTEST/file2.ext");
        this.localEnvironment.artifactDAO.put(artifact1);
        this.localEnvironment.artifactDAO.put(artifact2);
        logArtifact(artifact1, "local ");
        logArtifact(artifact2, "local ");

        Artifact artifact3 = buildArtifact("cadc:INTTEST/file3.ext");
        this.remoteEnvironment.artifactDAO.put(artifact3);
        logArtifact(artifact3, "remote");

        List<Artifact> ordered = new ArrayList<>();
        orderArtifacts(ordered, null);
        Assert.assertEquals(6, ordered.size());

        // 1st iterator loop [artifact1, null]
        Artifact local = ordered.get(0);
        Assert.assertNotNull(local);
        Assert.assertEquals(artifact1, local);

        Artifact remote = ordered.get(1);
        Assert.assertNull(remote);

        // 2nd iterator loop [artifact2, null]
        local = ordered.get(2);
        Assert.assertNotNull(local);
        Assert.assertEquals(artifact2, local);

        remote = ordered.get(3);
        Assert.assertNull(remote);

        // 3rd iterator loop [null, artifact3]
        local = ordered.get(4);
        Assert.assertNull(local);

        remote = ordered.get(5);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact3, remote);
    }

    /**
     * 1 Artifact in local, 2 Artifacts in remote.
     * The local Artifact orders after the the two remote Artifacts.
     * The first 2 loops should validate a null local Artifact and a remote Artifact.
     * The 3rd loop should validate a local Artifact and a null remote Artifact.
     */
    @Test
    public void localAfterRemote() throws Exception {
        Artifact artifact3 = buildArtifact("cadc:INTTEST/file3.ext");
        this.localEnvironment.artifactDAO.put(artifact3);
        logArtifact(artifact3, "local ");

        Artifact artifact1 = buildArtifact("cadc:INTTEST/file1.ext");
        Artifact artifact2 = buildArtifact("cadc:INTTEST/file2.ext");
        this.remoteEnvironment.artifactDAO.put(artifact1);
        this.remoteEnvironment.artifactDAO.put(artifact2);
        logArtifact(artifact1, "remote");
        logArtifact(artifact2, "remote");

        List<Artifact> ordered = new ArrayList<>();
        orderArtifacts(ordered, null);
        Assert.assertEquals(6, ordered.size());

        // 1st iterator loop [null, artifact1]
        Artifact local = ordered.get(0);
        Assert.assertNull(local);

        Artifact remote = ordered.get(1);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact1, remote);

        // 2nd iterator loop [null, artifact2]
        local = ordered.get(2);
        Assert.assertNull(local);

        remote = ordered.get(3);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact2, remote);

        // 3rd iterator loop [artifact3, null]
        local = ordered.get(4);
        Assert.assertNotNull(local);
        Assert.assertEquals(artifact3, local);

        remote = ordered.get(5);
        Assert.assertNull(remote);
    }

    /**
     * 2 Artifacts in local, 2 Artifacts in remote.
     * 1st local Artifact orders before 1st remote Artifact orders before 2nd local Artifact orders before 2nd remote Artifact.
     * 4 iterator loops should validate alternating local and remote Artifacts with a null Artifact.
     */
    @Test
    public void alternatingOrder() throws Exception {
        Artifact artifact1 = buildArtifact("cadc:INTTEST/file1.ext");
        Artifact artifact3 = buildArtifact("cadc:INTTEST/file3.ext");
        this.localEnvironment.artifactDAO.put(artifact1);
        this.localEnvironment.artifactDAO.put(artifact3);
        logArtifact(artifact1, "local ");
        logArtifact(artifact3, "local ");

        Artifact artifact2 = buildArtifact("cadc:INTTEST/file2.ext");
        Artifact artifact4 = buildArtifact("cadc:INTTEST/file4.ext");
        this.remoteEnvironment.artifactDAO.put(artifact2);
        this.remoteEnvironment.artifactDAO.put(artifact4);
        logArtifact(artifact2, "remote");
        logArtifact(artifact4, "remote");

        List<Artifact> ordered = new ArrayList<>();
        orderArtifacts(ordered, null);
        Assert.assertEquals(8, ordered.size());

        // 1st iterator loop [artifact1, null]
        Artifact local = ordered.get(0);
        Assert.assertNotNull(local);
        Assert.assertEquals(artifact1, local);

        Artifact remote = ordered.get(1);
        Assert.assertNull(remote);

        // 2nd iterator loop [null, artifact2]
        local = ordered.get(2);
        Assert.assertNull(local);

        remote = ordered.get(3);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact2, remote);

        // 3rd iterator loop [artifact3, null]
        local = ordered.get(4);
        Assert.assertNotNull(local);
        Assert.assertEquals(artifact3, local);

        remote = ordered.get(5);
        Assert.assertNull(remote);

        // 4th iterator loop [null, artifact4]
        local = ordered.get(6);
        Assert.assertNull(local);

        remote = ordered.get(7);
        Assert.assertNotNull(remote);
        Assert.assertEquals(artifact4, remote);
    }

    private void orderArtifacts(List<Artifact> ordered, String bucket) {
        BucketSelector bucketSelector = null;
        if (bucket != null) {
            bucketSelector = new BucketSelector(bucket);
        }
        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.artifactDAO,
                                                                    TestUtil.LUSKAN_URI, null, new IncludeArtifacts(),
                                                                    bucketSelector, null) {
                @Override
                void validate(Artifact local, Artifact remoteArtifact) {
                    ordered.add(local);
                    ordered.add(remoteArtifact);
                    logOrdered(local, remoteArtifact);
                }
            };
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

    }

    private Artifact buildArtifact(String uri) {
        return new Artifact(URI.create(uri), TestUtil.getRandomMD5(), new Date(), 1024L);
    }

    private void logArtifact(Artifact artifact, String site) {
        log.info(String.format("%s Artifact - %s", site, artifact.getURI()));
    }

    private void logOrdered(Artifact localArtifact, Artifact remoteArtifact) {
        String local = (localArtifact == null ? "null" : localArtifact.getURI().toString());
        String remote = (remoteArtifact == null ? "null" : remoteArtifact.getURI().toString());
        log.info(String.format("ordered:\nlocal  - %s\nremote - %s", local, remote));
    }

}

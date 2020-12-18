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

package org.opencadc.ratik;

import ca.nrc.cadc.db.DBUtil;
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
import java.util.Calendar;
import java.util.Date;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.UUID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.util.IncludeArtifacts;
import org.springframework.jdbc.core.JdbcTemplate;

public class InventoryValidatorTest {
    private static final Logger log = Logger.getLogger(InventoryValidatorTest.class);

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
    static URI REMOTE_RESOURCEID = URI.create("ivo://cadc.nrc.ca/minoc");

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

    public InventoryValidatorTest() throws Exception {

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

    /**
     * discrepancy: none
     * before: Artifact in L & R
     * after: Artifact in L & R
     */
    @Test
    public void noDiscrepancy_LocalIsStorage() throws Exception {
        noDiscrepancy(false);
    }

    @Test
    public void noDiscrepancyL_LocalIsGlobal() throws Exception {
        noDiscrepancy(true);
    }

    public void noDiscrepancy(boolean trackSiteLocations) throws Exception {
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(),
                                         new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact);
        this.remoteEnvironment.artifactDAO.put(artifact);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig,
                                                                    TestUtil.LUSKAN_URI, new IncludeArtifacts(),
                                                                    null, trackSiteLocations);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertEquals("metaChecksum mismatch", artifact.getMetaChecksum(), localArtifact.getMetaChecksum());
    }

    /** discrepancy: artifact in L && artifact not in R
     *
     * explanation0: filter policy at L changed to exclude artifact in R
     * evidence: Artifact in R without filter
     * action: delete Artifact, if (L==storage) create DeletedStorageLocationEvent
     *
     * before: Artifact in L & R, filter policy to exclude Artifact in R
     * after: Artifact not in L, DeletedStorageLocationEvent in L
     */
    @Test
    public void explanation0_ArtifactInLocal_LocalIsStorage() throws Exception {
        explanation0_ArtifactInLocal(false);
    }

    @Test
    public void explanation0_ArtifactInLocal_LocalIsGlobal() throws Exception {
        explanation0_ArtifactInLocal(true);
    }

    public void explanation0_ArtifactInLocal(boolean trackSiteLocations) throws Exception {
        Artifact artifact = new Artifact(URI.create("cadc:TEST/one.ext"), TestUtil.getRandomMD5(),
                                         new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact);
        this.remoteEnvironment.artifactDAO.put(artifact);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    trackSiteLocations);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNull("local artifact found", localArtifact);

        // L == storage should create a local DeletedStorageLocationEvent, L == global should not.
        DeletedStorageLocationEvent dsle = this.localEnvironment.deletedStorageLocationEventDAO.get(artifact.getID());
        if (trackSiteLocations) {
            Assert.assertNull("DeletedStorageLocationEvent found", dsle);
        } else {
            Assert.assertNotNull("DeletedStorageLocationEvent not found", dsle);
        }
    }

    /** discrepancy: artifact in L && artifact not in R
     *
     * explanation1: deleted from R, pending/missed DeletedArtifactEvent in L
     * evidence: DeletedArtifactEvent in R
     * action: put DAE, delete artifact
     *
     * before: Artifact in L, not in R, DeletedArtifactEvent in R
     * after: Artifact not in L, DeletedArtifactEvent in L
     */
    @Test
    public void explanation1_ArtifactInLocal_LocalIsStorage() throws Exception {
        explanation1_ArtifactInLocal(false);
    }

    @Test
    public void explanation1_ArtifactInLocal_LocalIsGlobal() throws Exception {
        explanation1_ArtifactInLocal(true);
    }

    public void explanation1_ArtifactInLocal(boolean trackSiteLocations) throws Exception {
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(),
                                         new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact);

        DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(artifact.getID());
        this.remoteEnvironment.deletedArtifactEventDAO.put(deletedArtifactEvent);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    trackSiteLocations);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNull("local artifact found", localArtifact);

        DeletedArtifactEvent dae = this.localEnvironment.deletedArtifactEventDAO.get(artifact.getID());
        Assert.assertNotNull("DeletedArtifactEvent not found", dae);
    }

    /** discrepancy: artifact in L && artifact not in R
     *
     * explanation2: L==global, deleted from R, pending/missed DeletedStorageLocationEvent in L
     * evidence: DeletedStorageLocationEvent in R
     * action: remove siteID from Artifact.storageLocations (see note)
     *
     * note: when removing siteID from Artifact.storageLocations in global, if the Artifact.siteLocations becomes empty
     * the artifact should be deleted (metadata-sync needs to also do this in response to a DeletedStorageLocationEvent)
     * TBD: must this also create a DeletedArtifactEvent?
     *
     * L == global
     * case 1
     * before: Artifact in L with R siteID, plus others, in Artifact.siteLocations, not in R, DeletedStorageLocationEvent in R
     * after: Artifact in L, R siteID not in Artifact.siteLocations
     *
     * case 2
     * before: Artifact in L with R siteID in Artifact.siteLocations, not in R, DeletedStorageLocationEvent in R
     * after: Artifact in not in L
     */
    @Test
    public void explanation2_ArtifactInLocal_LocalIsGlobal() throws Exception {
        this.remoteEnvironment.initStorageSite(REMOTE_RESOURCEID);

        // case 1
        UUID remoteSiteID = this.remoteEnvironment.storageSiteDAO.list().iterator().next().getID();
        UUID randomSiteID = UUID.randomUUID();

        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(),
                                         new Date(), 1024L);
        artifact.siteLocations.add(new SiteLocation(remoteSiteID));
        artifact.siteLocations.add(new SiteLocation(randomSiteID));
        this.localEnvironment.artifactDAO.put(artifact);

        DeletedStorageLocationEvent deletedStorageLocationEvent = new DeletedStorageLocationEvent(artifact.getID());
        this.remoteEnvironment.deletedStorageLocationEventDAO.put(deletedStorageLocationEvent);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    true);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertEquals("metaChecksum mismatch", artifact.getMetaChecksum(), localArtifact.getMetaChecksum());
        Assert.assertFalse("artifact contains remote site location",
                           localArtifact.siteLocations.contains(new SiteLocation(remoteSiteID)));
        Assert.assertTrue("artifact missing random site location",
                          localArtifact.siteLocations.contains(new SiteLocation(randomSiteID)));

        // cleanup between tests
        this.localEnvironment.cleanTestEnvironment();
        this.remoteEnvironment.cleanTestEnvironment();
        this.remoteEnvironment.initStorageSite(REMOTE_RESOURCEID);

        // case 2
        remoteSiteID = this.remoteEnvironment.storageSiteDAO.list().iterator().next().getID();
        artifact = new Artifact(URI.create("cadc:INTTEST/two.ext"), TestUtil.getRandomMD5(),
                                new Date(), 1024L);
        artifact.siteLocations.add(new SiteLocation(remoteSiteID));
        this.localEnvironment.artifactDAO.put(artifact);

        deletedStorageLocationEvent = new DeletedStorageLocationEvent(artifact.getID());
        this.remoteEnvironment.deletedStorageLocationEventDAO.put(deletedStorageLocationEvent);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    true);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNull("local artifact found", localArtifact);
    }

    /** discrepancy: artifact in L && artifact not in R
     *
     * explanation3: L==global, new Artifact in L, pending/missed Artifact or sync in R
     * evidence: ?
     * action: remove siteID from Artifact.storageLocations (see below)
     *
     * note: when removing siteID from Artifact.storageLocations in global, if the Artifact.siteLocations becomes empty
     * the artifact should be deleted (metadata-sync needs to also do this in response to a DeletedStorageLocationEvent)
     * TBD: must this also create a DeletedArtifactEvent?
     *
     * L == global
     * test1
     * before: Artifact in L with multiple Artifact.siteLocations, not in R
     * after: Artifact in L, siteID not in Artifact.siteLocations
     *
     * test2
     * before: Artifact in L with R siteID in Artifact.siteLocations, not in R
     * after: Artifact in not in L
     */
    @Test
    public void explanation3_ArtifactInLocal_LocalIsGlobal() throws Exception {
        this.remoteEnvironment.initStorageSite(REMOTE_RESOURCEID);

        // case 1
        UUID remoteSiteID = this.remoteEnvironment.storageSiteDAO.list().iterator().next().getID();
        UUID randomSiteID = UUID.randomUUID();

        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(),
                                         new Date(), 1024L);
        artifact.siteLocations.add(new SiteLocation(remoteSiteID));
        artifact.siteLocations.add(new SiteLocation(randomSiteID));
        this.localEnvironment.artifactDAO.put(artifact);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    true);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertEquals("metaChecksum mismatch", artifact.getMetaChecksum(), localArtifact.getMetaChecksum());
        Assert.assertFalse("artifact contains remote site location",
                           localArtifact.siteLocations.contains(new SiteLocation(remoteSiteID)));
        Assert.assertTrue("artifact missing random site location",
                          localArtifact.siteLocations.contains(new SiteLocation(randomSiteID)));

        // cleanup between tests
        this.localEnvironment.cleanTestEnvironment();
        this.remoteEnvironment.cleanTestEnvironment();
        this.remoteEnvironment.initStorageSite(REMOTE_RESOURCEID);

        // case 2
        remoteSiteID = this.remoteEnvironment.storageSiteDAO.list().iterator().next().getID();
        artifact = new Artifact(URI.create("cadc:INTTEST/two.ext"), TestUtil.getRandomMD5(),
                                new Date(), 1024L);
        artifact.siteLocations.add(new SiteLocation(remoteSiteID));
        this.localEnvironment.artifactDAO.put(artifact);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    true);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNull("local artifact found", localArtifact);
    }

    /** discrepancy: artifact in L && artifact not in R
     *
     * explanation4: L==storage, new Artifact in L, pending/missed new Artifact event in R
     * evidence: ?
     * action: none
     *
     * L == storage
     * before: Artifact in L, not in R
     * after: Artifact in L
     */
    @Test
    public void explanation4_ArtifactInLocal_LocalIsStorage() throws Exception {
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(),
                                         new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    false);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNull("local artifact found", localArtifact);
    }

    /** discrepancy: artifact in L && artifact not in R
     *
     * explanation6: deleted from R, lost DeletedArtifactEvent
     * evidence: ?
     * action: assume explanation3
     *
     * explanation7: L==global, lost DeletedStorageLocationEvent
     * evidence: ?
     * action: assume explanation3
     *
     * explanations 6 and 7 are covered by explanation 3
     * (explanation6 requires that R == storage and hence L == global because
     * at least right now only storage generates DeletedArtifactEvent).
     * The short hand there is that when it says evidence: ? that means with no evidence you
     * cannot tell the difference between the explanations with no evidence: any of them could be true.
     * But the correct action can be taken because it only depends on trackSiteLocations.
     */

    /**
     * explantion0: filter policy at L changed to include artifact in R
     * evidence: ?
     * action: equivalent to missed Artifact event (explanation3 below)
     *
     * explanation 0 is covered by explanation 3.
     */

    /** discrepancy: artifact not in L && artifact in R
     *
     * explanation1: deleted from L, pending/missed DeletedArtifactEvent in R
     * evidence: DeletedArtifactEvent in L
     * action: none
     *
     *  before: Artifact not in L, in R
     *  after: Artifact not in L
     */
    @Test
    public void explanation1_ArtifactNotInLocal_LocalIsStorage() throws Exception {
        explanation1_ArtifactNotInLocal(false);
    }

    @Test
    public void explanation1_ArtifactNotInLocal_LocalIsGlobal() throws Exception {
        explanation1_ArtifactNotInLocal(true);
    }

    public void explanation1_ArtifactNotInLocal(boolean trackSiteLocations) throws Exception {
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(),
                                         new Date(), 1024L);
        this.remoteEnvironment.artifactDAO.put(artifact);

        DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(artifact.getID());
        this.localEnvironment.deletedArtifactEventDAO.put(deletedArtifactEvent);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    trackSiteLocations);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNull("local artifact found", localArtifact);

        DeletedArtifactEvent localDeletedArtifactEvent = this.localEnvironment.deletedArtifactEventDAO.get(artifact.getID());
        Assert.assertNull("found DeletedArtifactEvent", localDeletedArtifactEvent);
    }

    /** discrepancy: artifact not in L && artifact in R
     *
     * explanation2: L==storage, deleted from L, pending/missed DeletedStorageLocationEvent in R
     * evidence: DeletedStorageLocationEvent in L
     * action: none
     *
     * L == storage
     * before: Artifact not in L, in R, DeletedStorageLocationEvent in L
     * after: Artifact not in L, DeletedStorageLocationEvent in L
     */
    @Test
    public void explanation2_ArtifactNotInLocal_LocalIsStorage() throws Exception {
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(),
                                         new Date(), 1024L);
        this.remoteEnvironment.artifactDAO.put(artifact);

        DeletedStorageLocationEvent localDeletedStorageLocationEvent = new DeletedStorageLocationEvent(artifact.getID());
        this.localEnvironment.deletedStorageLocationEventDAO.put(localDeletedStorageLocationEvent);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    false);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNull("local artifact not found", localArtifact);
    }

    /** discrepancy: artifact not in L && artifact in R
     *
     * explanation3: L==storage, new Artifact in R, pending/missed new Artifact event in L
     * evidence: ?
     * action: insert Artifact
     *
     * L == storage
     * before: Artifact not in L, in R
     * after: Artifact in L
     */
    @Test
    public void explanation3_ArtifactNotInLocal_LocalIsStorage() throws Exception {
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(),
                                         new Date(), 1024L);
        this.remoteEnvironment.artifactDAO.put(artifact);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    false);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertEquals("metaChecksum mismatch", artifact.getMetaChecksum(), localArtifact.getMetaChecksum());
    }

    /** discrepancy: artifact not in L && artifact in R
     *
     * explanation4: L==global, new Artifact in R, pending/missed changed Artifact event in L
     * evidence: Artifact in local db but siteLocations does not include remote siteID
     * action: add siteID to Artifact.siteLocations
     *
     * L == global
     * before: Artifact in L & R, R siteID not in L Artifact.siteLocations
     * after:  Artifact in L and R siteID in Artifact.siteLocations
     */
    @Test
    public void explanation4_ArtifactNotInL_LocalIsGlobal() throws Exception {
        this.remoteEnvironment.initStorageSite(REMOTE_RESOURCEID);
        UUID remoteSiteID = this.remoteEnvironment.storageSiteDAO.list().iterator().next().getID();
        UUID randomSiteID = UUID.randomUUID();

        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(),
                                         new Date(), 1024L);
        this.remoteEnvironment.artifactDAO.put(artifact);
        this.localEnvironment.artifactDAO.put(artifact);
        this.localEnvironment.artifactDAO.addSiteLocation(artifact, new SiteLocation(randomSiteID));

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    true);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertEquals("metaChecksum mismatch", artifact.getMetaChecksum(), localArtifact.getMetaChecksum());
        Assert.assertTrue("artifact does not contains remote site location",
                          localArtifact.siteLocations.contains(new SiteLocation(remoteSiteID)));
        Assert.assertTrue("artifact does not contains local site location",
                          localArtifact.siteLocations.contains(new SiteLocation(randomSiteID)));
    }

    /**
     * explanation6: deleted from L, lost DeletedArtifactEvent
     * evidence: ?
     * action: assume explanation3
     *
     * explanation7: L==storage, deleted from L, lost DeletedStorageLocationEvent
     * evidence: ?
     * action: assume explanation3
     *
     * explanations 6 and 7 are covered by explanation 3
     */

    /**
     * discrepancy: artifact.uri in both && artifact.id mismatch
     *
     * explanation1: same ID collision due to race condition that metadata-sync has to handle
     * evidence: no more evidence needed
     * action: pick winner, create DeletedArtifactEvent for loser, delete loser if it is in L
     *
     * case 1:
     * local Artifact contentModifiedDate before remote Artifact contentModifiedDate, delete local Artifact.
     * before: Artifact.uri in L && R, L Artifact.id != R Artifact.id,
     *         L Artifact.contentModifiedDate < R Artifact.contentModifiedDate
     * after:  Artifact not in L, DeletedArtifactEvent in L
     *
     * case 2:
     * local Artifact contentModifiedDate after remote Artifact contentModifiedDate, no action
     * before: Artifact.uri in L && R, L Artifact.id != R Artifact.id,
     *         L Artifact.contentModifiedDate >= R Artifact.contentModifiedDate
     * after:  Artifact in L
     */
    @Test
    public void artifactUriCollision_LocalIsStorage() throws Exception {
        artifactUriCollision(false);
    }

    @Test
    public void artifactUriCollision_LocalIsGlobal() throws Exception {
        artifactUriCollision(true);
    }

    public void artifactUriCollision(boolean trackSiteLocations) throws Exception {
        // case 1: local contentModifiedDate before remote contentModifiedDate
        URI contentCheckSum = TestUtil.getRandomMD5();
        UUID localID = UUID.randomUUID();
        UUID remoteID = UUID.randomUUID();
        Calendar contentModifiedDate = Calendar.getInstance();

        Artifact remoteArtifact = new Artifact(remoteID, URI.create("cadc:INTTEST/one.ext"), contentCheckSum,
                                               contentModifiedDate.getTime(), 1024L);
        this.remoteEnvironment.artifactDAO.put(remoteArtifact);

        contentModifiedDate.roll(Calendar.YEAR, false);
        Artifact localArtifact = new Artifact(localID, URI.create("cadc:INTTEST/one.ext"), contentCheckSum,
                                              contentModifiedDate.getTime(), 1024L);
        this.localEnvironment.artifactDAO.put(localArtifact);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    trackSiteLocations);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        localArtifact = this.localEnvironment.artifactDAO.get(localArtifact.getID());
        Assert.assertNull("local artifact found", localArtifact);

        DeletedArtifactEvent deletedArtifactEvent = this.localEnvironment.deletedArtifactEventDAO.get(localID);
        Assert.assertNotNull("DeletedArtifactEvent not found", deletedArtifactEvent);

        // case 2: local contentModifiedDate after remote contentModifiedDate
        contentModifiedDate = Calendar.getInstance();

        remoteArtifact = new Artifact(remoteID, URI.create("cadc:INTTEST/one.ext"), contentCheckSum,
                                      contentModifiedDate.getTime(), 1024L);
        this.remoteEnvironment.artifactDAO.put(remoteArtifact);

        contentModifiedDate.roll(Calendar.YEAR, true);
        localArtifact = new Artifact(localID, URI.create("cadc:INTTEST/one.ext"), contentCheckSum,
                                     contentModifiedDate.getTime(), 1024L);
        this.localEnvironment.artifactDAO.put(localArtifact);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    trackSiteLocations);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        localArtifact = this.localEnvironment.artifactDAO.get(localArtifact.getID());
        Assert.assertNotNull("local artifact not found", localArtifact);

        deletedArtifactEvent = this.localEnvironment.deletedArtifactEventDAO.get(localID);
        Assert.assertNull("DeletedArtifactEvent found", deletedArtifactEvent);
    }

    /**
     * discrepancy: artifact in both && valid metaChecksum mismatch
     *
     * explanation1: pending/missed artifact update in L
     * evidence: ??
     * action: put Artifact
     *
     * before: Artifact in L & R, update Artifact in R
     * after: R Artifact in L
     *
     * explanation2: pending/missed artifact update in R
     * evidence: ??
     * action: do nothing
     *
     * before: Artifact in L & R, update Artifact in L
     * after: Artifact in L
     */
    @Test
    public void artifactChecksumMismatch_Explanation1_LocalIsStorage() throws Exception {
        artifactChecksumMismatch_Explanation1(false);
    }

    @Test
    public void artifactChecksumMismatch_Explanation1_LocalIsGlobal() throws Exception {
        artifactChecksumMismatch_Explanation1(true);
    }

    public void artifactChecksumMismatch_Explanation1(boolean trackSiteLocations) throws Exception {
        UUID artifactID = UUID.randomUUID();
        URI artifactURI = URI.create("cadc:INTTEST/one.ext");

        Artifact artifact = new Artifact(artifactID, artifactURI, TestUtil.getRandomMD5(),
                                              new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact);
        this.remoteEnvironment.artifactDAO.put(artifact);

        artifact.contentType = "image/fits";
        this.remoteEnvironment.artifactDAO.put(artifact);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    trackSiteLocations);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertSame("local artifact metachecksum does not match",
                          artifact.getMetaChecksum(), localArtifact.getMetaChecksum());
    }

    @Test
    public void artifactChecksumMismatch_Explanation2_LocalIsStorage() throws Exception {
        artifactChecksumMismatch_Explanation2(false);
    }

    @Test
    public void artifactChecksumMismatch_Explanation2_LocalIsGlobal() throws Exception {
        artifactChecksumMismatch_Explanation2(true);
    }

    public void artifactChecksumMismatch_Explanation2(boolean trackSiteLocations) throws Exception {
        UUID artifactID = UUID.randomUUID();
        URI artifactURI = URI.create("cadc:INTTEST/one.ext");

        Artifact artifact = new Artifact(artifactID, artifactURI, TestUtil.getRandomMD5(),
                                         new Date(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact);
        this.remoteEnvironment.artifactDAO.put(artifact);

        artifact.contentType = "image/fits";
        this.localEnvironment.artifactDAO.put(artifact);

        try {
            System.setProperty("user.home", TMP_DIR);
            InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI,
                                                                    new IncludeArtifacts(),null,
                                                                    trackSiteLocations);
            testSubject.run();
        } finally {
            System.setProperty("user.home", USER_HOME);
        }

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getID());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertSame("local artifact metachecksum does not match",
                          artifact.getMetaChecksum(), localArtifact.getMetaChecksum());
    }

}

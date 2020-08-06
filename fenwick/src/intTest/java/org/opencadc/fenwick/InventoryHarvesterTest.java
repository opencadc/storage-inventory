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

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.Log4jInit;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.UUID;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.HarvestState;


/**
 * Integration test for the InventoryHarvester class.
 */
public class InventoryHarvesterTest {

    private static final Logger LOGGER = Logger.getLogger(StorageSiteSyncTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.fenwick", Level.DEBUG);
    }

    private final InventoryEnvironment inventoryEnvironment = new InventoryEnvironment();
    private final LuskanEnvironment luskanEnvironment = new LuskanEnvironment();
    private final Subject testUser = TestUtil.getConfiguredSubject();

    public InventoryHarvesterTest() throws Exception {

    }

    @Before
    public void beforeTest() throws Exception {
        inventoryEnvironment.cleanTestEnvironment();
        luskanEnvironment.cleanTestEnvironment();
    }

    // Tests

    @Test
    public void ensureStorageSiteSync() throws Exception {
        final InventoryHarvester testSubject = new InventoryHarvester(inventoryEnvironment.daoConfig,
                                                                      TestUtil.LUSKAN_URI, new AllArtifacts(),
                                                                      true);
        try {
            Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
                testSubject.doit();
                return null;
            });
            Assert.fail("Should throw IllegalArgumentException for missing storage site.");
        } catch (IllegalStateException illegalStateException) {
            // Good.
            Assert.assertEquals("Wrong message.", "No storage sites available to sync.",
                                illegalStateException.getMessage());
        }
    }

    /**
     * Test scenario:
     * - One Artifact in the inventory database.
     * - Two Artifacts in the Luskan database.
     * - One Deleted Artifact Event in the Luskan database to delete the one Artifact in the inventory database.
     *
     * @throws Exception Any errors.
     */
    @Test
    public void runAllArtifactsNoTrackSiteLocations() throws Exception {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        final Calendar calendar = Calendar.getInstance();
        calendar.set(2007, Calendar.SEPTEMBER, 18, 1, 13, 0);

        final Artifact artifactOne = new Artifact(URI.create("cadc:TEST/fileone.ext"), URI.create("md5:8989"),
                                                  calendar.getTime(), 8989L);
        final URI artifactOneMetaChecksum = artifactOne.computeMetaChecksum(messageDigest);
        luskanEnvironment.artifactDAO.put(artifactOne);

        calendar.set(2012, Calendar.NOVEMBER, 17, 8, 13, 0);

        final Artifact artifactTwo = new Artifact(URI.create("cadc:TEST/filetwo.ext"), URI.create("md5:89898"),
                                                  calendar.getTime(), 89898L);
        messageDigest.reset();
        final URI artifactTwoMetaChecksum = artifactTwo.computeMetaChecksum(messageDigest);
        luskanEnvironment.artifactDAO.put(artifactTwo);

        // Artifact three to be deleted.
        calendar.set(2010, Calendar.JULY, 9, 0, 22, 0);
        final UUID artifactThreeID = UUID.randomUUID();
        final Artifact artifactThree = new Artifact(artifactThreeID,
                                                    URI.create("cadc:TEST/filethree.ext"), URI.create("md5:98989"),
                                                    calendar.getTime(), 98989L);
        inventoryEnvironment.artifactDAO.put(artifactThree);

        final DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(artifactThree.getID());
        luskanEnvironment.deletedArtifactEventDAO.put(deletedArtifactEvent);

        // Run it.

        final InventoryHarvester testSubject = new InventoryHarvester(inventoryEnvironment.daoConfig,
                                                                      TestUtil.LUSKAN_URI, new AllArtifacts(),
                                                                      false);
        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            testSubject.doit();
            return null;
        });

        // End run.

        final Artifact expectedArtifactOne = inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/fileone.ext"));
        Assert.assertTrue("Should have no siteLocations.", expectedArtifactOne.siteLocations.isEmpty());
        Assert.assertEquals("Wrong meta checksum.", artifactOneMetaChecksum,
                            expectedArtifactOne.getMetaChecksum());

        final Artifact expectedArtifactTwo = inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/filetwo.ext"));
        Assert.assertTrue("Should have no siteLocations.", expectedArtifactTwo.siteLocations.isEmpty());
        Assert.assertEquals("Wrong meta checksum.", artifactTwoMetaChecksum,
                            expectedArtifactTwo.getMetaChecksum());

        // Deleted Artifacts should be dealt with first.  So this should not exist.
        Assert.assertNull("Artifact three should not exist.",
                          inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/filethree.ext")));

        final HarvestState artifactHarvestState = inventoryEnvironment.harvestStateDAO.get(Artifact.class.getName(),
                                                                                           TestUtil.LUSKAN_URI);
        Assert.assertEquals("Wrong last modified.", expectedArtifactTwo.getLastModified(),
                            artifactHarvestState.curLastModified);

        final HarvestState deletedArtifactEventHarvestState =
                inventoryEnvironment.harvestStateDAO.get(DeletedArtifactEvent.class.getName(), TestUtil.LUSKAN_URI);
        Assert.assertEquals("Wrong last modified.", deletedArtifactEvent.getLastModified(),
                            deletedArtifactEventHarvestState.curLastModified);

        //
        // Run it again.  It should be idempotent.
        //

        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            testSubject.doit();
            return null;
        });

        final Artifact expectedArtifactOneRound2 = inventoryEnvironment.artifactDAO.get(
                URI.create("cadc:TEST/fileone.ext"));
        Assert.assertTrue("Should have no siteLocations.", expectedArtifactOneRound2.siteLocations.isEmpty());
        Assert.assertEquals("Wrong meta checksum.", artifactOneMetaChecksum,
                            expectedArtifactOneRound2.getMetaChecksum());

        final Artifact expectedArtifactTwoRound2 = inventoryEnvironment.artifactDAO.get(
                URI.create("cadc:TEST/filetwo.ext"));
        Assert.assertTrue("Should have no siteLocations.", expectedArtifactTwoRound2.siteLocations.isEmpty());
        Assert.assertEquals("Wrong meta checksum.", artifactTwoMetaChecksum,
                            expectedArtifactTwoRound2.getMetaChecksum());

        // Deleted Artifacts should be dealt with first.  So this should not exist.
        Assert.assertNull("Artifact three should not exist.",
                          inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/filethree.ext")));

        final HarvestState artifactHarvestStateRound2 = inventoryEnvironment.harvestStateDAO.get(
                Artifact.class.getName(),
                TestUtil.LUSKAN_URI);
        Assert.assertEquals("Wrong last modified.", expectedArtifactTwoRound2.getLastModified(),
                            artifactHarvestStateRound2.curLastModified);

        final HarvestState deletedArtifactEventHarvestStateRound2 =
                inventoryEnvironment.harvestStateDAO.get(DeletedArtifactEvent.class.getName(), TestUtil.LUSKAN_URI);
        Assert.assertEquals("Wrong last modified.", deletedArtifactEvent.getLastModified(),
                            deletedArtifactEventHarvestStateRound2.curLastModified);
    }

    /**
     * Test scenario:
     * - One Artifact in the inventory database.
     * - Two Artifacts in the Luskan database.
     * - One Deleted Artifact Event in the Luskan database to delete the one Artifact in the inventory database.
     * - One Deleted Storage Location event in the Luskan database to remove a storage location for an Artifact.
     *
     * @throws Exception Any errors.
     */
    @Test
    public void runAllArtifactsTrackSiteLocations() throws Exception {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        final Calendar calendar = Calendar.getInstance();

        final UUID storageSiteUUID = UUID.randomUUID();
        luskanEnvironment.storageSiteDAO.put(new StorageSite(storageSiteUUID, URI.create("cadc:TEST/siteone"),
                                                             "Test Site"));

        calendar.set(2007, Calendar.SEPTEMBER, 18, 1, 13, 0);
        final Artifact artifactOne = new Artifact(URI.create("cadc:TEST/fileone.ext"), URI.create("md5:8989"),
                                                  calendar.getTime(), 8989L);
        final URI artifactOneMetaChecksum = artifactOne.computeMetaChecksum(messageDigest);

        luskanEnvironment.artifactDAO.put(artifactOne);

        messageDigest.reset();
        calendar.set(2019, Calendar.AUGUST, 9, 15, 13, 0);
        final URI artifactTwoStorageLocationID = URI.create("cadc:TEST/location/1");
        final Artifact artifactTwo = new Artifact(URI.create("cadc:TEST/filetwo.ext"), URI.create("md5:89898"),
                                                  calendar.getTime(), 89898L);
        artifactTwo.storageLocation = new StorageLocation(artifactTwoStorageLocationID);
        final URI artifactTwoMetaChecksum = artifactTwo.computeMetaChecksum(messageDigest);
        inventoryEnvironment.artifactDAO.put(artifactTwo);
        luskanEnvironment.artifactDAO.put(artifactTwo);

        // Verify the storage location
        Assert.assertEquals("Wrong storage location.", artifactTwo.storageLocation,
                            inventoryEnvironment.artifactDAO.get(artifactTwo.getURI()).storageLocation);

        // Artifact three to be deleted.
        calendar.set(2010, Calendar.JULY, 9, 0, 22, 0);
        final UUID artifactThreeID = UUID.randomUUID();
        final Artifact artifactThree = new Artifact(artifactThreeID,
                                                    URI.create("cadc:TEST/filethree.ext"), URI.create("md5:98989"),
                                                    calendar.getTime(), 98989L);
        inventoryEnvironment.artifactDAO.put(artifactThree);

        final DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(artifactThree.getID());
        luskanEnvironment.deletedArtifactEventDAO.put(deletedArtifactEvent);

        final DeletedStorageLocationEvent deletedStorageLocationEvent =
                new DeletedStorageLocationEvent(artifactTwo.getID());
        luskanEnvironment.deletedStorageLocationEventDAO.put(deletedStorageLocationEvent);

        // Run it.

        final InventoryHarvester testSubject = new InventoryHarvester(inventoryEnvironment.daoConfig,
                                                                      TestUtil.LUSKAN_URI, new AllArtifacts(),
                                                                      true);
        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            testSubject.doit();
            return null;
        });

        // End run.

        final SiteLocation expectedSiteLocation = new SiteLocation(storageSiteUUID);
        final Artifact expectedArtifactOne = inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/fileone.ext"));
        Assert.assertTrue("Should contain expected siteLocations.",
                          expectedArtifactOne.siteLocations.contains(expectedSiteLocation));
        Assert.assertEquals("Wrong meta checksum.", artifactOneMetaChecksum,
                            expectedArtifactOne.getMetaChecksum());
        Assert.assertNull("Should not have a storage location.", expectedArtifactOne.storageLocation);

        final Artifact expectedArtifactTwo = inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/filetwo.ext"));
        Assert.assertTrue("Should contain no siteLocations.",
                          expectedArtifactTwo.siteLocations.isEmpty());
        Assert.assertEquals("Wrong meta checksum.", artifactTwoMetaChecksum,
                            expectedArtifactTwo.getMetaChecksum());
        Assert.assertEquals("Storage Location should match.", artifactTwo.storageLocation,
                            expectedArtifactTwo.storageLocation);

        // Deleted Artifacts should be dealt with first.  So this should not exist.
        Assert.assertNull("Artifact three should not exist.",
                          inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/filethree.ext")));

        final HarvestState artifactHarvestState = inventoryEnvironment.harvestStateDAO.get(Artifact.class.getName(),
                                                                                           TestUtil.LUSKAN_URI);
        Assert.assertEquals("Wrong last modified.", expectedArtifactTwo.getLastModified(),
                            artifactHarvestState.curLastModified);

        final HarvestState deletedArtifactEventHarvestState =
                inventoryEnvironment.harvestStateDAO.get(DeletedArtifactEvent.class.getName(), TestUtil.LUSKAN_URI);
        Assert.assertEquals("Wrong last modified.", deletedArtifactEvent.getLastModified(),
                            deletedArtifactEventHarvestState.curLastModified);

        //
        // Run it again.  It should be idempotent.
        //

        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            testSubject.doit();
            return null;
        });

        final Artifact expectedArtifactOneRound2 =
                inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/fileone.ext"));
        Assert.assertTrue("Should have no siteLocations.",
                          expectedArtifactOneRound2.siteLocations.contains(expectedSiteLocation));
        Assert.assertEquals("Wrong meta checksum.", artifactOneMetaChecksum,
                            expectedArtifactOneRound2.getMetaChecksum());

        final Artifact expectedArtifactTwoRound2 =
                inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/filetwo.ext"));
        Assert.assertTrue("Should have no siteLocations.",
                          expectedArtifactTwoRound2.siteLocations.isEmpty());
        Assert.assertEquals("Wrong meta checksum.", artifactTwoMetaChecksum,
                            expectedArtifactTwoRound2.getMetaChecksum());
        Assert.assertEquals("Storage location should match.", artifactTwo.storageLocation,
                            expectedArtifactTwoRound2.storageLocation);

        // Deleted Artifacts should be dealt with first.  So this should not exist.
        Assert.assertNull("Artifact three should not exist.",
                          inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/filethree.ext")));

        final HarvestState artifactHarvestStateRound2 =
                inventoryEnvironment.harvestStateDAO.get(Artifact.class.getName(), TestUtil.LUSKAN_URI);
        Assert.assertEquals("Wrong last modified.", expectedArtifactTwoRound2.getLastModified(),
                            artifactHarvestStateRound2.curLastModified);

        final HarvestState deletedArtifactEventHarvestStateRound2 =
                inventoryEnvironment.harvestStateDAO.get(DeletedArtifactEvent.class.getName(), TestUtil.LUSKAN_URI);
        Assert.assertEquals("Wrong last modified.", deletedArtifactEvent.getLastModified(),
                            deletedArtifactEventHarvestStateRound2.curLastModified);
    }

    /**
     * Scenario:
     * - Set include directory to something non existent.
     * - Ensure error thrown.
     *
     * @throws Exception Any unexpected errors.
     */
    @Test
    public void runIncludeArtifactsNoTrackSiteLocationsIOError() throws Exception {
        final String oldSetting = System.getProperty("user.home");
        final Path tempLocation = Files.createTempDirectory(InventoryHarvesterTest.class.getName());
        System.setProperty("user.home", tempLocation.toString());
        LOGGER.debug("Now looking for includes in " + System.getProperty("user.home") + "/config/include");

        try {
            final InventoryHarvester testSubject = new InventoryHarvester(inventoryEnvironment.daoConfig,
                                                                          TestUtil.LUSKAN_URI, new IncludeArtifacts(),
                                                                          false);
            Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
                testSubject.doit();
                return null;
            });

            Assert.fail("Should throw IOException.");
        } catch (PrivilegedActionException e) {
            // Good.
            final Throwable baseThrowable = e.getCause();
            Assert.assertEquals("Wrong exception type.", IOException.class, baseThrowable.getClass());
        } finally {
            if (oldSetting != null) {
                System.setProperty("user.home", oldSetting);
            } else {
                System.getProperties().remove("user.home");
            }
        }
    }

    /**
     * Scenario:
     * - Set include directory to something empty.
     * - Ensure error thrown.
     *
     * @throws Exception Any unexpected errors.
     */
    @Test
    public void runIncludeArtifactsNoTrackSiteLocationsNotFoundError() throws Exception {
        final String oldSetting = System.getProperty("user.home");
        final Path tempLocation = Files.createTempDirectory(InventoryHarvesterTest.class.getName());
        System.setProperty("user.home", tempLocation.toString());
        LOGGER.debug("Now looking for includes in " + System.getProperty("user.home") + "/config/include");
        Files.createDirectories(new File(System.getProperty("user.home") + "/config/include").toPath());

        try {
            final InventoryHarvester testSubject = new InventoryHarvester(inventoryEnvironment.daoConfig,
                                                                          TestUtil.LUSKAN_URI, new IncludeArtifacts(),
                                                                          false);
            Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
                testSubject.doit();
                return null;
            });

            Assert.fail("Should throw ResourceNotFoundException.");
        } catch (PrivilegedActionException e) {
            // Good.
            final Throwable baseThrowable = e.getCause();
            Assert.assertEquals("Wrong exception type.", ResourceNotFoundException.class,
                                baseThrowable.getClass());
        } finally {
            if (oldSetting != null) {
                System.setProperty("user.home", oldSetting);
            } else {
                System.getProperties().remove("user.home");
            }
        }
    }

    /**
     * Scenario
     *  - Write an include SQL file
     *  - Add two artifacts to Luskan database
     *  - Include SQL filter should ignore one of the Artifacts.
     * @throws Exception    Any unexpected errors.
     */
    @Test
    public void runIncludeArtifactsNoTrackSiteLocations() throws Exception {
        LOGGER.debug("Looking for includes in " + System.getProperty("user.home") + "/config/include");
        final Path includePath = new File(System.getProperty("user.home") + "/config/include").toPath();
        Files.createDirectories(includePath);

        final File includeFile =
                new File(includePath.toFile(), InventoryHarvesterTest.class.getSimpleName() + "_"
                                               + UUID.randomUUID() + ".sql");
        includeFile.deleteOnExit();

        final FileWriter fileWriter = new FileWriter(includeFile);
        fileWriter.write("WHERE uri LIKE 'cadc:INTTEST%'");
        fileWriter.flush();
        fileWriter.close();

        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        final Calendar calendar = Calendar.getInstance();
        calendar.set(2007, Calendar.SEPTEMBER, 18, 1, 13, 0);

        final Artifact artifactOne = new Artifact(URI.create("cadc:TEST/fileone.ext"), URI.create("md5:8989"),
                                                  calendar.getTime(), 8989L);
        luskanEnvironment.artifactDAO.put(artifactOne);

        calendar.set(2012, Calendar.NOVEMBER, 17, 8, 13, 0);

        final Artifact artifactTwo = new Artifact(URI.create("cadc:INTTEST/filetwo.ext"), URI.create("md5:89898"),
                                                  calendar.getTime(), 89898L);
        final URI artifactTwoMetaChecksum = artifactTwo.computeMetaChecksum(messageDigest);
        luskanEnvironment.artifactDAO.put(artifactTwo);

        final InventoryHarvester testSubject = new InventoryHarvester(inventoryEnvironment.daoConfig,
                                                                      TestUtil.LUSKAN_URI, new IncludeArtifacts(),
                                                                      false);
        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            testSubject.doit();
            return null;
        });

        // Artifact one should not have been included
        Assert.assertNull("Should've ignored Artifact One.",
                          inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/fileone.ext")));

        final Artifact expectedArtifactTwo =
                inventoryEnvironment.artifactDAO.get(URI.create("cadc:INTTEST/filetwo.ext"));
        Assert.assertTrue("Should contain no siteLocations.",
                          expectedArtifactTwo.siteLocations.isEmpty());
        Assert.assertEquals("Wrong meta checksum.", artifactTwoMetaChecksum,
                            expectedArtifactTwo.getMetaChecksum());
        Assert.assertEquals("Storage Location should match.", artifactTwo.storageLocation,
                            expectedArtifactTwo.storageLocation);
    }

    /**
     * Scenario
     *  - Write an include SQL file
     *  - Add three artifacts to Luskan database
     *  - Include SQL filter should ignore one of the Artifacts.
     *  - The siteLocations should be updated in included Artifact(s).
     * @throws Exception    Any unexpected errors.
     */
    @Test
    public void runIncludeArtifactsTrackSiteLocations() throws Exception {
        LOGGER.debug("Looking for includes in " + System.getProperty("user.home") + "/config/include");
        final Path includePath = new File(System.getProperty("user.home") + "/config/include").toPath();
        Files.createDirectories(includePath);

        final File includeFile =
                new File(includePath.toFile(), InventoryHarvesterTest.class.getSimpleName() + "_"
                                               + UUID.randomUUID() + ".sql");
        includeFile.deleteOnExit();
        final FileWriter fileWriter = new FileWriter(includeFile);
        fileWriter.write("WHERE uri LIKE 'cadc:INTTEST%'");
        fileWriter.flush();
        fileWriter.close();

        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        final Calendar calendar = Calendar.getInstance();

        final UUID storageSiteUUID = UUID.randomUUID();
        luskanEnvironment.storageSiteDAO.put(new StorageSite(storageSiteUUID, URI.create("cadc:INTTEST/siteone"),
                                                             "Int Test Site"));

        calendar.set(2007, Calendar.SEPTEMBER, 18, 1, 13, 0);
        final Artifact artifactOne = new Artifact(URI.create("cadc:INTTEST/fileone.ext"), URI.create("md5:8989"),
                                                  calendar.getTime(), 8989L);
        final URI artifactOneMetaChecksum = artifactOne.computeMetaChecksum(messageDigest);
        luskanEnvironment.artifactDAO.put(artifactOne);

        calendar.set(2012, Calendar.NOVEMBER, 17, 8, 13, 0);
        final Artifact artifactTwo = new Artifact(URI.create("cadc:TEST/filetwo.ext"), URI.create("md5:89898"),
                                                  calendar.getTime(), 89898L);
        luskanEnvironment.artifactDAO.put(artifactTwo);

        calendar.set(2009, Calendar.OCTOBER, 31, 21, 0, 0);
        final Artifact artifactThree = new Artifact(URI.create("cadc:INTTEST/filethree.ext"), URI.create("md5:98989"),
                                                  calendar.getTime(), 98989L);
        messageDigest.reset();
        final URI artifactThreeMetaChecksum = artifactThree.computeMetaChecksum(messageDigest);
        luskanEnvironment.artifactDAO.put(artifactThree);

        final InventoryHarvester inventoryHarvester = new InventoryHarvester(inventoryEnvironment.daoConfig,
                                                                             TestUtil.LUSKAN_URI,
                                                                             new IncludeArtifacts(), true);
        Subject.doAs(testUser, (PrivilegedExceptionAction<Object>) () -> {
            inventoryHarvester.doit();
            return null;
        });

        final SiteLocation expectedSiteLocation = new SiteLocation(storageSiteUUID);

        // Artifact one should not have been included
        Assert.assertNull("Should've ignored Artifact Two.",
                          inventoryEnvironment.artifactDAO.get(URI.create("cadc:TEST/filetwo.ext")));

        final Artifact expectedArtifactOne =
                inventoryEnvironment.artifactDAO.get(URI.create("cadc:INTTEST/fileone.ext"));
        Assert.assertTrue("Should contain one siteLocation.",
                          expectedArtifactOne.siteLocations.contains(expectedSiteLocation));
        Assert.assertEquals("Wrong meta checksum.", artifactOneMetaChecksum,
                            expectedArtifactOne.getMetaChecksum());
        Assert.assertEquals("Storage Location should match.", artifactOne.storageLocation,
                            expectedArtifactOne.storageLocation);

        final Artifact expectedArtifactThree =
                inventoryEnvironment.artifactDAO.get(URI.create("cadc:INTTEST/filethree.ext"));
        Assert.assertTrue("Should contain one siteLocation.",
                          expectedArtifactThree.siteLocations.contains(expectedSiteLocation));
        Assert.assertEquals("Wrong meta checksum.", artifactThreeMetaChecksum,
                            expectedArtifactThree.getMetaChecksum());
        Assert.assertEquals("Artifacts should match.", artifactThree, expectedArtifactThree);
    }

    // End tests
}

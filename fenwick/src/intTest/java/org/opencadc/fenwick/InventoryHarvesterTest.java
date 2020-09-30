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

import ca.nrc.cadc.date.DateUtil;
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
import java.text.DateFormat;
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
import org.opencadc.inventory.db.DeletedEventDAO;
import org.opencadc.inventory.db.HarvestState;


/**
 * Integration test for the InventoryHarvester class.
 */
public class InventoryHarvesterTest {

    private static final Logger LOGGER = Logger.getLogger(InventoryHarvesterTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.fenwick", Level.DEBUG);
    }

    private final InventoryEnvironment inventoryEnvironment = new InventoryEnvironment();
    private final LuskanEnvironment luskanEnvironment = new LuskanEnvironment();
    private final Subject testUser = TestUtil.getConfiguredSubject();
    
    private final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

    public InventoryHarvesterTest() throws Exception {

    }

    @Before
    public void beforeTest() throws Exception {
        inventoryEnvironment.cleanTestEnvironment();
        luskanEnvironment.cleanTestEnvironment();
    }

    @Test
    public void testMissingStorageSiteSync() throws Exception {
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
        doAllArtifacts(false);
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
        doAllArtifacts(true);
    }
    
    // track==true: harvest site to global  -- site setup, global asserts
    // track==false: harvest global to site -- global setup, site asserts
    private void doAllArtifacts(boolean track) throws Exception {
        final Calendar calendar = Calendar.getInstance();

        StorageSite expectedSite = new StorageSite(URI.create("cadc:TEST/siteone"), "Test Site", true, false);
        luskanEnvironment.storageSiteDAO.put(expectedSite);
        
        // normal artifact at site1
        calendar.set(2007, Calendar.SEPTEMBER, 18, 1, 13, 0);
        final Artifact artifactOne = new Artifact(URI.create("cadc:TEST/fileone.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 8989L);
        if (track) {
            artifactOne.storageLocation = new StorageLocation(URI.create("cadc:TEST/location/1"));
        }

        luskanEnvironment.artifactDAO.put(artifactOne);
        Thread.sleep(20L);
        
        // normal artifact at site1
        calendar.set(2019, Calendar.AUGUST, 9, 15, 13, 0);
        final Artifact artifactTwo = new Artifact(URI.create("cadc:TEST/filetwo.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 89898L);
        if (track) {
            artifactTwo.storageLocation = new StorageLocation(URI.create("cadc:TEST/location/2"));
        }
        luskanEnvironment.artifactDAO.put(artifactTwo);
        Thread.sleep(20L);

        LOGGER.warn(artifactOne.getURI() + " " + df.format(artifactOne.getLastModified()));
        LOGGER.warn(artifactTwo.getURI() + " " + df.format(artifactTwo.getLastModified()));
        
        // artifact that gets deleted
        calendar.set(2010, Calendar.JULY, 9, 0, 22, 0);
        final Artifact artifactThree = new Artifact(URI.create("cadc:TEST/filethree.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 98989L);
        inventoryEnvironment.artifactDAO.put(artifactThree);
        Thread.sleep(20L);

        final DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(artifactThree.getID());
        luskanEnvironment.deletedArtifactEventDAO.put(deletedArtifactEvent);
        Thread.sleep(20L);
        
        Artifact artifactFour = null;
        DeletedStorageLocationEvent deletedStorageLocationEvent = null;
        if (track) {
            // artifact in global inventory that has site removed
            calendar.set(2015, Calendar.OCTOBER, 9, 1, 2, 3);
            artifactFour = new Artifact(URI.create("cadc:TEST/filefour.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 98989L);
            artifactFour.siteLocations.add(new SiteLocation(expectedSite.getID()));
            //artifactFour.siteLocations.add(new SiteLocation(UUID.randomUUID())); // another site so the artifact is not removed??
            inventoryEnvironment.artifactDAO.put(artifactFour);
            Thread.sleep(20L);

            deletedStorageLocationEvent =  new DeletedStorageLocationEvent(artifactFour.getID());
            luskanEnvironment.deletedStorageLocationEventDAO.put(deletedStorageLocationEvent);
            Thread.sleep(20L);
        }
        
        // Run it.

        final InventoryHarvester testSubject = new InventoryHarvester(inventoryEnvironment.daoConfig, TestUtil.LUSKAN_URI, new AllArtifacts(), track);
        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            testSubject.doit();
            return null;
        });

        // End run.

        final SiteLocation expectedSiteLocation = new SiteLocation(expectedSite.getID());
        
        final Artifact a1 = inventoryEnvironment.artifactDAO.get(artifactOne.getID());
        Assert.assertNotNull(a1);
        Assert.assertEquals("Artifact.uri", artifactOne.getURI(), a1.getURI());
        Assert.assertEquals("Artifact.metaChecksum", artifactOne.getMetaChecksum(), a1.getMetaChecksum());
        if (track) {
            Assert.assertTrue("Artifact.siteLocations", a1.siteLocations.contains(expectedSiteLocation));
        } else {
            Assert.assertTrue("Artifact.siteLocations", a1.siteLocations.isEmpty());
        }
        Assert.assertNull("Artifact.storageLocation", a1.storageLocation);
        
        final Artifact a2 = inventoryEnvironment.artifactDAO.get(artifactTwo.getID());
        Assert.assertEquals("Artifact.uri", artifactTwo.getURI(), a2.getURI());
        Assert.assertEquals("Artifact.metaChecksum", artifactTwo.getMetaChecksum(), a2.getMetaChecksum());
        if (track) {
            Assert.assertTrue("Artifact.siteLocations", a2.siteLocations.contains(expectedSiteLocation));
        } else {
            Assert.assertTrue("Artifact.siteLocations", a1.siteLocations.isEmpty());
        }
        Assert.assertNull("Artifact.storageLocation", a2.storageLocation);
        
        LOGGER.warn(" src: " + artifactOne.getURI() + " " + df.format(artifactOne.getLastModified()));
        LOGGER.warn("dest: " + a1.getURI() + " " + df.format(a1.getLastModified()));
        LOGGER.warn(" src: " + artifactTwo.getURI() + " " + df.format(artifactTwo.getLastModified()));
        LOGGER.warn("dest: " + a2.getURI() + " " + df.format(a2.getLastModified()));
        
        Assert.assertNull("Artifact three should not exist.", inventoryEnvironment.artifactDAO.get(artifactThree.getID()));
        final DeletedArtifactEvent d3 = inventoryEnvironment.deletedArtifactEventDAO.get(artifactThree.getID());
        Assert.assertNotNull("DeletedArtifactEvent", d3);
        
        Artifact a4 = null;
        if (track) {
            a4 = inventoryEnvironment.artifactDAO.get(artifactFour.getID());
            Assert.assertNotNull(a4);
            Assert.assertEquals("Artifact.uri", artifactFour.getURI(), a4.getURI());
            Assert.assertEquals("Artifact.metaChecksum", artifactFour.getMetaChecksum(), a4.getMetaChecksum());
            Assert.assertTrue("Artifact.siteLocations", a4.siteLocations.isEmpty());
            Assert.assertNull("Artifact.storageLocation", a4.storageLocation);
        }
        
        final HarvestState artifactHarvestState = inventoryEnvironment.harvestStateDAO.get(Artifact.class.getName(), TestUtil.LUSKAN_URI);
        Assert.assertEquals("Artifact HarvestState.curlastModified", 
                artifactTwo.getLastModified(), artifactHarvestState.curLastModified);

        final HarvestState deletedArtifactEventState = inventoryEnvironment.harvestStateDAO.get(DeletedArtifactEvent.class.getName(), TestUtil.LUSKAN_URI);
        Assert.assertNotNull(deletedArtifactEventState);
        Assert.assertEquals("DeletedArtifactEventState HarvestState.curlastModified", 
                deletedArtifactEvent.getLastModified(), deletedArtifactEventState.curLastModified);

        HarvestState deletedStorageLocationEventState = null;
        if (track) {
            deletedStorageLocationEventState = 
                    inventoryEnvironment.harvestStateDAO.get(DeletedStorageLocationEvent.class.getName(), TestUtil.LUSKAN_URI);
            Assert.assertNotNull(deletedStorageLocationEventState);
            Assert.assertEquals("DeletedStorageLocationEvent HarvestState.curlastModified", 
                    deletedStorageLocationEvent.getLastModified(), deletedStorageLocationEventState.curLastModified);
        }
        
        //
        // Run it again.  It should be idempotent.
        //

        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            testSubject.doit();
            return null;
        });

        final Artifact a1b = inventoryEnvironment.artifactDAO.get(artifactOne.getID());
        Assert.assertNotNull(a1b);
        Assert.assertEquals("Artifact.uri", artifactOne.getURI(), a1b.getURI());
        Assert.assertEquals("Artifact.metaChecksum", artifactOne.getMetaChecksum(), a1b.getMetaChecksum());
        if (track) {
            Assert.assertTrue("Artifact.siteLocations.", a1b.siteLocations.contains(expectedSiteLocation));
        } else {
            Assert.assertTrue("Artifact.siteLocations", a1.siteLocations.isEmpty());
        }
        Assert.assertNull("Artifact.storageLocation", a1b.storageLocation);

        final Artifact a2b = inventoryEnvironment.artifactDAO.get(artifactTwo.getID());
        Assert.assertEquals("Artifact.uri", artifactTwo.getURI(), a2b.getURI());
        Assert.assertEquals("Artifact.metaChecksum", artifactTwo.getMetaChecksum(), a2b.getMetaChecksum());
        if (track) {
            Assert.assertTrue("Artifact.siteLocations.", a2b.siteLocations.contains(expectedSiteLocation));
        } else {
            Assert.assertTrue("Artifact.siteLocations", a1.siteLocations.isEmpty());
        }
        Assert.assertNull("Artifact.storageLocation", a2b.storageLocation);

        Assert.assertNull("Artifact three should not exist.", inventoryEnvironment.artifactDAO.get(artifactThree.getID()));
        final DeletedArtifactEvent d3b = inventoryEnvironment.deletedArtifactEventDAO.get(artifactThree.getID());
        Assert.assertNotNull("DeletedArtifactEvent", d3b);
        
        if (track) {
            Artifact a4b = inventoryEnvironment.artifactDAO.get(artifactFour.getID());
            Assert.assertNotNull(a4);
            Assert.assertEquals("Artifact.uri", artifactFour.getURI(), a4b.getURI());
            Assert.assertEquals("Artifact.metaChecksum", artifactFour.getMetaChecksum(), a4b.getMetaChecksum());
            Assert.assertTrue("Artifact.siteLocations.", a4b.siteLocations.isEmpty());
            Assert.assertNull("Artifact.storageLocation", a4b.storageLocation);
        }

        // verify that harvest state did not change again
        
        final HarvestState artifactHarvestState2 =
                inventoryEnvironment.harvestStateDAO.get(Artifact.class.getName(), TestUtil.LUSKAN_URI);
        Assert.assertEquals("Artifact HarvestState.curlastModified", 
                artifactHarvestState.curLastModified, artifactHarvestState2.curLastModified);

        final HarvestState deletedArtifactEventHarvestState2 =
                inventoryEnvironment.harvestStateDAO.get(DeletedArtifactEvent.class.getName(), TestUtil.LUSKAN_URI);
        Assert.assertEquals("DeletedArtifactEvent HarvestState.curLastModified", 
                deletedArtifactEventState.curLastModified, deletedArtifactEventHarvestState2.curLastModified);
        
        if (track) {
            final HarvestState deletedStorageLocationEventState2 = 
                inventoryEnvironment.harvestStateDAO.get(DeletedStorageLocationEvent.class.getName(), TestUtil.LUSKAN_URI);
            Assert.assertEquals("DeletedStorageLocationEvent HarvestState.curLastModified", 
                    deletedStorageLocationEventState.curLastModified, deletedStorageLocationEventState2.curLastModified);
        }
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
    public void runIncludeArtifacts() throws Exception {
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
        final Artifact artifactOne = new Artifact(URI.create("cadc:TEST/fileone.ext"), 
                TestUtil.getRandomMD5(), calendar.getTime(), 8989L);
        luskanEnvironment.artifactDAO.put(artifactOne);

        calendar.set(2012, Calendar.NOVEMBER, 17, 8, 13, 0);
        final Artifact artifactTwo = new Artifact(URI.create("cadc:INTTEST/filetwo.ext"), 
                TestUtil.getRandomMD5(), calendar.getTime(), 89898L);
        luskanEnvironment.artifactDAO.put(artifactTwo);

        final InventoryHarvester testSubject = new InventoryHarvester(inventoryEnvironment.daoConfig,
                                                                      TestUtil.LUSKAN_URI, new IncludeArtifacts(),
                                                                      false);
        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            testSubject.doit();
            return null;
        });

        Assert.assertNull("not included", inventoryEnvironment.artifactDAO.get(artifactOne.getID()));

        Assert.assertNotNull("included", inventoryEnvironment.artifactDAO.get(artifactTwo.getID()));
    }
}

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

package org.opencadc.fenwick;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.inventory.util.AllArtifacts;
import org.opencadc.inventory.util.IncludeArtifacts;

/**
 *
 * @author pdowler
 */
public class ArtifactSyncTest {
    private static final Logger log = Logger.getLogger(ArtifactSyncTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.fenwick", Level.INFO);
    }

    private final InventoryEnvironment inventoryEnvironment = new InventoryEnvironment();
    private final LuskanEnvironment luskanEnvironment = new LuskanEnvironment();
    private final Subject testUser = TestUtil.getConfiguredSubject();
    
    public ArtifactSyncTest() throws Exception { 
    }
    
    @Before
    public void beforeTest() throws Exception {
        inventoryEnvironment.cleanTestEnvironment();
        luskanEnvironment.cleanTestEnvironment();
    }
    
    @Test
    public void runAllArtifactsNoTrackSiteLocations() throws Exception {
        doAllArtifacts(false);
    }

    @Test
    public void runAllArtifactsTrackSiteLocations() throws Exception {
        doAllArtifacts(true);
    }
    
    // track==true:  harvest site to global -- site setup, global asserts
    // track==false: harvest global to site -- global setup, site asserts
    //
    // 
    private void doAllArtifacts(boolean track) throws Exception {
        final StorageSite site1 = new StorageSite(URI.create("cadc:TEST/site1"), "Test Site", true, false);
        final SiteLocation sloc1 = new SiteLocation(site1.getID());
        final SiteLocation sloc2 = new SiteLocation(UUID.randomUUID());

        // create and execute with no source content
        StorageSite trackSite = null;
        if (track) {
            trackSite = site1;
        }
        final ArtifactSync sync = new ArtifactSync(inventoryEnvironment.artifactDAO, TestUtil.LUSKAN_URI, 6, 6, new AllArtifacts(), trackSite);
        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            sync.doit();
            return null;
        });
        
        // source artifacts
        long t = System.currentTimeMillis();
        final Artifact a1 = new Artifact(URI.create("cadc:TEST/a1"), TestUtil.getRandomMD5(), new Date(t - 300), 8989L);
        final Artifact a2 = new Artifact(URI.create("cadc:TEST/a2"), TestUtil.getRandomMD5(), new Date(t - 200), 8989L);
        final Artifact a3 = new Artifact(URI.create("cadc:TEST/a3"), TestUtil.getRandomMD5(), new Date(t - 100), 8989L);
        
        StorageLocation storLoc2 = new StorageLocation(URI.create("cadc:TEST/location/2"));
        if (track) {
            a1.storageLocation = new StorageLocation(URI.create("cadc:TEST/location/1"));
            a2.storageLocation = storLoc2;
            a3.storageLocation = new StorageLocation(URI.create("cadc:TEST/location/3"));
        }

        luskanEnvironment.artifactDAO.put(a1);
        Thread.sleep(6L);
        luskanEnvironment.artifactDAO.put(a2);
        Thread.sleep(6L);
        luskanEnvironment.artifactDAO.put(a3);
        Thread.sleep(6L);
        
        if (track) {
            // global: check merge new SiteLocation with a2
            a2.storageLocation = null;
            a2.siteLocations.add(sloc2);
            inventoryEnvironment.artifactDAO.put(a2);
        } else {
            // storage: check preserve storageLocation with a2
            a2.storageLocation = new StorageLocation(URI.create("cadc:TEST/location/2"));
            inventoryEnvironment.artifactDAO.put(a2);
        }
        
        // execute again
        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            sync.doit();
            return null;
        });
        
        // verify: a1,a2,a3 synced
        // a2: merge new SiteLocation or preserve StorageLocation
        Artifact actual1 = inventoryEnvironment.artifactDAO.get(a1.getID());
        Assert.assertNotNull(actual1);
        Assert.assertEquals(a1.getMetaChecksum(), actual1.getMetaChecksum());
        if (track) {
            Assert.assertEquals(1, actual1.siteLocations.size());
            Assert.assertTrue(actual1.siteLocations.contains(sloc1));
            
            // global increments lastModified
            Assert.assertTrue(actual1.getLastModified().after(a1.getLastModified()));
        } else {
            Assert.assertTrue(actual1.siteLocations.isEmpty());
            
            Assert.assertEquals(a1.getLastModified(), actual1.getLastModified());
        }
        
        Artifact actual2 = inventoryEnvironment.artifactDAO.get(a2.getID());
        Assert.assertNotNull(actual2);
        Assert.assertEquals(a2.getMetaChecksum(), actual2.getMetaChecksum());
        if (track) {
            Assert.assertEquals(2, actual2.siteLocations.size());
            Assert.assertTrue(actual2.siteLocations.contains(sloc1));
            Assert.assertTrue(actual2.siteLocations.contains(sloc2));
            
            // global increments lastModified
            Assert.assertTrue(actual2.getLastModified().after(a2.getLastModified()));
        } else {
            Assert.assertTrue(actual2.siteLocations.isEmpty());
            Assert.assertEquals(storLoc2, actual2.storageLocation);
            
            Assert.assertEquals(a2.getLastModified(), actual2.getLastModified());
        }
        
        Artifact actual3 = inventoryEnvironment.artifactDAO.get(a3.getID());
        Assert.assertNotNull(actual3);
        Assert.assertEquals(a3.getMetaChecksum(), actual3.getMetaChecksum());
        if (track) {
            Assert.assertEquals(1, actual3.siteLocations.size());
            Assert.assertTrue(actual3.siteLocations.contains(sloc1));
            
            // global increments lastModified
            Assert.assertTrue(actual3.getLastModified().after(a3.getLastModified()));
        } else {
            Assert.assertTrue(actual3.siteLocations.isEmpty());
            
            Assert.assertEquals(a3.getLastModified(), actual3.getLastModified());
        }
        
        HarvestState hs = inventoryEnvironment.harvestStateDAO.get(Artifact.class.getName(), TestUtil.LUSKAN_URI);
        Assert.assertNotNull(hs);
        Assert.assertEquals(a3.getID(), hs.curID);
        Assert.assertEquals(a3.getLastModified(), hs.curLastModified);
        
        log.info("Run it again.  It should be idempotent...");

        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            sync.doit();
            return null;
        });

        hs = inventoryEnvironment.harvestStateDAO.get(Artifact.class.getName(), TestUtil.LUSKAN_URI);
        Assert.assertNotNull(hs);
        Assert.assertEquals(a3.getID(), hs.curID);
        Assert.assertEquals(a3.getLastModified(), hs.curLastModified);
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
        final Path includePath = new File("build/tmp/config/").toPath();
        Files.createDirectories(includePath);

        final File includeFile = new File(includePath.toFile(), "artifact-filter.sql");
        includeFile.deleteOnExit();

        final FileWriter fileWriter = new FileWriter(includeFile);
        fileWriter.write("WHERE uri LIKE 'cadc:INTTEST%'");
        fileWriter.flush();
        fileWriter.close();

        final Calendar calendar = Calendar.getInstance();
        calendar.set(2007, Calendar.SEPTEMBER, 18, 1, 13, 0);
        final Artifact artifactOne = new Artifact(URI.create("cadc:TEST/fileone.ext"), 
                TestUtil.getRandomMD5(), calendar.getTime(), 8989L);
        artifactOne.storageLocation = new StorageLocation(URI.create("cadc:TEST/location/1"));
        luskanEnvironment.artifactDAO.put(artifactOne);
        Thread.sleep(50L);

        calendar.set(2012, Calendar.NOVEMBER, 17, 8, 13, 0);
        final Artifact artifactTwo = new Artifact(URI.create("cadc:INTTEST/filetwo.ext"), 
                TestUtil.getRandomMD5(), calendar.getTime(), 89898L);
        artifactTwo.storageLocation = new StorageLocation(URI.create("cadc:INTTEST/location/2"));
        luskanEnvironment.artifactDAO.put(artifactTwo);

        final ArtifactSync testSubject = new ArtifactSync(inventoryEnvironment.artifactDAO, 
                TestUtil.LUSKAN_URI, 6, 6, new IncludeArtifacts(includeFile), null);
        
        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            testSubject.doit();
            return null;
        });

        Assert.assertNull("not included", inventoryEnvironment.artifactDAO.get(artifactOne.getID()));

        Assert.assertNotNull("included", inventoryEnvironment.artifactDAO.get(artifactTwo.getID()));
    }
    
    @Test
    public void testCollisions() throws Exception {
        log.info("testCollisions - START");
        
        StorageSite expectedSite = new StorageSite(URI.create("cadc:TEST/siteone"), "Test Site", true, false);
        luskanEnvironment.storageSiteDAO.put(expectedSite);
        
        // create an Artifact.uri collision where existing artifact is newer and harvested gets skipped
        final Artifact artifactCollision1 = new Artifact(URI.create("cadc:TEST/collision1"), TestUtil.getRandomMD5(), new Date(), 78787L);
        artifactCollision1.storageLocation = new StorageLocation(URI.create("foo:bar/baz"));
        luskanEnvironment.artifactDAO.put(artifactCollision1);
        
        Date newerCLM = new Date(artifactCollision1.getContentLastModified().getTime() + 20000L);
        final Artifact artifactCollisionKeeper1 = new Artifact(URI.create("cadc:TEST/collision1"), TestUtil.getRandomMD5(), newerCLM, 78787L);
        inventoryEnvironment.artifactDAO.put(artifactCollisionKeeper1);
        Thread.sleep(20L);
        
        // create an Artifact.uri collision where existing artifact is older and harvested gets applied
        final Artifact artifactCollisionKeeper2 = new Artifact(URI.create("cadc:TEST/collision2"), TestUtil.getRandomMD5(), new Date(), 78787L);
        artifactCollisionKeeper2.storageLocation = new StorageLocation(URI.create("foo:bar/baz2"));
        luskanEnvironment.artifactDAO.put(artifactCollisionKeeper2);
        
        Date olderCLM = new Date(artifactCollisionKeeper2.getContentLastModified().getTime() - 20000L);
        final Artifact artifactCollision2 = new Artifact(URI.create("cadc:TEST/collision2"), TestUtil.getRandomMD5(), olderCLM, 78787L);
        luskanEnvironment.artifactDAO.put(artifactCollisionKeeper2);
        inventoryEnvironment.artifactDAO.put(artifactCollision2);
        Thread.sleep(20L);
        
        final ArtifactSync testSubject = new ArtifactSync(inventoryEnvironment.artifactDAO, 
                TestUtil.LUSKAN_URI, 6, 6, new AllArtifacts(), expectedSite);
        
        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            testSubject.doit();
            return null;
        });
        
        // artifactCollision1 vs artifactCollisionKeeper1
        Assert.assertNull("artifactCollision1 should have been skipped", 
                inventoryEnvironment.artifactDAO.get(artifactCollision1.getID()));
        Assert.assertNotNull("artifactCollisionKeeper1 should have been retained", 
                inventoryEnvironment.artifactDAO.get(artifactCollisionKeeper1.getID()));
        
        // artifactCollision2 vs artifactCollisionKeeper2
        Assert.assertNull("artifactCollision2 should have been removed", 
                inventoryEnvironment.artifactDAO.get(artifactCollision2.getID()));
        Assert.assertNotNull("artifactCollision2 DeletedArtifactEvent should have been created", 
                inventoryEnvironment.deletedArtifactEventDAO.get(artifactCollision2.getID()));
        Assert.assertNotNull("artifactCollisionKeeper2 should have been retained", 
                inventoryEnvironment.artifactDAO.get(artifactCollisionKeeper2.getID()));
        
        log.info("testCollisions - IDEMPOTENT");
        
        Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
            testSubject.doit();
            return null;
        });
        
        log.info("testCollisions - DONE");
    }
}

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
 ************************************************************************
 */

package org.opencadc.critwall;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.PrivilegedAction;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter;
import org.opencadc.inventory.util.BucketSelector;


public class FileSyncTest {
    private static final Logger log = Logger.getLogger(FileSyncTest.class);
    private static final String TEST_ROOT = "build/tmp/fsroot/critwallTests";
    private static final String TEST_RESOURCE_ID = "ivo://cadc.nrc.ca/data";
    private static Map<String,Object> daoConfig;
    private static ConnectionConfig cc;

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.critwall", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.storage.fs", Level.INFO);
    }

    private OpaqueFileSystemStorageAdapter oStorageAdapter;
    private ArtifactDAO dao = new ArtifactDAO();
    private ArtifactDAO jobDao = new ArtifactDAO();
    private Subject anonSubject = AuthenticationUtil.getAnonSubject();

    public FileSyncTest() throws Exception {
        daoConfig = new TreeMap<String,Object>();
        try {
            DBConfig dbrc = new DBConfig();
            cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
            DBUtil.createJNDIDataSource("jdbc/FileSyncTest", cc);

            daoConfig.put(SQLGenerator.class.getName(), SQLGenerator.class);
            daoConfig.put("jndiDataSourceName", "jdbc/FileSyncTest");
            daoConfig.put("database", TestUtil.DATABASE);
            daoConfig.put("schema", TestUtil.SCHEMA);
            dao.setConfig(daoConfig);

            String testDir = TEST_ROOT + File.separator + "testFileSync";

            // Create the test directory the fs storage adapter needs
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrw-");
            FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
            Files.createDirectories(Paths.get(testDir), attr);

        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw ex;
        }

    }

    // todo: consider moving this to s shared parent function so FileSyncJob can use it as well.
    private void wipe_clean(Iterator<Artifact> artifactIterator) {
        log.debug("wipe_clean running...");
        int deletedArtifacts = 0;

        while (artifactIterator.hasNext()) {
            log.debug("something to remove...");
            Artifact a = artifactIterator.next();
            log.debug("deleting test uri: " + a.getURI() + " ID: " + a.getID());
            dao.delete(a.getID());
            deletedArtifacts++;
        }

        if (deletedArtifacts > 0) {
            log.info("deleted " + deletedArtifacts+ " artifacts");
        }
    }

    @Before
    public void cleanTestEnvironment() throws Exception {
        log.debug("cleaning stored artifacts...");
        Iterator<Artifact> storedArtifacts = dao.storedIterator(null);
        log.debug("got an iterator back: " + storedArtifacts);
        wipe_clean(storedArtifacts);

        log.debug("cleaning unstored artifacts...");
        Iterator<Artifact> unstoredArtifacts = dao.unstoredIterator(null);
        log.debug("got an iterator back: " + storedArtifacts);
        wipe_clean(unstoredArtifacts);

        // Clean up critwall tests in fsroot
        log.debug("deleting contents of test directories in: " + TEST_ROOT);

        oStorageAdapter = new OpaqueFileSystemStorageAdapter(new File(TEST_ROOT), 1);
        Iterator<StorageMetadata> iter = oStorageAdapter.iterator();

        while (iter.hasNext()) {
            StorageMetadata sm = iter.next();
            log.debug("deleting storage location: " + sm.getStorageLocation());
            oStorageAdapter.delete(sm.getStorageLocation());
        }
    }

    private void createTestDirectory(String testDirectory) throws IOException {
        // Create the test directory the fs storage adapter needs
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrw-");
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
        Files.createDirectories(Paths.get(testDirectory), attr);
        log.debug("created test dir: " + testDirectory);
    }

    private void createTestMetadata(TreeMap<Integer,Artifact> artifactMap) {
        for(Artifact arti : artifactMap.values()) {
            dao.put(arti);
            log.debug("putting test artifact " + arti.getURI() + " to database");
        }
    }


    private TreeMap<Integer, Artifact> mkSmallDataset() {
        TreeMap<Integer, Artifact> artifactMap = new TreeMap<Integer, Artifact>();
        try {
            // set up 4-5 IRIS artifacts need: checksum, size, artifact uri.
            Artifact testArtifact = new Artifact(new URI("ad:IRIS/I429B4H0.fits"),
                new URI("md5:e3922d47243563529f387ebdf00b66da"), new Date(),
                1008000L);
            log.debug("putting test artifact 1 to database");
            artifactMap.put(1, testArtifact);

            testArtifact = new Artifact(new URI("ad:IRIS/f429h000_preview_1024.png"),
                new URI("md5:ffdd0ee84d648035b0b4bd8eac6c849e"), new Date(),
                471897L);

            log.debug("putting test artifact 2 to database");
            artifactMap.put(2, testArtifact);

            testArtifact = new Artifact(new URI("ad:IRIS/I426B4H0.fits"),
                new URI("md5:fcd1402ee90e52b49e6624117ab79871"), new Date(),
                1008000L);
            log.debug("putting test artifact 3 to database");
            artifactMap.put(3, testArtifact);

            testArtifact = new Artifact(new URI("ad:IRIS/f426h000_preview_256.png"),
                new URI("md5:e8ded674e62d623d8bacab66215c1361"), new Date(),
                113116L);
            log.debug("putting test artifact 4 to database");
            artifactMap.put(4, testArtifact);
        } catch (URISyntaxException uriex) {
            Assert.fail("URI error: " + uriex);
        }

        return artifactMap;
    }

    private TreeMap<Integer,Artifact> mkLargeDataset() {
        log.info("making 16 element dataset");
        // Used in the different test suites to pass to the function that populates
        // the test database.
        TreeMap<Integer,Artifact> artifactMap = new TreeMap<Integer, Artifact>();
        try {
            // generate a 4-item map
            artifactMap = mkSmallDataset();

            // Build on the small dataset
            //            IRAS-25um
            //            ad:IRIS/I422B4H0.fits ; md5:fd02b367a37f1ec989be20be40672fc5; 1008000
            artifactMap.put(5, new Artifact(new URI("ad:IRIS/I422B4H0.fits"),
                new URI("md5:fd02b367a37f1ec989be20be40672fc5"), new Date(),
                1008000L));

            //            ad:IRIS/I422B1H0.fits; md5:bcaa00dd3c18332d60d2f25ba31030fb; 1008000
            artifactMap.put(6, new Artifact(new URI("ad:IRIS/I422B1H0.fits"),
                new URI("md5:bcaa00dd3c18332d60d2f25ba31030fb"), new Date(),
                1008000L));

            //            ad:IRIS/I422B2H0.fits; md5:e7abb9f19fc3a28cdd926dd1e74abec7; 1008000
            artifactMap.put(7, new Artifact(new URI("ad:IRIS/I422B2H0.fits"),
                new URI("md5:e7abb9f19fc3a28cdd926dd1e74abec7"), new Date(),
                1008000L));

            //            ad:IRIS/I422B3H0.fits; md5:2cb35c6af7e7e4df1aac3e91d4a17628; 1008000
            artifactMap.put(8, new Artifact(new URI("ad:IRIS/I422B3H0.fits"),
                new URI("md5:2cb35c6af7e7e4df1aac3e91d4a17628"), new Date(),
                1008000L));

            //            ad:IRIS/I421B4H0.fits; md5:fd2f33f79aefc15a84f5dbfb6fa61179; 1008000;
            artifactMap.put(9, new Artifact(new URI("ad:IRIS/I421B4H0.fits"),
                new URI("md5:fd2f33f79aefc15a84f5dbfb6fa61179"), new Date(),
                1008000L));

            //            ad:IRIS/I421B1H0.fits; md5:e188c72cec7466e77371687eacef4188; 1008000;
            artifactMap.put(10, new Artifact(new URI("ad:IRIS/I421B1H0.fits"),
                new URI("md5:e188c72cec7466e77371687eacef4188"), new Date(),
                1008000L));

            //            ad:IRIS/I421B2H0.fits; md5:50959fa149de2850f94513f3d5c1b2d8; 1008000;
            artifactMap.put(11, new Artifact(new URI("ad:IRIS/I421B2H0.fits"),
                new URI("md5:50959fa149de2850f94513f3d5c1b2d8"), new Date(),
                1008000L));

            //            ad:IRIS/I421B3H0.fits; md5:e2617db27776cdf97840f727d9411dc0; 1008000;
            artifactMap.put(12, new Artifact(new URI("ad:IRIS/I421B3H0.fits"),
                new URI("md5:e2617db27776cdf97840f727d9411dc0"), new Date(),
                1008000L));

            //            ad:IRIS/I420B4H0.fits; md5:29e647300f3fbb46d4406d1957f7f61c; 1008000;
            artifactMap.put(13, new Artifact(new URI("ad:IRIS/I420B4H0.fits"),
                new URI("md5:29e647300f3fbb46d4406d1957f7f61c"), new Date(),
                1008000L));

            //            ad:IRIS/I420B1H0.fits; md5:7c6bfc10301fb617ce95dc338448cd03;1008000;
            artifactMap.put(14, new Artifact(new URI("ad:IRIS/I420B1H0.fits"),
                new URI("md5:7c6bfc10301fb617ce95dc338448cd03"), new Date(),
                1008000L));

            //            ad:IRIS/I420B2H0.fits; md5:5a4b9bb5dfc64ba0564ece68821b7ea1; 1008000;
            artifactMap.put(15, new Artifact(new URI("ad:IRIS/I420B2H0.fits"),
                new URI("md5:5a4b9bb5dfc64ba0564ece68821b7ea1"), new Date(),
                1008000L));

            //            ad:IRIS/I420B3H0.fits; md5:65e71fad7cdbcb644922b89bc735f709; 1008000;
            artifactMap.put(16, new Artifact(new URI("ad:IRIS/I420B3H0.fits"),
                new URI("md5:65e71fad7cdbcb644922b89bc735f709"), new Date(),
                1008000L));

            log.info("done making 16 element dataset");

        } catch (URISyntaxException uriex) {
            Assert.fail("URI error: " + uriex);
        }

        return artifactMap;
    }


    public void fileSyncTestBody(int nthreads, String testDir, TreeMap<Integer,Artifact> artMap) throws Exception {

        createTestMetadata(artMap);
        log.info("test metadata put to database");

        OpaqueFileSystemStorageAdapter localStorage = new OpaqueFileSystemStorageAdapter(new File(testDir), 1);
        log.debug("created storage adapter for test dir.");

        log.debug("nthreads: " + nthreads);

        // Can't count on computed bucket, so use entire range to cover all
        String goodRange = "0-f";
        BucketSelector bucketSel = new BucketSelector(goodRange);
        log.debug("bucket selector: " + bucketSel);

        URI locatorResourceID = new URI(TEST_RESOURCE_ID);

        log.info("FileSync: START");
        FileSync fs = new FileSync(daoConfig, cc, localStorage, locatorResourceID, bucketSel, nthreads);
        fs.testRunLoops = 1;
        Subject.doAs(anonSubject, (PrivilegedAction<Object>) () -> {
            fs.doit();
            return null;
        });
        log.info("FileSync: DONE");
        
        // Check the storage locations, as these should get updated within a reasonable
        // amount of timeA
        Iterator artIter = dao.unstoredIterator(null);

        while (artIter.hasNext()) {
            log.debug("waiting for file sync jobs to update database");
            Thread.sleep(1000);
            artIter = dao.unstoredIterator(null);
        }

        // check job succeeded by checking storage locations of at least one of
        // the artifacts from the map are not nullA
        // for one or two of the artifacts

        for (Artifact arti : artMap.values()) {
            Artifact thirdStoredArtifact = dao.get(arti.getURI());
            log.debug("stored artifact: " + thirdStoredArtifact);
            log.debug("stored artifact: " + thirdStoredArtifact.storageLocation);
            Assert.assertNotNull("storage location of artifact should not be null.", thirdStoredArtifact.storageLocation);

            // check for file on disk, throw away the bytes
            FileOutputStream dest = new FileOutputStream("/dev/null");
            log.debug("shunting to /dev/null");
            localStorage.get(thirdStoredArtifact.storageLocation, dest);
        }

    }

    @Test
    public void testValidFileSyncSmallSet1() {
        String testDir = TEST_ROOT + File.separator + "testValidFileSync";

        try {
            createTestDirectory(testDir);
            TreeMap<Integer, Artifact> work = mkSmallDataset();
            fileSyncTestBody(1, testDir, work);


        } catch (Exception unexpected) {
            Assert.fail("unexpected exception: " + unexpected);
            log.debug(unexpected);
        }
        log.info("testValidFileSync - DONE");
    }
    
    @Test
    public void testValidFileSyncSmallSet4() {
        String testDir = TEST_ROOT + File.separator + "testValidFileSync";

        try {
            createTestDirectory(testDir);
            TreeMap<Integer, Artifact> work = mkSmallDataset();
            fileSyncTestBody(1, testDir, work);


        } catch (Exception unexpected) {
            Assert.fail("unexpected exception: " + unexpected);
            log.debug(unexpected);
        }
        log.info("testValidFileSync - DONE");
    }

    @Test
    public void testValidFileSyncLargeSet2() {
        String testDir = TEST_ROOT + File.separator + "testValidFileSyncLargeSet";

        try {
            createTestDirectory(testDir);
            TreeMap<Integer, Artifact> work = mkLargeDataset();
            fileSyncTestBody(2, testDir, work);

        } catch (Exception unexpected) {
            Assert.fail("unexpected exception: " + unexpected);
            log.debug(unexpected);
        }
        log.info("testValidFileSyncLargeSet - DONE");
    }

    @Test
    public void testRetryOnRepeatedQuery() {
        String testDir = TEST_ROOT + File.separator + "testRetryOnRepeatedQuery";
        try {
            createTestDirectory(testDir);
            Artifact testArtifact = new Artifact(new URI("ad:IRIS/no-such-file.fits"),
                new URI("md5:e3922d47243563529f387ebdf00b66da"), new Date(),
                1008000L);
            dao.put(testArtifact);
            
            OpaqueFileSystemStorageAdapter localStorage = new OpaqueFileSystemStorageAdapter(new File(testDir), 1);
            BucketSelector bucketSel = new BucketSelector(testArtifact.getBucket().substring(0,1));
            URI locatorResourceID = new URI(TEST_RESOURCE_ID);

            // make sure FileSyncJob actually fails to update
            final FileSync fs = new FileSync(daoConfig, cc, localStorage, locatorResourceID, bucketSel, 1);
            fs.testRunLoops = 1;
            Subject.doAs(anonSubject, (PrivilegedAction<Object>) () -> {
                fs.doit();
                return null;
            });
            
            boolean found = false;
            int num = 0;
            try (ResourceIterator<Artifact> iter = dao.unstoredIterator(testArtifact.getBucket())) {
                while (iter.hasNext()) {
                    Artifact unstored = iter.next();
                    num++;
                    if (testArtifact.getID().equals(unstored.getID())) {
                        found = true;
                    }
                }
            }
            Assert.assertTrue(found);
            Assert.assertEquals(1, num);
            
            // now with loops
            final FileSync fs2 = new FileSync(daoConfig, cc, localStorage, locatorResourceID, bucketSel, 1);
            fs2.testRunLoops = 4; 
            Subject.doAs(anonSubject, (PrivilegedAction<Object>) () -> {
                fs2.doit();
                return null;
            });
            
        } catch (Exception unexpected) {
            log.info("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
            
        }
        log.info("testValidFileSyncLargeSet - DONE");
    }
}

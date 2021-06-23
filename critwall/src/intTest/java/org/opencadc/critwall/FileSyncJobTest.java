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
 ************************************************************************
 */

package org.opencadc.critwall;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
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
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter;

public class FileSyncJobTest {
    private static final Logger log = Logger.getLogger(FileSyncJobTest.class);
    private static final String TEST_ROOT = "build/tmp/fsroot/critwallTests";
    private static final String TEST_ARTIFACT_URI = "cadc:IRIS/I212B2H0.fits";
    private static final String TEST_ARTIFACT_NOT_FOUND = "cadc:IRIS/NotFound.fits";
    private static final String TEST_RESOURCE_ID = "ivo://cadc.nrc.ca/global/raven";
    private static final String CERTIFICATE_FILE = "critwall-test.pem";

    private OpaqueFileSystemStorageAdapter oa = null;

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.critwall", Level.DEBUG);
        Log4jInit.setLevel("org.opencadc.inventory.storage.fs", Level.INFO);
    }

    private ArtifactDAO dao = new ArtifactDAO();
    private Subject anonSubject = AuthenticationUtil.getAnonSubject();

    public FileSyncJobTest() throws Exception {
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
            DBUtil.createJNDIDataSource("jdbc/FileSyncJobTest", cc);

            Map<String,Object> config = new TreeMap<String,Object>();
            config.put(SQLGenerator.class.getName(), SQLGenerator.class);
            config.put("jndiDataSourceName", "jdbc/FileSyncJobTest");
            config.put("database", TestUtil.DATABASE);
            config.put("schema", TestUtil.SCHEMA);
            dao.setConfig(config);

            String testDir = TEST_ROOT + File.separator + "testValidJob";

            // Create the test directory the fs storage adapter needs
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrw-");
            FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
            Files.createDirectories(Paths.get(testDir), attr);

        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw ex;
        }

    }

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

        oa = new OpaqueFileSystemStorageAdapter(new File(TEST_ROOT), 1);
        Iterator<StorageMetadata> iter = oa.iterator();

        while (iter.hasNext()) {
            StorageMetadata sm = iter.next();
            log.debug("deleting storage location: " + sm.getStorageLocation());
            oa.delete(sm.getStorageLocation());
        }
    }

    private void createTestDirectory(String testDirectory) throws IOException {
        // Create the test directory the fs storage adapter needs
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrw-");
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
        Files.createDirectories(Paths.get(testDirectory), attr);
    }

    @Test
    public void testValidJobWithAnonSubject() {
        String testDir = TEST_ROOT + File.separator + "testValidJobWithAnonSubject";
        testValidJob(testDir, anonSubject);
    }
    
    @Test
    public void testValidJobWithCertSubject() {
        String testDir = TEST_ROOT + File.separator + "testValidJobWithCertSubject";
        final File certificateFile = FileUtil.getFileFromResource(CERTIFICATE_FILE, FileSyncJobTest.class);
        testValidJob(testDir, SSLUtil.createSubject(certificateFile));
    }
    
    public void testValidJob(String testDir, Subject testSubject) {
        try {
            createTestDirectory(testDir);

            OpaqueFileSystemStorageAdapter sa = new OpaqueFileSystemStorageAdapter(new File(testDir), 1);

            URI artifactID = new URI(TEST_ARTIFACT_URI);
            URI resourceID = new URI(TEST_RESOURCE_ID);

            // Set up an Artifact in the database to start.
            Artifact artifactToUpdate = new Artifact(artifactID,
                new URI("md5:646d3c548ffb98244a0fc52b60556082"), new Date(),
                1008000L);

            log.debug("putting test artifact to database");
            dao.put(artifactToUpdate);

            FileSyncJob fsj = new FileSyncJob(artifactToUpdate, resourceID, sa, dao, testSubject);
            fsj.run();

            // check job succeeded by trying to get artifact by location
            Artifact storedArtifact = dao.get(artifactToUpdate.getID());
            Assert.assertNotNull("storage location of artifact should not be null.", storedArtifact.storageLocation);

            // check for file on disk, throw away the bytes
            FileOutputStream dest = new FileOutputStream("/dev/null");
            log.debug("shunting to /dev/null");
            sa.get(storedArtifact.storageLocation, dest);

        } catch (Exception unexpected) {
            Assert.fail("unexpected exception: " + unexpected);
            log.debug(unexpected);
        }
        log.info("testValidJob - DONE");
    }

    @Test
    public void testResourceNotFoundException() {
        final String testDir = TEST_ROOT + File.separator + "ResourceNotFoundException";

        try {
            createTestDirectory(testDir);

            OpaqueFileSystemStorageAdapter sa = new OpaqueFileSystemStorageAdapter(new File(TEST_ROOT), 1);

            // note: this tests that transfer negotiaiton failed with a ResourceNotFoundException
            URI artifactID = new URI(TEST_ARTIFACT_NOT_FOUND);
            URI resourceID = new URI(TEST_RESOURCE_ID);

            // Set up an Artifact in the database to start.
            // Set checksum to the wrong value.
            Artifact artifactToUpdate = new Artifact(artifactID,
                new URI("md5:0123456789abcdef0123456789abcdef"), new Date(),
                1008000L);

            log.debug("putting test artifact to database");
            dao.put(artifactToUpdate);

            FileSyncJob fsj = new FileSyncJob(artifactToUpdate, resourceID, sa, dao, anonSubject);
            fsj.run();

            log.debug("finished run in failure test.");
            // check job failed by verifying that storage location not set
            Artifact storedArtifact = dao.get(artifactToUpdate.getID());
            Assert.assertNull(storedArtifact.storageLocation);
            
        } catch (Exception unexpected) {
            log.debug("unexpected exception: " + unexpected);
            Assert.fail("unexpected exception");
        }
        log.info("testResourceNotFoundException - DONE");
    }
    
    // TODO: craft a test that will fail in the anonymous part of syncArtifact so
    // unwrap PrivilegedActionException isn't removed by accident again
    
    @Test
    public void testInvalidJobBadChecksum() {
        final String testDir = TEST_ROOT + File.separator + "testValidJobBadChecksum";

        try {
            createTestDirectory(testDir);

            OpaqueFileSystemStorageAdapter sa = new OpaqueFileSystemStorageAdapter(new File(TEST_ROOT), 1);

            URI artifactID = new URI(TEST_ARTIFACT_URI);
            URI resourceID = new URI(TEST_RESOURCE_ID);

            // Set up an Artifact in the database to start.
            // Set checksum to the wrong value.
            Artifact artifactToUpdate = new Artifact(artifactID,
                new URI("md5:0123456789abcdef0123456789abcdef"), new Date(),
                1008000L);

            log.debug("putting test artifact to database");
            dao.put(artifactToUpdate);

            FileSyncJob fsj = new FileSyncJob(artifactToUpdate, resourceID, sa, dao, anonSubject);
            fsj.run();

            log.debug("finished run in failure test.");
            // check job failed by verifying that storage location not set
            Artifact storedArtifact = dao.get(artifactToUpdate.getID());
            Assert.assertNull(storedArtifact.storageLocation);
            
        } catch (Exception unexpected) {
            log.debug("unexpected exception: " + unexpected);
            Assert.fail("unexpected exception");
        }
        log.info("testValidJobBadChecksum - DONE");
    }

    @Test
    public void testInvalidJobBadContentLen() {
        final String testDir = TEST_ROOT + File.separator + "testInvalidJobBadContentLen";

        try {
            createTestDirectory(testDir);

            OpaqueFileSystemStorageAdapter sa = new OpaqueFileSystemStorageAdapter(new File(TEST_ROOT), 1);

            URI artifactID = new URI(TEST_ARTIFACT_URI);
            URI resourceID = new URI(TEST_RESOURCE_ID);

            // Set up an Artifact in the database to start.
            // Set checksum to the wrong value.
            Artifact artifactToUpdate = new Artifact(artifactID,
                new URI("md5:646d3c548ffb98244a0fc52b60556082"), new Date(),
                2000000L);

            log.debug("putting test artifact to database");
            dao.put(artifactToUpdate);

            FileSyncJob fsj = new FileSyncJob(artifactToUpdate, resourceID, sa, dao, anonSubject);
            fsj.run();

            log.debug("finished run in failure test.");
            // check job failed by verifying that storage location not set
            Artifact storedArtifact = dao.get(artifactToUpdate.getID());
            Assert.assertNull(storedArtifact.storageLocation);

        } catch (Exception unexpected) {
            log.debug("unexpected exception: " + unexpected);
            Assert.fail("unexpected exception");
        }

        log.info("testInvalidJobBadContentLen - DONE");
    }

    @Test
    public void testStorageLocationNotNull() {
        final String testDir = TEST_ROOT + File.separator + "testStorageLocationNotNull";

        try {
            createTestDirectory(testDir);

            OpaqueFileSystemStorageAdapter sa = new OpaqueFileSystemStorageAdapter(new File(TEST_ROOT), 1);

            URI artifactID = new URI(TEST_ARTIFACT_URI);
            URI resourceID = new URI(TEST_RESOURCE_ID);

            // Set up an Artifact in the database to start.
            Artifact artifactToUpdate = new Artifact(artifactID,
                new URI("md5:646d3c548ffb98244a0fc52b60556082"), new Date(),
                1008000L);
            StorageLocation testLocation = new StorageLocation(new URI("uri:do-not-overwrite-me"));
            // Set storage location to a value, as FileSyncJob should quit
            // when it discovers it
            artifactToUpdate.storageLocation = testLocation;

            log.debug("putting test artifact to database");
            dao.put(artifactToUpdate);
            

            FileSyncJob fsj = new FileSyncJob(artifactToUpdate, resourceID, sa, dao, anonSubject);
            fsj.run();

            log.debug("successfully finished FileSyncJob run in test.");

            // check job did nothing to the storageLocation
            Artifact storedArtifact = dao.get(artifactToUpdate.getID());
            Assert.assertEquals(testLocation, storedArtifact.storageLocation);

        } catch (Exception unexpected) {
            log.debug("unexpected exception: " + unexpected);
            Assert.fail("unexpected exception");
        }

        log.info("testStorageLocationNotNull - DONE");
    }

}

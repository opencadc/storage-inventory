/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2019.                            (c) 2019.
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

package org.opencadc.inventory.storage.fs;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.fs.FileSystemStorageAdapter.BucketMode;

/**
 * @author majorb
 *
 */
public class FileSystemStorageAdapterTest {
    
    private static final Logger log = Logger.getLogger(FileSystemStorageAdapterTest.class);
    
    private static final String TEST_ROOT = "build/tmp/fsroot";
    
    private static final String dataString = "abcdefghijklmnopqrstuvwxyz";
    private static final byte[] data = dataString.getBytes();

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.DEBUG);
    }
    
    @BeforeClass
    public static void setup() {
        try {
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrw-");
            FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
            Files.createDirectories(Paths.get(TEST_ROOT), attr);
        } catch (Throwable t) {
            log.error("setup error", t);
        }
    }
    
    private void createInstanceTestRoot(String path) throws IOException {
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrw-");
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
        Files.createDirectories(Paths.get(path), attr);
    }
    
    @Test
    public void testPutGetDeleteURIMode() {
        this.testPutGetDelete(BucketMode.URI);
    }
    
    @Test
    public void testPutGetDeleteURIBucketMode() {
        this.testPutGetDelete(BucketMode.URIBUCKET);
    }
    
    private void testPutGetDelete(BucketMode bucketMode) {
        try {
            
            log.info("testPutGetDelete(" + bucketMode + ") - start");
            
            String testDir = TEST_ROOT + File.separator + "testPutGetDelete-" + bucketMode;
            this.createInstanceTestRoot(testDir);
            
            URI uri = URI.create("test:path/file");
            MessageDigest md = MessageDigest.getInstance("MD5");
            String md5Val = HexUtil.toHex(md.digest(data));
            URI checksum = URI.create("md5:" + md5Val);
            log.info("expected md5sum: " + checksum);
            long length = data.length;
            NewArtifact newArtifact = new NewArtifact(uri);
            newArtifact.contentChecksum = checksum;
            newArtifact.contentLength = length;
            
            ByteArrayInputStream source = new ByteArrayInputStream(data);
            
            FileSystemStorageAdapter fs = new FileSystemStorageAdapter(
                testDir, bucketMode);
            StorageMetadata storageMetadata = fs.put(newArtifact, source);
            
            TestOutputStream dest = new TestOutputStream();
            fs.get(storageMetadata.getStorageLocation(), dest);
            
            String resultData = new String(dest.mydata);
            log.info("result data: " + resultData);
            Assert.assertEquals("data", dataString, resultData);
            
            fs.delete(storageMetadata.getStorageLocation());
            
            try {
                fs.get(storageMetadata.getStorageLocation(), dest);
                Assert.fail("Should have received resource not found exception");
            } catch (ResourceNotFoundException e) {
                // expected
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            log.info("testPutGetDelete(" + bucketMode + ") - end");
        }
    }
    
    @Test
    public void testUnsortedIteratorURIMode() {
        this.testUnsortedIterator(BucketMode.URI);
    }
    
    @Test
    public void testUnsortedIteratorURIBucketMode() {
        this.testUnsortedIterator(BucketMode.URIBUCKET);
    }
    
    private void testUnsortedIterator(BucketMode bucketMode) {
        try {
            
            log.info("testUnsortedIterator(" + bucketMode + ") - start");
            
            String testDir = TEST_ROOT + File.separator + "testUnsortedIterator-" + bucketMode;
            this.createInstanceTestRoot(testDir);
            
            FileSystemStorageAdapter fs = new FileSystemStorageAdapter(testDir, bucketMode);
            
            MessageDigest md = MessageDigest.getInstance("MD5");
            String md5Val = HexUtil.toHex(md.digest(data));
            URI checksum = URI.create("md5:" + md5Val);
            log.info("expected md5sum: " + checksum);
            long length = data.length;
            
            String[] files = new String[] {
                "test:dir1/file1",
                "test:dir1/file2",
                "test:dir1/dir2/file3",
                "test:dir1/dir2/file4",
                "test:dir1/file5",
                "test:dir1/dir3/dir4/file6",
                "test:dir1/dir3/dir4/file7",
                "test:dir1/dir3/file8",
                "test:dir1/file9",
                "test:dir1/dir4/file10",
                "test:dir5/file11",
                "test:dir5/dir6/dir7/dir8/file12",
            };
            
            List<StorageMetadata> storageMetadataList = new ArrayList<StorageMetadata>();
            
            for (String file : files) {
                URI uri = URI.create(file);
                NewArtifact newArtifact = new NewArtifact(uri);
                newArtifact.contentChecksum = checksum;
                newArtifact.contentLength = length;
                
                ByteArrayInputStream source = new ByteArrayInputStream(data);

                StorageMetadata meta = fs.put(newArtifact, source);
                storageMetadataList.add(meta);
                log.debug("added " + meta.getStorageLocation().getStorageID());
            }
            
            Iterator<StorageMetadata> iterator = fs.unsortedIterator("");
            List<URI> visitedStorageIDs = new ArrayList<URI>();
            StorageMetadata next = null;
            URI nextStorageID = null;
            StorageMetadata orig = null;
            int count = 0;
            while (iterator.hasNext()) {
                next = iterator.next();
                nextStorageID = next.getStorageLocation().getStorageID();
                int index = storageMetadataList.indexOf(next);
                if (index < 0) {
                    Assert.fail("encounted unknown file: " + nextStorageID);
                } else {
                    orig = storageMetadataList.get(index);
                }
                if (visitedStorageIDs.contains(nextStorageID)) {
                    Assert.fail("already visited file: " + nextStorageID);
                }
                Assert.assertEquals("checksum", checksum, next.getContentChecksum());
                Assert.assertEquals("length", new Long(length), next.getContentLength());
                if (bucketMode.equals(BucketMode.URI)) {
                    // check the artifact URI
                    Assert.assertEquals("artifactURI", orig.artifactURI, next.artifactURI);
                }
                visitedStorageIDs.add(nextStorageID);
                count++;
            }
            
            Assert.assertEquals("file count", storageMetadataList.size(), count);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            log.info("testUnsortedIterator(" + bucketMode + ") - end");
        }
    }
    
    @Test
    public void testIterateSubsetURIMode() {
        try {
            
            log.info("testIterateSubsetURIMode - start");
            
            String testDir = TEST_ROOT + File.separator + "testIterateSubsetURIMode";
            this.createInstanceTestRoot(testDir);
            
            FileSystemStorageAdapter fs = new FileSystemStorageAdapter(testDir, BucketMode.URI);
            
            MessageDigest md = MessageDigest.getInstance("MD5");
            String md5Val = HexUtil.toHex(md.digest(data));
            URI checksum = URI.create("md5:" + md5Val);
            log.info("expected md5sum: " + checksum);
            long length = data.length;
            
            String[] files = new String[] {
                "test:dir1/file1",
                "test:dir1/file2",
                "test:dir1/dir2/file3",
                "test:dir1/dir2/file4",
                "test:dir1/file5",
                "test:dir1/dir3/dir4/file6",
                "test:dir1/dir3/dir4/file7",
                "test:dir1/dir3/file8",
                "test:dir1/file9",
                "test:dir1/dir4/file10",
                "test:dir5/file11",
                "test:dir5/dir6/dir7/dir8/file12",
            };
            
            List<URI> storageIDs = new ArrayList<URI>();
            
            for (String file : files) {
                URI uri = URI.create(file);
                NewArtifact newArtifact = new NewArtifact(uri);
                newArtifact.contentChecksum = checksum;
                newArtifact.contentLength = length;

                ByteArrayInputStream source = new ByteArrayInputStream(data);
                
                StorageMetadata meta = fs.put(newArtifact, source);
                storageIDs.add(meta.getStorageLocation().getStorageID());
                log.debug("added " + meta.getStorageLocation().getStorageID());
            }
            
            Iterator<StorageMetadata> iterator = fs.unsortedIterator("test:dir1");
            List<URI> visitedStorageIDs = new ArrayList<URI>();
            StorageMetadata next = null;
            URI nextStorageID = null;
            int count = 0;
            while (iterator.hasNext()) {
                next = iterator.next();
                nextStorageID = next.getStorageLocation().getStorageID();
                log.debug("Next in iterator: " + nextStorageID);
                if (!storageIDs.contains(nextStorageID)) {
                    Assert.fail("encounted unknown file: " + nextStorageID);
                }
                if (visitedStorageIDs.contains(nextStorageID)) {
                    Assert.fail("already visited file: " + nextStorageID);
                }
                Assert.assertEquals("checksum", checksum, next.getContentChecksum());
                Assert.assertEquals("length", new Long(length), next.getContentLength());
                visitedStorageIDs.add(nextStorageID);
                count++;
            }
            
            // take the two non 'dir1' buckets out of the expected list
            Assert.assertEquals("file count", storageIDs.size() - 2, count);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            log.info("testIterateSubsetURIMode - end");
        }
    }
    
    private class TestOutputStream extends ByteArrayOutputStream {
        
        byte[] mydata = new byte[data.length];
        int mypos = 0;

        @Override
        public void write(byte[] buf, int pos, int bytes) {
            System.arraycopy(buf, pos, mydata, mypos, bytes);
            mypos += bytes;
        }
        
    }

}

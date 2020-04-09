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

package org.opencadc.inventory.storage.fs;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * Integration tests that interact with the file system. These tests require a file system
 * that supports posix extended attributes.
 * 
 * @author pdowler
 */
public class OpaqueFileSystemStorageAdapterTest {
    private static final Logger log = Logger.getLogger(OpaqueFileSystemStorageAdapterTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage.fs", Level.INFO);
    }
    
    OpaqueFileSystemStorageAdapter adapter;
    int depth = 2;
            
    public OpaqueFileSystemStorageAdapterTest() { 
        File tmp = new File("build/tmp");
        File root = new File(tmp, "opaque-int-tests");
        root.mkdir();

        this.adapter = new OpaqueFileSystemStorageAdapter(root, depth); // opaque
        log.info("    content path: " + adapter.contentPath);
        log.info("transaction path: " + adapter.txnPath);
        Assert.assertTrue("testInit: contentPath", Files.exists(adapter.contentPath));
        Assert.assertTrue("testInit: txnPath", Files.exists(adapter.txnPath));
    }
    
    @Before
    public void init_cleanup() throws IOException {
        log.info("init_cleanup: delete all content from " + adapter.contentPath);
        if (Files.exists(adapter.contentPath)) {
            Files.walkFileTree(adapter.contentPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        log.info("init_cleanup: delete all content from " + adapter.contentPath + " DONE");
    }
    
    @Test
    public void testPutGetDelete() {
        try {
            String dataString = "abcdefghijklmnopqrstuvwxyz";
            byte[] data = dataString.getBytes();
            
            URI artifactURI = URI.create("test:path/file");
            MessageDigest md = MessageDigest.getInstance("MD5");
            String md5Val = HexUtil.toHex(md.digest(data));
            NewArtifact newArtifact = new NewArtifact(artifactURI);
            newArtifact.contentChecksum = URI.create("md5:" + md5Val);;
            newArtifact.contentLength = (long) data.length;
            
            ByteArrayInputStream bis = new ByteArrayInputStream(data);

            StorageMetadata storageMetadata = adapter.put(newArtifact, bis);
            log.info("put: " + storageMetadata.getStorageLocation());
            Assert.assertNotNull(storageMetadata);
            Assert.assertNotNull(storageMetadata.getStorageLocation());
            Assert.assertEquals(newArtifact.contentChecksum, storageMetadata.getContentChecksum());
            Assert.assertEquals(newArtifact.contentLength, storageMetadata.getContentLength());
            Assert.assertEquals("artifactURI",  artifactURI, storageMetadata.artifactURI);
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(storageMetadata.getStorageLocation(), bos);
            byte[] actual = bos.toByteArray();
            Assert.assertEquals(("data length"), data.length, actual.length);
            String resultData = new String(bos.toByteArray());
            Assert.assertEquals("data", dataString, resultData);

            // other metadata output from StorageAdapter tested in list() and iterator()
            
            adapter.delete(storageMetadata.getStorageLocation());
            try {
                adapter.get(storageMetadata.getStorageLocation(), new ByteArrayOutputStream());
                Assert.fail("Should have received resource not found exception");
            } catch (ResourceNotFoundException expected) {
                log.info("caught expected: " + expected);
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testIterator() {
        try {
            String dataString = "abcdefghijklmnopqrstuvwxyz";
            byte[] data = dataString.getBytes();
            MessageDigest md = MessageDigest.getInstance("MD5");
            String md5Val = HexUtil.toHex(md.digest(data));
            URI checksum = URI.create("md5:" + md5Val);
            log.info("expected md5sum: " + checksum);

            int num = 6;
            SortedSet<StorageMetadata> explist = new TreeSet<StorageMetadata>();
            for (int i = 0; i < num; i++) {
                String suri = "test:FOO/bar" + i;
                URI uri = URI.create(suri);
                NewArtifact newArtifact = new NewArtifact(uri);
                newArtifact.contentChecksum = checksum;
                newArtifact.contentLength = (long) data.length;

                ByteArrayInputStream source = new ByteArrayInputStream(data);

                StorageMetadata meta = adapter.put(newArtifact, source);
                explist.add(meta);
                log.info("added file: " + meta.getStorageLocation());
            }
            // put + delete leaves empty dirs behind
            for (int i = num; i < 4 * num; i++) {
                String suri = "test:FOO/bar" + i;
                URI uri = URI.create(suri);
                NewArtifact newArtifact = new NewArtifact(uri);
                newArtifact.contentChecksum = checksum;
                newArtifact.contentLength = (long) data.length;

                ByteArrayInputStream source = new ByteArrayInputStream(data);

                StorageMetadata meta = adapter.put(newArtifact, source);
                adapter.delete(meta.getStorageLocation());
                log.info("added dir: " + meta.getStorageLocation().storageBucket);
            }
            
            // iterate all
            {
                Iterator<StorageMetadata> ai = adapter.iterator();
                Iterator<StorageMetadata> ei = explist.iterator();
                int count = 0;
                while (ai.hasNext()) {
                    count++;
                    StorageMetadata expected = ei.next();
                    StorageMetadata actual = ai.next();
                    log.info("adapter.iterator: " + actual);
                    
                    Assert.assertEquals("order " + count, expected.getStorageLocation(), actual.getStorageLocation());

                    Assert.assertEquals("checksum", checksum, actual.getContentChecksum());
                    Assert.assertEquals("length", new Long(data.length), actual.getContentLength());
                    Assert.assertEquals("artifactURI",expected.artifactURI, actual.artifactURI);
                    
                }
                Assert.assertEquals("file count", explist.size(), count);
            }
            
            // iterate individual top-level buckets
            {
                int n = 0;
                for (int i = 0; i < Artifact.URI_BUCKET_CHARS.length(); i++) {
                    String bucketPrefix = Artifact.URI_BUCKET_CHARS.substring(i, i + 1);
                    log.info("iterator: " + bucketPrefix);
                    
                    Iterator<StorageMetadata> bi = adapter.iterator(bucketPrefix);
                    while (bi.hasNext()) {
                        StorageMetadata sm = bi.next();
                        Assert.assertTrue("prefix match", sm.getStorageLocation().storageBucket.startsWith(bucketPrefix));
                        n++;
                    }
                }
                Assert.assertEquals("file count", explist.size(), n);
            }
            
            // test list function duplicates iterator content
            {
                SortedSet<StorageMetadata> aset = adapter.list(null);
                Iterator<StorageMetadata> ai = aset.iterator();
                Iterator<StorageMetadata> ei = explist.iterator();
                int count = 0;
                while (ai.hasNext()) {
                    count++;
                    StorageMetadata expected = ei.next();
                    StorageMetadata actual = ai.next();
                    log.info("adapter.list: " + actual);
                    
                    Assert.assertEquals("order " + count, expected.getStorageLocation(), actual.getStorageLocation());

                    Assert.assertEquals("checksum", checksum, actual.getContentChecksum());
                    Assert.assertEquals("length", new Long(data.length), actual.getContentLength());
                    Assert.assertEquals("artifactURI",expected.artifactURI, actual.artifactURI);
                    
                }
                Assert.assertEquals("file count", explist.size(), count);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}

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

import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.io.DiscardOutputStream;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * Integration tests that interact with the file system. These tests require a file system
 * that supports posix extended attributes.
 * 
 * @author pdowler
 */
public class OpaqueByteRangeTest {
    private static final Logger log = Logger.getLogger(OpaqueByteRangeTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage.fs", Level.INFO);
    }
    
    OpaqueFileSystemStorageAdapter adapter;
    int depth = 2;
            
    public OpaqueByteRangeTest() { 
        File tmp = new File("build/tmp");
        File root = new File(tmp, "opaque-int-tests");
        root.mkdir();

        this.adapter = new OpaqueFileSystemStorageAdapter(root, depth); // opaque
        log.debug("    content path: " + adapter.contentPath);
        log.debug("transaction path: " + adapter.txnPath);
        Assert.assertTrue("testInit: contentPath", Files.exists(adapter.contentPath));
        Assert.assertTrue("testInit: txnPath", Files.exists(adapter.txnPath));
    }
    
    //@Before
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
    public void testFullRead() {
        try {
            int mib = 16;
            long datalen = mib * 1024L * 1024L; // 1 MiB
            
            URI artifactURI = URI.create("test:path/file");
            NewArtifact newArtifact = new NewArtifact(artifactURI);
            InputStream istream = getInputStreamOfRandomBytes(datalen);

            log.info("put: " + mib + " MiB file...");
            long t1 = System.nanoTime();
            StorageMetadata storageMetadata = adapter.put(newArtifact, istream);
            long t2 = System.nanoTime();
            final long putMicros = (t2 - t1) / 1024L;
            StringBuilder sb = new StringBuilder();
            sb.append("put: ").append(storageMetadata.getStorageLocation());
            sb.append(" -- ");
            
            sb.append(Long.toString(putMicros)).append(" microsec");
            double spd = (double) (10 * datalen / putMicros) / 10.0;
            sb.append(" aka ~" + spd + " MiB/sec");
            log.info(sb);
            
            Assert.assertNotNull(storageMetadata);
            Assert.assertNotNull(storageMetadata.getStorageLocation());
            Assert.assertEquals(datalen, storageMetadata.getContentLength().longValue());
            
            
            ByteRange r = new ByteRange(0, datalen);

            SortedSet<ByteRange> br = new TreeSet<>();
            br.add(r);
            DigestOutputStream out = new DigestOutputStream(new DiscardOutputStream(), MessageDigest.getInstance("MD5"));
            ByteCountOutputStream bcos = new ByteCountOutputStream(out);
            t1 = System.nanoTime();
            adapter.get(storageMetadata.getStorageLocation(), bcos);
            //adapter.get(storageMetadata.getStorageLocation(), bcos, br);
            t2 = System.nanoTime();
            final long getMicros = (t2 - t1) / 1024L;
            Assert.assertEquals("num bytes returned", datalen, bcos.getByteCount());
            URI actualMD5 = URI.create("md5:" + HexUtil.toHex(out.getMessageDigest().digest()));
            Assert.assertEquals("checksum", storageMetadata.getContentChecksum(), actualMD5);
            sb = new StringBuilder();
            sb.append("read ").append(r);
            while (sb.length() < 32) {
                sb.append(" ");
            }
            sb.append(Long.toString(getMicros)).append(" microsec");
            spd = (double) (10 * datalen / getMicros) / 10.0;
            sb.append(" aka ~" + spd + " MiB/sec");
            log.info(sb);
            
            adapter.delete(storageMetadata.getStorageLocation());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    //@Test
    public void testReadByteRangeForward() {
        int[] readOrder = new int[] {0, 1, 2, 0, 1, 2, 0, 1, 2};
        doReadPattern(readOrder);
    }
    
    //@Test
    public void testReadByteRangeReverse() {
        int[] readOrder = new int[] {2, 1, 0, 2, 1, 0, 2, 1, 0};
        doReadPattern(readOrder);
    }
    
    public void doReadPattern(int[] readOrder) {
        try {
            int mib = 16;
            long datalen = mib * 1024L * 1024L; // 1 MiB
            
            URI artifactURI = URI.create("test:path/file");
            NewArtifact newArtifact = new NewArtifact(artifactURI);
            InputStream istream = getInputStreamOfRandomBytes(datalen);

            log.info("put: " + mib + " MiB file...");
            StorageMetadata storageMetadata = adapter.put(newArtifact, istream);
            log.info("put: " + storageMetadata.getStorageLocation());
            Assert.assertNotNull(storageMetadata);
            Assert.assertNotNull(storageMetadata.getStorageLocation());
            Assert.assertEquals(datalen, storageMetadata.getContentLength().longValue());
            
            List<ByteRange> ranges = new ArrayList<>();
            long rlen = 16 * 1024L; // 16KiB
            ranges.add(new ByteRange(0L, rlen));                    // at start
            ranges.add(new ByteRange(datalen / 2L, rlen));          // near the middle
            ranges.add(new ByteRange(datalen - rlen - 1L, rlen));   // at end
            
            for (int i : readOrder) {
                ByteRange r = ranges.get(i);
                
                SortedSet<ByteRange> br = new TreeSet<>();
                br.add(r);
                ByteCountOutputStream bcos = new ByteCountOutputStream(new DiscardOutputStream());
                long t1 = System.nanoTime();
                adapter.get(storageMetadata.getStorageLocation(), bcos, br);
                long t2 = System.nanoTime();
                final long micros = (t2 - t1) / 1024L;
                Assert.assertEquals("num bytes returned", rlen, bcos.getByteCount());
                StringBuilder sb = new StringBuilder();
                sb.append("read ").append(r);
                while (sb.length() < 32) {
                    sb.append(" ");
                }
                sb.append(Long.toString(micros)).append(" microsec");
                log.info(sb);
            }
            
            //adapter.delete(storageMetadata.getStorageLocation());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    private InputStream getInputStreamOfRandomBytes(long numBytes) {
        
        Random rnd = new Random();
        
        return new InputStream() {
            long tot = 0L;
            
            @Override
            public int read() throws IOException {
                if (tot == numBytes) {
                    return -1;
                }
                tot++;
                return rnd.nextInt(255);
            }
            
            @Override
            public int read(byte[] bytes) throws IOException {
                return read(bytes, 0, bytes.length);
            }

            @Override
            public int read(byte[] bytes, int off, int len) throws IOException {
                if (tot == numBytes) {
                    return -1;
                }
                int num = len;
                if (tot + len > numBytes) {
                    num = (int) (numBytes - tot);
                }
                byte val = (byte) rnd.nextInt(255);
                Arrays.fill(bytes, off, off + num - 1, val);
                tot += num;
                return num;
            }
        };
    }
}

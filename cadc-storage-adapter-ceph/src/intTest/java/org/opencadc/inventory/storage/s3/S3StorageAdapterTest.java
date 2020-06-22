
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

package org.opencadc.inventory.storage.s3;

import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import nom.tam.fits.BasicHDU;
import nom.tam.fits.Fits;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * Test the S3StorageAdapter against a real S3 API endpoint.
 * 
 * @author pdowler
 */
abstract class S3StorageAdapterTest {
    private static final Logger log = Logger.getLogger(S3StorageAdapterTest.class);
    
    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
    }
    
    protected final S3StorageAdapter adapter;
    
    protected S3StorageAdapterTest(S3StorageAdapter impl) {
        this.adapter = impl;
    }

    //@Before
    @After 
    public abstract void cleanup() throws Exception;
    
    //@Test
    public void testNoOp() {
    }
    
    @Test
    public void testPutGetDelete() {
        URI artifactURI = URI.create("cadc:TEST/testPutGetDelete");
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Random rnd = new Random();
            byte[] data = new byte[1024];
            rnd.nextBytes(data);
            
            NewArtifact na = new NewArtifact(artifactURI);
            md.update(data);
            URI expectedChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            na.contentChecksum = expectedChecksum;
            na.contentLength = (long) data.length;
            log.debug("testPutGetDelete random data: " + data.length + " " + expectedChecksum);
            
            log.debug("testPutGetDelete put: " + artifactURI);
            StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
            log.info("testPutGetDelete put: " + artifactURI + " to " + sm.getStorageLocation());
            
            // verify data stored
            log.debug("testPutGetDelete get: " + artifactURI);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(sm.getStorageLocation(), bos);
            md.reset();
            byte[] actual = bos.toByteArray();
            md.update(actual);
            URI actualChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("testPutGetDelete get: " + artifactURI + " " + actual.length + " " + actualChecksum);
            Assert.assertEquals("length", (long) na.contentLength, actual.length);
            Assert.assertEquals("checksum", na.contentChecksum, actualChecksum);

            log.debug("testPutGetDelete delete: " + sm.getStorageLocation());
            adapter.delete(sm.getStorageLocation());
            Assert.assertTrue("deleted", !adapter.exists(sm.getStorageLocation()));
            log.info("testPutGetDelete deleted: " + sm.getStorageLocation());
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testPutGetDeleteMinimal_S3_API_Limitation_FAIL() {
        URI artifactURI = URI.create("cadc:TEST/testPutGetDeleteMinimal");
        
        try {
            Random rnd = new Random();
            byte[] data = new byte[1024];
            rnd.nextBytes(data);
            
            final NewArtifact na = new NewArtifact(artifactURI);
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(data);
            URI expectedChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            
            // test that we can store raw input stream data with no metadata
            
            //na.contentChecksum = expectedChecksum;
            //na.contentLength = (long) data.length;
            
            //na.contentLength = null;                  // NullPointerException unboxing
            //na.contentLength = -1L;                     // IllegalStateException: Content length must be greater than or equal to zero
            //na.contentLength = 0L;                    // Error Code: XAmzContentSHA256Mismatch
            //na.contentLength = (long) data.length - 1;  // Error Code: XAmzContentSHA256Mismatch
            //na.contentLength = (long) data.length + 1;  // hangs for ~2 min, Error while reading from stream.
            //na.contentLength = Long.MAX_VALUE;        // Error Code: SignatureDoesNotMatch
            log.debug("testPutGetDeleteMinimal random data: " + data.length + " " + expectedChecksum);
            
            log.debug("testPutGetDeleteMinimal put: " + artifactURI);
            StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
            log.info("testPutGetDeleteMinimal put: " + artifactURI + " to " + sm.getStorageLocation());
            
            // verify data stored
            log.debug("testPutGetDeleteMinimal get: " + artifactURI);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(sm.getStorageLocation(), bos);
            byte[] actual = bos.toByteArray();
            md.update(actual);
            URI actualChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("testPutGetDeleteMinimal get: " + artifactURI + " " + actual.length + " " + actualChecksum);
            Assert.assertEquals("length", (long) na.contentLength, actual.length);
            Assert.assertEquals("checksum", expectedChecksum, actualChecksum);
            
            // TODO: verify metadata captured without using iterator
            
            log.debug("testPutGetDeleteMinimal delete: " + sm.getStorageLocation());
            adapter.delete(sm.getStorageLocation());
            Assert.assertTrue("deleted", !adapter.exists(sm.getStorageLocation()));
            log.info("testPutGetDeleteMinimal deleted: " + sm.getStorageLocation());
        //} catch (UnsupportedOperationException limitation) {
        //    log.warn("caught S3 API limitation: " + limitation);
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testPutGetDeleteWrongMD5() {
        URI artifactURI = URI.create("cadc:TEST/testPutGetDeleteWrongMD5");
        
        try {
            Random rnd = new Random();
            byte[] data = new byte[1024];
            rnd.nextBytes(data);
            
            NewArtifact na = new NewArtifact(artifactURI);
            na.contentChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"); // md5 of 0-length file
            na.contentLength = (long) data.length;
            log.debug("testPutGetDeleteWrongMD5 random data: " + data.length + " " +  na.contentChecksum);
            
            try {
                log.debug("testPutGetDeleteWrongMD5 put: " + artifactURI);
                StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
                Assert.fail("testPutGetDeleteWrongMD5 put: " + artifactURI + " to " + sm.getStorageLocation());
            } catch (PreconditionFailedException expected) {
                log.info("testPutGetDeleteWrongMD5 caught: " + expected);
            }
            
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testPutLargeStreamReject() {
        URI artifactURI = URI.create("cadc:TEST/testPutLargeStreamReject");
        
        final NewArtifact na = new NewArtifact(artifactURI);
        
        // ceph/S3 limit of 5GiB
        long numBytes = (long) 6 * 1024 * 1024 * 1024; 
        na.contentLength = numBytes;
        na.contentChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"); // md5 of 0-length file
            
        try {
            InputStream istream = getInputStreamThatFails();
            log.info("testPutCheckDeleteLargeStreamReject put: " + artifactURI + " " + numBytes);
            StorageMetadata sm = adapter.put(na, istream);
            Assert.fail("expected ByteLimitExceededException, got: " + sm);
        } catch (ByteLimitExceededException expected) {
            log.info("caught: " + expected);
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testPutLargeStreamFail_S3_API_Limitation_FAIL() {
        URI artifactURI = URI.create("cadc:TEST/testPutLargeStreamFail");
        
        final NewArtifact na = new NewArtifact(artifactURI);
        
        // ceph limit of 5GiB
        long numBytes = (long) 6 * 1024 * 1024 * 1024; 
            
        try {
            InputStream istream = getInputStreamOfRandomBytes(numBytes);
            log.info("testPutCheckDeleteLargeStreamFail put: " + artifactURI + " " + numBytes);
            StorageMetadata sm = adapter.put(na, istream);
            
            Assert.assertFalse("put should have failed, but object exists", adapter.exists(sm.getStorageLocation()));
            
            Assert.fail("expected ByteLimitExceededException, got: " + sm);
        } catch (ByteLimitExceededException expected) {
            log.info("caught: " + expected);
        //} catch (UnsupportedOperationException limitation) {
        //    log.warn("caught S3 API limitation: " + limitation);
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testNotExists() {
        try {
            // neither bucket nor key exist
            StorageLocation doesNotExist = new StorageLocation(URI.create("foo:does-not-exist"));
            doesNotExist.storageBucket = "non-existent-bucket";
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try { 
                adapter.get(doesNotExist, bos);
                Assert.fail("get: " + doesNotExist + " returned " + bos.size() + " bytes");
            } catch (ResourceNotFoundException expected) {
                log.debug("caught expected: " + expected);
                Assert.assertTrue(expected.getMessage().startsWith("not found: "));
            }
            
            // make sure bucket exists
            URI artifactURI = URI.create("cadc:TEST/testNotExists");
            Random rnd = new Random();
            byte[] data = new byte[1024];
            rnd.nextBytes(data);
            
            MessageDigest md = MessageDigest.getInstance("MD5");
            NewArtifact na = new NewArtifact(artifactURI);
            md.update(data);
            na.contentChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            na.contentLength = (long) data.length;
            StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
            log.info("testPutGetDelete put: " + artifactURI + " to " + sm.getStorageLocation());
            
            doesNotExist.storageBucket = sm.getStorageLocation().storageBucket; // existing bucket
            bos.reset();
            try { 
                adapter.get(doesNotExist, bos);
                Assert.fail("get: " + doesNotExist + " returned " + bos.size() + " bytes");
            } catch (ResourceNotFoundException expected) {
                log.debug("caught expected: " + expected);
                Assert.assertTrue(expected.getMessage().startsWith("not found: "));
            }
            
            adapter.delete(sm.getStorageLocation());
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testIterator() {
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Random rnd = new Random();
            byte[] data = new byte[1024];
            
            SortedSet<StorageMetadata> expected = new TreeSet<>();
            for (int i = 0; i < 10; i++) {
                URI artifactURI = URI.create("cadc:TEST/testIterator-" + i);
                rnd.nextBytes(data);
                NewArtifact na = new NewArtifact(artifactURI);
                md.update(data);
                // contentChecksum currently required for round-trip
                na.contentChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
                na.contentLength = (long) data.length;
                StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
                Assert.assertNotNull(sm.artifactURI);
                //Assert.assertNotNull(sm.contentLastModified);
                log.debug("testIterator put: " + artifactURI + " to " + sm.getStorageLocation());
                expected.add(sm);
            }
            log.info("testIterator created: " + expected.size());
            
            // full iterator
            List<StorageMetadata> actual = new ArrayList<>();
            Iterator<StorageMetadata> iter = adapter.iterator();
            while (iter.hasNext()) {
                StorageMetadata sm = iter.next();
                log.debug("found: " + sm.getStorageLocation() + " " + sm.getContentLength() + " " + sm.getContentChecksum());
                actual.add(sm);
            }
            
            Assert.assertEquals("iterator.size", expected.size(), actual.size());
            Iterator<StorageMetadata> ei = expected.iterator();
            Iterator<StorageMetadata> ai = actual.iterator();
            while (ei.hasNext()) {
                StorageMetadata em = ei.next();
                StorageMetadata am = ai.next();
                log.debug("compare: " + em.getStorageLocation() + " vs " + am.getStorageLocation());
                Assert.assertEquals("order", em, am);
                Assert.assertEquals("length", em.getContentLength(), am.getContentLength());
                Assert.assertEquals("checksum", em.getContentChecksum(), am.getContentChecksum());
                
                Assert.assertNotNull("artifactUIRI", am.artifactURI);
                Assert.assertEquals("artifactURI", em.artifactURI, am.artifactURI);
                
                Assert.assertNotNull("contentLastModified", am.contentLastModified);
                //Assert.assertEquals("contentLastModified", em.contentLastModified, am.contentLastModified);
            }
            
            // rely on cleanup()
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testIteratorBucketPrefix() {
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Random rnd = new Random();
            byte[] data = new byte[1024];
            
            SortedSet<StorageMetadata> expected = new TreeSet<>();
            for (int i = 0; i < 10; i++) {
                URI artifactURI = URI.create("cadc:TEST/testIteratorBucketPrefix-" + i);
                rnd.nextBytes(data);
                NewArtifact na = new NewArtifact(artifactURI);
                md.update(data);
                na.contentChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
                na.contentLength = (long) data.length;
                StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
                log.debug("testList put: " + artifactURI + " to " + sm.getStorageLocation());
                expected.add(sm);
            }
            log.info("testIteratorBucketPrefix created: " + expected.size());
            
            int found = 0;
            for (byte b = 0; b < 16; b++) {
                String bpre = HexUtil.toHex(b).substring(1);
                log.debug("bucket prefix: " + bpre);
                Iterator<StorageMetadata> i = adapter.iterator(bpre);
                while (i.hasNext()) {
                    StorageMetadata sm = i.next();
                    Assert.assertTrue("prefix match", sm.getStorageLocation().storageBucket.startsWith(bpre));
                    found++;
                }
            }
            Assert.assertEquals("found with bucketPrefix", expected.size(), found);
            
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    private InputStream getInputStreamOfRandomBytes(long numBytes) {
        
        Random rnd = new Random();
        byte val = (byte) rnd.nextInt(127);
        
        
        return new InputStream() {
            long tot = 0L;
            long ncalls = 0L;
            
            @Override
            public int read() throws IOException {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public int read(byte[] bytes) throws IOException {
                int ret = super.read(bytes, 0, bytes.length);
                return ret;
            }

            @Override
            public int read(byte[] bytes, int off, int len) throws IOException {
                if (len == 0) {
                    return 0;
                }
                
                if (tot >= numBytes) {
                    return -1;
                }
                long rem = numBytes - tot;
                long ret = Math.min(len, rem);
                Arrays.fill(bytes, val);
                tot += ret;
                ncalls++;
                if ((ncalls % 10000) == 0) {
                    long mib = tot / (1024L * 1024L);
                    log.info("output: " + mib + " MiB");
                }
                return (int) ret;
            }
        };
    }
    
    private InputStream getInputStreamThatFails() {
        return new InputStream() {
            
            @Override
            public int read() throws IOException {
                throw new RuntimeException("BUG: stream should not be read");
            }
            
            @Override
            public int read(byte[] bytes) throws IOException {
                throw new RuntimeException("BUG: stream should not be read");
            }

            @Override
            public int read(byte[] bytes, int off, int len) throws IOException {
                throw new RuntimeException("BUG: stream should not be read");
            }
        };
    }
    
    private void ensureMEFTestFile() throws Exception {
        throw new UnsupportedOperationException("not implemented");
        /*
        final long expectedContentLength = 312151680L;
        try (final S3Client s3Client = S3Client.builder()
                                               .endpointOverride(ENDPOINT)
                                               .region(Region.of(REGION))
                                               .build()) {
            try {
                s3Client.headObject(HeadObjectRequest.builder().bucket(BUCKET_NAME).key(MEF_OBJECT_ID).build());
                LOGGER.info(String.format("Test file %s/%s exists.", BUCKET_NAME, MEF_OBJECT_ID));
            } catch (NoSuchKeyException e) {
                LOGGER.info("*********");
                LOGGER.info(String.format("Test file (%s/%s) does not exist.  Uploading file from VOSpace...",
                                          BUCKET_NAME, MEF_OBJECT_ID));
                LOGGER.info("*********");

                final URL sourceURL = new URL("https://www.cadc-ccda.hia-iha.nrc-cnrc.gc" +
                                              ".ca/files/vault/CADCtest/Public/test-megaprime.fits.fz");


                try (final InputStream inputStream = openStream(sourceURL)) {
                    s3Client.putObject(PutObjectRequest.builder()
                                                       .bucket(BUCKET_NAME)
                                                       .key(MEF_OBJECT_ID)
                                                       .build(),
                                       RequestBody.fromInputStream(inputStream, expectedContentLength));
                }

                LOGGER.info("*********");
                LOGGER.info(String.format("Test file (%s/%s) uploaded.", BUCKET_NAME, MEF_OBJECT_ID));
                LOGGER.info("*********");
            }
        }
        */
    }
    
    public void getHeaders() throws Exception {
        log.info("Skip to headers...");
        log.info("***");
        ensureMEFTestFile();
        final URI testURI = URI.create("cadc:TEST/getHeaders");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest.getInstance("MD5"));
        final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);

        final Set<String> cutouts = new HashSet<>();
        cutouts.add("fhead");

        adapter.get(new StorageLocation(testURI), byteCountOutputStream, cutouts);
        byteCountOutputStream.close();
        log.info("***");
        log.info("Skip to headers done.");
    }

    public void getCutouts() throws Exception {
        final URI testURI = URI.create("cadc:TEST/getCutouts");
        //final long expectedByteCount = 159944L;
        //final URI expectedChecksum = URI.create("md5:7c6372a8d20da28b54b6b50ce36f9195");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest.getInstance("MD5"));
        final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
        final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

        final Set<String> cutouts = new HashSet<>();
        //cutouts.add("[SCI,10][80:220,100:150]");
        //cutouts.add("[1][10:16,70:90]");
        //cutouts.add("[106][8:32,88:112]");
        //cutouts.add("[126]");

        adapter.get(new StorageLocation(testURI), byteCountOutputStream, cutouts);
        byteCountOutputStream.close();

        //Assert.assertEquals("Wrong byte count.", expectedByteCount, byteCountOutputStream.getByteCount());
        //Assert.assertEquals("Wrong checksum.", expectedChecksum,
        //                    URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
        //                                             new BigInteger(1, messageDigest.digest()).toString(16))));

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final Fits fitsFile = new Fits();
        fitsFile.read(byteArrayInputStream);

        Assert.assertEquals("Wrong number of HDUs.", 1, fitsFile.getNumberOfHDUs());

        final BasicHDU<?> hdu1 = fitsFile.getHDU(0);
        //final BasicHDU<?> hdu106 = fitsFile.getHDU(1);
        //final BasicHDU<?> hdu126 = fitsFile.getHDU(2);

        byteArrayInputStream.close();
    }
}

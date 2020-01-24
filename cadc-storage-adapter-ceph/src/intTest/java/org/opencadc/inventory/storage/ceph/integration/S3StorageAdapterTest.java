
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
 *
 ************************************************************************
 */

package org.opencadc.inventory.storage.ceph.integration;

import nom.tam.fits.BasicHDU;
import nom.tam.fits.Fits;
import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.s3.S3StorageAdapter;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.StringUtil;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


public class S3StorageAdapterTest {

    private static final Logger LOGGER = Logger.getLogger(S3StorageAdapterTest.class);
    private static final URI ENDPOINT = URI.create("http://dao-wkr-04.cadc.dao.nrc.ca:8080");
    private static final String REGION = Region.US_EAST_1.id();
    private static final String DIGEST_ALGORITHM = "MD5";
    private static final String DEFAULT_BUCKET_NAME = "cadctest";
    private static final String MEF_OBJECT_ID = "test-megaprime-s3.fits.fz";

    /**
     * Override the bucket to use by setting -Dbucket.name=mybucket in your test command.  Should have a
     * public-read-write ACL to make these tests easier to run.
     */
    private static final String BUCKET_NAME = System.getProperty("s3.bucket.name", DEFAULT_BUCKET_NAME);

    /**
     * Used to issue tests with the FITS Header jumping as it should remain untouched.  Has a public-read ACL.
     */
    private static final String LIST_BUCKET_NAME = "cadctestlist";


    /**
     * The list-s3.out file contains the list of objects in S3 in UTF-8 binary order as it was taken from S3.
     *
     * @throws Exception Any exceptions.
     */
    @Test
    public void list() throws Exception {
        /*
         * The list-s3.out file contains 2011 objects listed from the aws command line in whatever order it
         * provided.  Read it back in here as a base to check list sorting.
         */
        final File s3ListOutput = FileUtil.getFileFromResource("list-s3.out", S3StorageAdapter.class);
        final List<String> s3AdapterListObjectsOutput = new ArrayList<>();
        final List<String> s3ListOutputItems = new ArrayList<>();
        final FileReader fileReader = new FileReader(s3ListOutput);
        final BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            s3ListOutputItems.add(line);
        }

        bufferedReader.close();

        final List<String> utf8SortedItems = new ArrayList<>(s3ListOutputItems);

        // Ensure sorted by UTF-8
        utf8SortedItems.sort(Comparator.comparing(o -> new String(o.getBytes(StandardCharsets.UTF_8),
                                                                  StandardCharsets.UTF_8)));

        final S3StorageAdapter testSubject = new S3StorageAdapter(ENDPOINT, REGION);
        final long start = System.currentTimeMillis();

        for (final Iterator<StorageMetadata> storageMetadataIterator = testSubject.iterator(LIST_BUCKET_NAME);
             storageMetadataIterator.hasNext(); ) {
            final StorageMetadata storageMetadata = storageMetadataIterator.next();
            final URI artifactURI = storageMetadata.artifactURI;
            s3AdapterListObjectsOutput.add((artifactURI != null)
                                           ? artifactURI.toASCIIString()
                                           : storageMetadata.getStorageLocation().getStorageID()
                                                            .getSchemeSpecificPart());
            // Do nothing
        }
        LOGGER.debug(String.format("Listed %d items in %d milliseconds.", s3AdapterListObjectsOutput.size(),
                                   System.currentTimeMillis() - start));

        Assert.assertEquals("Wrong UTF-8 list output.", utf8SortedItems, s3AdapterListObjectsOutput);

        final List<String> stringSortedItems = new ArrayList<>(s3ListOutputItems);

        stringSortedItems.sort(Comparator.comparing(o -> o));

        Assert.assertEquals("Wrong plain String list output.", stringSortedItems, s3AdapterListObjectsOutput);
    }

    @Test
    public void put() throws Exception {
        final URI testURI = URI.create(String.format("cadc:%s/%s.fits", BUCKET_NAME, UUID.randomUUID().toString()));
        try {
            final S3StorageAdapter putTestSubject = new S3StorageAdapter(ENDPOINT, REGION);
            final NewArtifact artifact = new NewArtifact(testURI);

            artifact.contentChecksum = URI.create("md5:9307240a34ed65a0a252b0046b6e87be");
            artifact.contentLength = 312151680L;

            final URL sourceURL = new URL("https://www.cadc-ccda.hia-iha.nrc-cnrc.gc" +
                                          ".ca/files/vault/CADCtest/Public/test-megaprime.fits.fz");
            final InputStream inputStream = openStream(sourceURL);

            LOGGER.info(String.format("PUTting file from %s.", sourceURL.toExternalForm()));
            final StorageMetadata storageMetadata = putTestSubject.put(artifact, inputStream);

            inputStream.close();

            final URI resultChecksum = storageMetadata.getContentChecksum();
            final long resultLength = storageMetadata.getContentLength();

            Assert.assertEquals("Checksum does not match.", artifact.contentChecksum, resultChecksum);
            Assert.assertEquals("Lengths do not match.", artifact.contentLength.longValue(), resultLength);

            // Get it out again.
            final S3StorageAdapter getTestSubject = new S3StorageAdapter(ENDPOINT, REGION);

            final OutputStream outputStream = new ByteArrayOutputStream();
            final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream,
                                                                                 MessageDigest.getInstance(
                                                                                         S3StorageAdapterTest.DIGEST_ALGORITHM));
            final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
            final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

            getTestSubject.get(storageMetadata.getStorageLocation(), byteCountOutputStream);
            Assert.assertEquals("Retrieved file is not the same.", artifact.contentLength.longValue(),
                                byteCountOutputStream.getByteCount());
            Assert.assertEquals("Wrong checksum.", artifact.contentChecksum,
                                URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                                                         new BigInteger(1, messageDigest.digest()).toString(16))));
        } finally {
            final S3StorageAdapter deleteTestSubject = new S3StorageAdapter(ENDPOINT, REGION);
            try {
                deleteTestSubject.delete(new StorageLocation(testURI));
            } catch (Exception e) {
                // Oh well.
            }
        }
    }

    private InputStream openStream(final URL sourceURL) throws Exception {
        final HttpURLConnection sourceURLConnection = (HttpURLConnection) sourceURL.openConnection();
        sourceURLConnection.setDoInput(true);
        sourceURLConnection.setInstanceFollowRedirects(true);

        final String location = sourceURLConnection.getHeaderField("Location");
        if (StringUtil.hasText(location)) {
            return openStream(new URL(location));
        } else {
            return sourceURLConnection.getInputStream();
        }
    }


    /**
     * Exists to test what happens when the InputStream from Reading of a file breaks.  This was used to source a file
     * from another host, and mid-read, killing the source host.
     *
     * @throws Exception Any exception.
     */
    @Test
    @Ignore
    public void ensureFileStreamReadBreak() throws Exception {
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

                final URL sourceURL = new URL("http://mach378.cadc.dao.nrc.ca:9000/test-megaprime.fits.fz");

                //final URL sourceURL = new URL("https://www.cadc-ccda.hia-iha.nrc-cnrc.gc" +
                //                              ".ca/files/vault/CADCtest/Public/test-megaprime.fits.fz");

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
    }

    private void ensureMEFTestFile() throws Exception {
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
    }

    @Test
    public void get() throws Exception {
        final S3StorageAdapter testSubject = new S3StorageAdapter(ENDPOINT, REGION);
        final URI testURI = URI.create(String.format("s3:%s", "03e6ab81-ea89-4098-8eae-1267bb52c50a"));
        final long expectedByteCount = 3144960L;
        final URI expectedChecksum = URI.create("md5:9307240a34ed65a0a252b0046b6e87be");

        final OutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(S3StorageAdapterTest.DIGEST_ALGORITHM));
        final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
        final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

        final StorageLocation storageLocation = new StorageLocation(testURI);
        storageLocation.storageBucket = "fed08";

        testSubject.get(storageLocation, byteCountOutputStream);

        Assert.assertEquals("Wrong byte count.", expectedByteCount, byteCountOutputStream.getByteCount());
        Assert.assertEquals("Wrong checksum.", expectedChecksum,
                            URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                                                     new BigInteger(1, messageDigest.digest()).toString(16))));
    }

    @Test
    public void jumpHDUs() throws Exception {
        LOGGER.info("Jumping HDUS...");
        LOGGER.info("***");
        ensureMEFTestFile();
        final Map<Integer, Long> hduByteOffsets = new HashMap<>();
        hduByteOffsets.put(0, 0L);
        hduByteOffsets.put(19, 148380480L);
        hduByteOffsets.put(40, 312117120L);

        try (S3Client s3Client = S3Client.builder()
                                         .endpointOverride(ENDPOINT)
                                         .region(Region.of(REGION))
                                         .build()) {
            for (final Map.Entry<Integer, Long> entry : hduByteOffsets.entrySet()) {
                LOGGER.info(String.format("\nReading %d bytes at %d.\n", 2880, entry.getValue()));
                final long start = System.currentTimeMillis();
                s3Client.getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(MEF_OBJECT_ID)
                                                   .range(String.format("bytes=%d-%d",
                                                                        entry.getValue(),
                                                                        entry.getValue() + 2880L))
                                                   .build(),
                                   ResponseTransformer.toBytes());
                final long readTime = System.currentTimeMillis() - start;
                LOGGER.info(String.format("\nRead time for HDU %d is %d.\n", entry.getKey(), readTime));
            }
        }

        LOGGER.info("***");
        LOGGER.info("Jumping HDUS done.");
    }

    @Test
    public void jumpHDUsReconnect() throws Exception {
        LOGGER.info("Jumping HDUS with reconnect...");
        LOGGER.info("***");
        ensureMEFTestFile();
        final Map<Integer, Long> hduByteOffsets = new HashMap<>();
        hduByteOffsets.put(0, 0L);
        hduByteOffsets.put(19, 148380480L);
        hduByteOffsets.put(40, 312117120L);

        for (final Map.Entry<Integer, Long> entry : hduByteOffsets.entrySet()) {
            LOGGER.info(String.format("\nReading %d bytes at %d.\n", 2880, entry.getValue()));
            final S3Client innerS3Client = S3Client.builder()
                                                   .endpointOverride(ENDPOINT)
                                                   .region(Region.of(REGION))
                                                   .build();
            final long start = System.currentTimeMillis();
            final byte[] bytes = innerS3Client.getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(
                    MEF_OBJECT_ID)
                                                                         .range(String.format("bytes=%d-%d",
                                                                                              entry.getValue(),
                                                                                              entry.getValue() + 2880L))
                                                                         .build(),
                                                         ResponseTransformer.toBytes()).asByteArray();
            final long readTime = System.currentTimeMillis() - start;
            innerS3Client.close();
            LOGGER.info(
                    String.format("\nReading %d bytes for HDU %d is %d with reconnect.\n", bytes.length, entry.getKey(),
                                  readTime));
        }

        LOGGER.info("Jumping HDUS with reconnect done.");
        LOGGER.info("***");
    }

    @Test
    @Ignore
    public void getHeaders() throws Exception {
        LOGGER.info("Skip to headers...");
        LOGGER.info("***");
        ensureMEFTestFile();
        final S3StorageAdapter testSubject = new S3StorageAdapter(ENDPOINT, REGION);
        final URI testURI = URI.create(String.format("cadc:%s/%s", BUCKET_NAME, MEF_OBJECT_ID));

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(S3StorageAdapterTest.DIGEST_ALGORITHM));
        final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);

        final Set<String> cutouts = new HashSet<>();
        cutouts.add("fhead");

        testSubject.get(new StorageLocation(testURI), byteCountOutputStream, cutouts);
        byteCountOutputStream.close();
        LOGGER.info("***");
        LOGGER.info("Skip to headers done.");
    }

    @Test
    @Ignore("Not currently supported.")
    public void getCutouts() throws Exception {
        final S3StorageAdapter testSubject = new S3StorageAdapter(ENDPOINT, REGION);
        final URI testURI = URI.create("cadc:jenkinsd/test-hst-mef.fits");
        final long expectedByteCount = 159944L;
        final URI expectedChecksum = URI.create("md5:7c6372a8d20da28b54b6b50ce36f9195");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(S3StorageAdapterTest.DIGEST_ALGORITHM));
        final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
        final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

        final Set<String> cutouts = new HashSet<>();
        //cutouts.add("[SCI,10][80:220,100:150]");
        //cutouts.add("[1][10:16,70:90]");
        //cutouts.add("[106][8:32,88:112]");
        //cutouts.add("[126]");

        testSubject.get(new StorageLocation(testURI), byteCountOutputStream, cutouts);
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

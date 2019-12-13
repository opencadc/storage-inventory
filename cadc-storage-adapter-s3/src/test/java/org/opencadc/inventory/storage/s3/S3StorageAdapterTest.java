
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

package org.opencadc.inventory.storage.s3;

import nom.tam.fits.BasicHDU;
import nom.tam.fits.Fits;
import nom.tam.util.ArrayDataInput;
import nom.tam.util.BufferedDataInputStream;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageMetadata;
import software.amazon.awssdk.regions.Region;

import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.util.FileUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class S3StorageAdapterTest {

    private static final URI ENDPOINT = URI.create("http://dao-wkr-04.cadc.dao.nrc.ca:8080");
    private static final String REGION = Region.US_EAST_1.id();

    @Test
    @Ignore
    public void get() throws Exception {
        final S3StorageAdapter testSubject = new S3StorageAdapter(ENDPOINT, REGION);
        final URI testURI = URI.create("cadc:jenkinsd/test-jcmt.fits");
        final long expectedByteCount = 3144960L;
        final URI expectedChecksum = URI.create("md5:9307240a34ed65a0a252b0046b6e87be");

        final OutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(S3StorageAdapter.DIGEST_ALGORITHM));
        final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
        final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

        testSubject.get(new StorageLocation(testURI), byteCountOutputStream);

        Assert.assertEquals("Wrong byte count.", expectedByteCount, byteCountOutputStream.getByteCount());
        Assert.assertEquals("Wrong checksum.", expectedChecksum,
                            URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                                                     new BigInteger(1, messageDigest.digest()).toString(16))));
    }

    @Test
    public void getHeaders() throws Exception {
        final S3StorageAdapter testSubject = new S3StorageAdapter(ENDPOINT, REGION);
        final String fileName = System.getProperty("file.name");
        final URI testURI = URI.create(
                String.format("cadc:jenkinsd/%s", fileName == null ? "test-megaprime-s3.fits.fz" : fileName));

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(S3StorageAdapter.DIGEST_ALGORITHM));
        final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
        //final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

        final Set<String> cutouts = new HashSet<>();
        cutouts.add("fhead");

        testSubject.get(new StorageLocation(testURI), byteCountOutputStream, cutouts);
        byteCountOutputStream.close();
    }

    @Test
    @Ignore
    public void getCutouts() throws Exception {
        final S3StorageAdapter testSubject = new S3StorageAdapter(ENDPOINT, REGION);
        final URI testURI = URI.create("cadc:jenkinsd/test-hst-mef.fits");
        final long expectedByteCount = 159944L;
        final URI expectedChecksum = URI.create("md5:7c6372a8d20da28b54b6b50ce36f9195");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(S3StorageAdapter.DIGEST_ALGORITHM));
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

    @Test
    @Ignore
    public void put() throws Exception {
        final S3StorageAdapter putTestSubject = new S3StorageAdapter(ENDPOINT, REGION);
        final URI testURI = URI.create("site:jenkinsd/test-jcmt.fits");
        final File file = FileUtil.getFileFromResource("test-jcmt.fits", S3StorageAdapterTest.class);
        final NewArtifact artifact = new NewArtifact(testURI);

        artifact.contentChecksum = URI.create("md5:9307240a34ed65a0a252b0046b6e87be");
        artifact.contentLength = 3144960L;

        final InputStream inputStream = new FileInputStream(file);
        try {
            putTestSubject.delete(new StorageLocation(testURI));
        } catch (IOException e) {
            // Doesn't exist.  Good!
        }
        final StorageMetadata storageMetadata = putTestSubject.put(artifact, inputStream);
        inputStream.close();

        final URI resultChecksum = storageMetadata.getContentChecksum();
        final long resultLength = storageMetadata.getContentLength();

        Assert.assertEquals("Checksum does not match.", artifact.contentChecksum, resultChecksum);
        Assert.assertEquals("Lengths do not match.", artifact.contentLength.longValue(), resultLength);

        // Get it out again.
        final S3StorageAdapter getTestSubject = new S3StorageAdapter(ENDPOINT, REGION);

        final OutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(S3StorageAdapter.DIGEST_ALGORITHM));
        final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
        final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

        getTestSubject.get(new StorageLocation(testURI), byteCountOutputStream);
        Assert.assertEquals("Retrieved file is not the same.", artifact.contentLength.longValue(),
                            byteCountOutputStream.getByteCount());
        Assert.assertEquals("Wrong checksum.", artifact.contentChecksum,
                            URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                                                     new BigInteger(1, messageDigest.digest()).toString(16))));
    }

    private static class TestByteCountInputStreamWrapper implements InputStreamWrapper {

        private final int expectedByteCount;

        public TestByteCountInputStreamWrapper(final int expectedByteCount) {
            this.expectedByteCount = expectedByteCount;
        }

        /**
         * Read the bytes of the inputStream.
         *
         * @param inputStream The InputStream to read bytes from.
         */
        @Override
        public void read(InputStream inputStream) throws IOException {
            final byte[] buffer = new byte[8092];
            int byteCount = 0;
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) >= 0) {
                byteCount += bytesRead;
            }

            Assert.assertEquals(String.format("Should have %d bytes.", expectedByteCount), expectedByteCount,
                                byteCount);
        }
    }
}


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
import nom.tam.fits.FitsException;
import nom.tam.fits.Header;
import nom.tam.fits.HeaderCard;
import nom.tam.fits.ImageData;
import nom.tam.fits.ImageHDU;
import nom.tam.image.StandardImageTiler;
import nom.tam.util.ArrayDataOutput;
import nom.tam.util.BufferedDataOutputStream;
import org.apache.log4j.Logger;
import org.opencadc.inventory.storage.Cutout;
import org.opencadc.inventory.storage.CutoutValidator;
import org.opencadc.inventory.storage.HDULocation;
import org.opencadc.inventory.storage.NamedHDULocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.NumberedHDULocation;
import org.opencadc.inventory.storage.PrimaryHDULocation;
import org.opencadc.inventory.storage.ReadException;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.WriteException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageMetadata;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import ca.nrc.cadc.io.ByteCountInputStream;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.StringUtil;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.math.BigInteger;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/**
 * Implementation of a Storage Adapter using the Amazon S3 API.
 */
public class S3StorageAdapter implements StorageAdapter {

    private static final Logger LOGGER = Logger.getLogger(S3StorageAdapter.class);
    static final String DIGEST_ALGORITHM = "MD5";
    private static final int BUFFER_SIZE_BYTES = 8192;

    // S3Client is thread safe, and re-usability is encouraged.
    private final S3Client s3Client;


    public S3StorageAdapter(final URI endpoint, final String regionName) {
        this(S3Client.builder()
                     .endpointOverride(endpoint)
                     .region(Region.of(regionName))
                     .build());
    }

    public S3StorageAdapter(final S3Client s3Client) {
        this.s3Client = s3Client;
    }


    private String parseObjectID(final URI storageID) {
        final String path = storageID.getSchemeSpecificPart();
        final String[] pathItems = path.split("/");
        return pathItems[pathItems.length - 1];
    }

    private String parseBucket(final URI storageID) {
        final String path = storageID.getSchemeSpecificPart();
        final String[] pathItems = path.split("/");
        return pathItems[0];
    }

    private MessageDigest createDigester() {
        try {
            return MessageDigest.getInstance(DIGEST_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Unable to proceed with checksum implementation.", e);
        }
    }

    private URI createChecksum(final MessageDigest messageDigest) {
        return URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                                        new BigInteger(1, messageDigest.digest()).toString(16)));
    }

    private void transferInputStreamTo(final InputStream inputStream, final OutputStream outputStream)
            throws IOException {
        final byte[] buffer = new byte[BUFFER_SIZE_BYTES];
        int read;
        while ((read = inputStream.read(buffer, 0, BUFFER_SIZE_BYTES)) >= 0) {
            outputStream.write(buffer, 0, read);
        }
    }

    @Override
    public void get(StorageLocation storageLocation, OutputStream outputStream)
            throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException,
                   TransientException {
        final URI storageID = storageLocation.getStorageID();

        try (final InputStream inputStream = s3Client.getObject(GetObjectRequest.builder()
                                                                                .bucket(parseBucket(storageID))
                                                                                .key(parseObjectID(storageID))
                                                                                .build(),
                                                                ResponseTransformer.toInputStream())) {

            transferInputStreamTo(inputStream, outputStream);
        } catch (NoSuchKeyException e) {
            throw new ResourceNotFoundException(e.getMessage(), e);
        } catch (S3Exception | SdkClientException e) {
            throw new StorageEngageException(e.getMessage(), e);
        } catch (IOException e) {
            throw new ReadException(e.getMessage(), e);
        }
    }

    @Override
    public void get(StorageLocation storageLocation, OutputStream outputStream, Set<String> cutouts)
            throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException,
                   TransientException {
        if ((cutouts == null) || cutouts.isEmpty()) {
            get(storageLocation, outputStream);
        } else {
            final URI storageID = storageLocation.getStorageID();
            final long start = System.currentTimeMillis();
            try (final InputStream inputStream = s3Client.getObject(GetObjectRequest.builder()
                                                                                    .bucket(parseBucket(storageID))
                                                                                    .key(parseObjectID(storageID))
                                                                                    .build(),
                                                                    ResponseTransformer.toInputStream())) {

                final Fits fitsFile = new Fits(inputStream);
                final ArrayDataOutput dataOutput = new BufferedDataOutputStream(outputStream);

                // Just get the first cutout for now.
                BasicHDU<?> hdu;
                long beforeHDU = System.currentTimeMillis();
                int count = 1;
                while ((hdu = fitsFile.readHDU()) != null) {
                    final Header header = hdu.getHeader();
                    final long afterReadHDU = System.currentTimeMillis();
                    final HeaderCard headerNameCard = header.findCard("EXTNAME");
                    log(String.format("Read HDU %d (%s) in %d milliseconds.", count, headerNameCard == null
                                                                                     ? "N/A"
                                                                                     : headerNameCard.getValue(),
                                      afterReadHDU - beforeHDU));
                    if (hdu.getAxes() != null) {
                        final int axesCount = hdu.getAxes().length;
                        for (int i = 0; i < axesCount; i++) {
                            header.findCard(String.format("NAXIS%d", i + 1)).setValue(0);
                        }
                    }

                    final long beforeWriteHDU = System.currentTimeMillis();
                    header.write(dataOutput);
                    dataOutput.write(new short[0]);
                    log(String.format("Wrote HDU %d (%s) in %d milliseconds.", count, headerNameCard == null
                                                                                      ? "N/A"
                                                                                      : headerNameCard.getValue(),
                                      System.currentTimeMillis() - beforeWriteHDU));
                    beforeHDU = System.currentTimeMillis();
                    count++;
                }
            } catch (FitsException e) {
                throw new ReadException("Unable to process FITS file.", e);
            } catch (NoSuchKeyException e) {
                throw new ResourceNotFoundException(e.getMessage(), e);
            } catch (S3Exception | SdkClientException e) {
                throw new StorageEngageException(e.getMessage(), e);
            } catch (IOException e) {
                throw new ReadException(e.getMessage(), e);
            }
            log(String.format("Read and wrote HDUs in %d milliseconds.", System.currentTimeMillis() - start));
        }
    }

    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream inputStream)
            throws StreamCorruptedException, ReadException, WriteException, StorageEngageException, TransientException {
        final URI storageID = newArtifact.getArtifactURI();
        final DigestInputStream digestInputStream = new DigestInputStream(inputStream, createDigester());
        final ByteCountInputStream byteCountInputStream = new ByteCountInputStream(digestInputStream);
        final InputStream bufferedInputStream = new BufferedInputStream(byteCountInputStream);

        try {
            s3Client.putObject(PutObjectRequest.builder()
                                               .bucket(parseBucket(storageID))
                                               .key(parseObjectID(storageID))
                                               .build(),
                               RequestBody.fromInputStream(bufferedInputStream, newArtifact.contentLength));

            final URI expectedChecksum = newArtifact.contentChecksum;
            final URI calculatedChecksum = createChecksum(digestInputStream.getMessageDigest());

            if (expectedChecksum == null) {
                LOGGER.debug("No checksum provided.  Defaulting the calculated one.");
            } else if (!expectedChecksum.equals(calculatedChecksum)) {
                throw new StreamCorruptedException(
                        String.format("Checksums do not match.  Expected %s but was %s.", expectedChecksum.toString(),
                                      calculatedChecksum.toString()));
            }

            final Long expectedContentLength = newArtifact.contentLength;
            final long calculatedContentLength = byteCountInputStream.getByteCount();

            if (expectedContentLength == null) {
                LOGGER.debug("No content length provided.  Defaulting the calculated one.");
            } else if (expectedContentLength != calculatedContentLength) {
                throw new StreamCorruptedException(
                        String.format("Content lengths do not match.  Expected %d but was %d.", expectedContentLength,
                                      calculatedContentLength));
            }

            return new StorageMetadata(new StorageLocation(storageID), calculatedChecksum, calculatedContentLength);
        } catch (S3Exception | SdkClientException e) {
            throw new StorageEngageException(e.getMessage(), e);
        } catch (IOException e) {
            throw new WriteException(e.getMessage(), e);
        }
    }

    @Override
    public void delete(StorageLocation storageLocation)
            throws ResourceNotFoundException, IOException, StorageEngageException, TransientException {
    }

    @Override
    public Iterator<StorageMetadata> iterator()
            throws ReadException, WriteException, StorageEngageException, TransientException {
        return null;
    }

    @Override
    public Iterator<StorageMetadata> iterator(String s)
            throws ReadException, WriteException, StorageEngageException, TransientException {
        return null;
    }

    @Override
    public Iterator<StorageMetadata> unsortedIterator(String s)
            throws ReadException, WriteException, StorageEngageException, TransientException {
        return null;
    }

    void log(final String message) {
        System.out.println(message);
    }
}

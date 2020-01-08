
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
import nom.tam.util.ArrayDataOutput;
import nom.tam.util.BufferedDataOutputStream;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.ReadException;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.WriteException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageMetadata;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


/**
 * Implementation of a Storage Adapter using the Amazon S3 API.
 */
public class S3StorageAdapter implements StorageAdapter {

    private static final Logger LOGGER = Logger.getLogger(S3StorageAdapter.class);

    static final String DIGEST_ALGORITHM = "MD5";
    private static final int BUFFER_SIZE_BYTES = 8192;
    private static final int DEFAULT_BUCKET_HASH_LENGTH = 5;


    // S3Client is thread safe, and re-usability is encouraged.
    private final S3Client s3Client;


    public S3StorageAdapter(final URI endpoint, final String regionName) {
        this(S3Client.builder()
                     .endpointOverride(endpoint)
                     .region(Region.of(regionName))
                     .build());
    }

    S3StorageAdapter(final S3Client s3Client) {
        this.s3Client = s3Client;
    }


    private String parseKey(final URI storageID) {
        final String path = storageID.getSchemeSpecificPart();
        final String[] pathItems = path.split("/");
        return pathItems[pathItems.length - 1];
    }

    private String parseBucket(final URI storageID) {
        return InventoryUtil.computeBucket(storageID, 5);
    }

    String generateObjectID() {
        return UUID.randomUUID().toString();
    }

    private MessageDigest createDigester() {
        try {
            return MessageDigest.getInstance(DIGEST_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Unable to proceed with checksum implementation.", e);
        }
    }

    private void transferInputStreamTo(final InputStream inputStream, final OutputStream outputStream)
            throws IOException {
        final byte[] buffer = new byte[BUFFER_SIZE_BYTES];
        int read;
        while ((read = inputStream.read(buffer, 0, BUFFER_SIZE_BYTES)) >= 0) {
            outputStream.write(buffer, 0, read);
        }
    }

    /**
     * Get from storage the artifact identified by storageLocation.
     *
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest            The destination stream.
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException             If the storage system failed to stream.
     * @throws StorageEngageException    If the adapter failed to interact with storage.
     */
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest)
            throws ResourceNotFoundException, ReadException, StorageEngageException {
        final URI storageID = storageLocation.getStorageID();

        try (final InputStream inputStream = s3Client.getObject(GetObjectRequest.builder()
                                                                                .bucket(parseBucket(storageID))
                                                                                .key(parseKey(storageID))
                                                                                .build(),
                                                                ResponseTransformer.toInputStream())) {

            transferInputStreamTo(inputStream, dest);
        } catch (NoSuchKeyException e) {
            throw new ResourceNotFoundException(e.getMessage(), e);
        } catch (S3Exception | SdkClientException e) {
            throw new StorageEngageException(e.getMessage(), e);
        } catch (IOException e) {
            throw new ReadException(e.getMessage(), e);
        }
    }

    /**
     * Get from storage the artifact identified by storageLocation.
     *
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest            The destination stream.
     * @param cutouts         Cutouts to be applied to the artifact
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException             If the storage system failed to stream.
     * @throws StorageEngageException    If the adapter failed to interact with storage.
     */
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest, Set<String> cutouts)
            throws ResourceNotFoundException, ReadException, StorageEngageException {
        if ((cutouts == null) || cutouts.isEmpty()) {
            get(storageLocation, dest);
        } else {
            final URI storageID = storageLocation.getStorageID();
            final long start = System.currentTimeMillis();
            try (final ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(
                    GetObjectRequest.builder()
                                    .bucket(parseBucket(storageID))
                                    .key(parseKey(storageID))
                                    .build(),
                    ResponseTransformer.toInputStream())) {

                final Fits fitsFile = new Fits(inputStream);
                final ArrayDataOutput dataOutput = new BufferedDataOutputStream(dest);

                // Just get the first cutout for now.
                BasicHDU<?> hdu;
                long beforeHDU = System.currentTimeMillis();
                int count = 1;
                while ((hdu = fitsFile.readHDU()) != null) {
                    final Header header = hdu.getHeader();
                    final long afterReadHDU = System.currentTimeMillis();
                    final HeaderCard headerNameCard = header.findCard("EXTNAME");
                    LOGGER.debug(String.format("%d,\"%s\",%d,\"milliseconds\"",
                                               count,
                                               headerNameCard == null ? "N/A" : headerNameCard.getValue(),
                                               afterReadHDU - beforeHDU));
                    beforeHDU = System.currentTimeMillis();
                    if (hdu.getAxes() != null) {
                        final int axesCount = hdu.getAxes().length;
                        for (int i = 0; i < axesCount; i++) {
                            header.findCard(String.format("NAXIS%d", i + 1)).setValue(0);
                        }
                    }

                    header.write(dataOutput);
                    dataOutput.write(new short[0]);
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
            LOGGER.debug(String.format("Read and wrote HDUs in %d milliseconds.", System.currentTimeMillis() - start));
        }
    }

    /**
     * Write an artifact to storage.
     * The value of storageBucket in the returned StorageMetadata and StorageLocation can be used to
     * retrieve batches of artifacts in some of the iterator signatures defined in this interface.
     * Batches of artifacts can be listed by bucket in two of the iterator methods in this interface.
     * If storageBucket is null then the caller will not be able perform bucket-based batch
     * validation through the iterator methods.
     *
     * @param newArtifact The holds information about the incoming artifact.  If the contentChecksum
     *                    and contentLength are set, they will be used to validate the bytes received.
     * @param source      The stream from which to read.
     * @return The storage metadata.
     *
     * @throws StreamCorruptedException If the calculated checksum does not the expected checksum.
     * @throws ReadException            If the client failed to stream.
     * @throws WriteException           If the storage system failed to stream.
     * @throws StorageEngageException   If the adapter failed to interact with storage.
     */
    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source)
            throws StreamCorruptedException, ReadException, WriteException, StorageEngageException {
        final String objectID = generateObjectID();
        final String bucket = InventoryUtil.computeBucket(objectID, DEFAULT_BUCKET_HASH_LENGTH);

        try {
            final PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
                                                                                     .bucket(bucket)
                                                                                     .key(objectID);

            final Map<String, String> metadata = new HashMap<>();
            metadata.put("uri", newArtifact.getArtifactURI().toASCIIString().trim());

            if (newArtifact.contentChecksum != null) {
                putObjectRequestBuilder.contentMD5(newArtifact.contentChecksum.getSchemeSpecificPart());
                metadata.put("md5", newArtifact.contentChecksum.toString());
            }

            if (newArtifact.contentLength != null) {
                putObjectRequestBuilder.contentLength(newArtifact.contentLength);
            }

            putObjectRequestBuilder.metadata(metadata);

            s3Client.putObject(putObjectRequestBuilder.build(), RequestBody.fromInputStream(source,
                                                                                            newArtifact.contentLength));

            return head(bucket, objectID);
        } catch (S3Exception e) {
            throw new StorageEngageException(e.getMessage(), e);
        } catch (SdkClientException e) {
            throw new WriteException(e.getMessage(), e);
        }
    }

    StorageMetadata head(final String bucket, final String key) {
        final HeadObjectRequest.Builder headBuilder = HeadObjectRequest.builder().key(key);
        final HeadObjectResponse headResponse = s3Client.headObject(StringUtil.hasLength(bucket)
                                                                    ? headBuilder.bucket(bucket).build()
                                                                    : headBuilder.build());
        final String artifactURIMetadataValue = headResponse.metadata().get("uri");
        final URI metadataArtifactURI = StringUtil.hasLength(artifactURIMetadataValue)
                                        ? URI.create(artifactURIMetadataValue)
                                        : URI.create(String.format("%s:%s/%s", "UNKNOWN", bucket, key));
        final String md5ChecksumValue = headResponse.sseCustomerKeyMD5();
        final URI md5 = URI.create(String.format("%s:%s", "md5", StringUtil.hasLength(md5ChecksumValue)
                                                                 ? md5ChecksumValue
                                                                 : headResponse.eTag().replaceAll("\"", "")));

        return new StorageMetadata(new StorageLocation(metadataArtifactURI), md5, headResponse.contentLength());
    }

    /**
     * Delete from storage the artifact identified by storageLocation.
     *
     * @param storageLocation Identifies the artifact to delete.
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException               If an unrecoverable error occurred.
     * @throws StorageEngageException    If the adapter failed to interact with storage.
     * @throws TransientException        If an unexpected, temporary exception occurred.
     */
    public void delete(StorageLocation storageLocation)
            throws ResourceNotFoundException, IOException, StorageEngageException, TransientException {
        final DeleteObjectRequest.Builder deleteObjectRequestBuilder = DeleteObjectRequest.builder();
        if (StringUtil.hasLength(storageLocation.storageBucket)) {
            deleteObjectRequestBuilder.bucket(storageLocation.storageBucket);
        }

        deleteObjectRequestBuilder.key(parseKey(storageLocation.getStorageID()));

        try {
            s3Client.deleteObject(deleteObjectRequestBuilder.build());
        } catch (S3Exception e) {
            throw new StorageEngageException(e.getMessage(), e);
        } catch (SdkClientException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    ListObjectsResponse listObjects(final String storageBucket, final String nextMarker) {
        final ListObjectsRequest.Builder listBuilder = ListObjectsRequest.builder();

        listBuilder.bucket(storageBucket);

        if (StringUtil.hasLength(nextMarker)) {
            listBuilder.marker(nextMarker);
        }

        return s3Client.listObjects(listBuilder.build());
    }

    /**
     * Iterator of items ordered by their storageIDs.
     *
     * @return An iterator over an ordered list of items in storage.
     */
    @Override
    public Iterator<StorageMetadata> iterator() {
        return iterator(null);
    }

    /**
     * Iterator of items ordered by their storageIDs in the given bucket.
     *
     * @param storageBucket Only iterate over items in this bucket.
     * @return An iterator over an ordered list of items in this storage bucket.
     */
    @Override
    public Iterator<StorageMetadata> iterator(final String storageBucket) {
        return new S3StorageMetadataIterator(this, storageBucket);
    }

    /**
     * An unordered iterator of items in the given bucket.
     *
     * @param storageBucket Only iterate over items in this bucket.
     * @return An iterator over an ordered list of items in this storage bucket.
     */
    @Override
    public Iterator<StorageMetadata> unsortedIterator(final String storageBucket) {
        return iterator(storageBucket);
    }
}

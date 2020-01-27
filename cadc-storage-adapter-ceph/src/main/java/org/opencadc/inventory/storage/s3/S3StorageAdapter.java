
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

import ca.nrc.cadc.net.IncorrectContentChecksumException;
import ca.nrc.cadc.net.IncorrectContentLengthException;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import nom.tam.fits.BasicHDU;
import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.Header;
import nom.tam.fits.HeaderCard;
import nom.tam.util.ArrayDataOutput;
import nom.tam.util.BufferedDataOutputStream;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.ReadException;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.WriteException;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;


/**
 * Implementation of a Storage Adapter using the Amazon S3 API.
 */
public class S3StorageAdapter implements StorageAdapter {

    private static final Logger LOGGER = Logger.getLogger(S3StorageAdapter.class);

    public static final String CONFIG_FILE = "cadc-storage-adapter-ceph.properties";
    public static final String CONFIG_PROPERTY_BUCKETDEPTH = "bucketLength";

    private static final int BUFFER_SIZE_BYTES = 8192;
    private static final String DEFAULT_CHECKSUM_ALGORITHM = "md5";
    private static final String ARTIFACT_URI_KEY = "uri";
    private static final String DEFAULT_BUCKET_HASH_LENGTH = "5";
    static final String STORAGE_ID_URI_TEMPLATE = "s3:%s";
    static final String CHECKSUM_URI_TEMPLATE = "md5:%s";


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


    URI generateStorageID() {
        return URI.create(String.format(S3StorageAdapter.STORAGE_ID_URI_TEMPLATE, UUID.randomUUID().toString()));
    }

    /**
     * Obtain the configured bucket name length.
     * @return  Integer bucket length.  Never null.
     */
    private int getBucketNameLength() {
        final PropertiesReader pr = new PropertiesReader(CONFIG_FILE);
        final Optional<String> optionalBucketLength =
                Optional.ofNullable(pr.getFirstPropertyValue(CONFIG_PROPERTY_BUCKETDEPTH));
        return Integer.parseInt(optionalBucketLength.orElse(DEFAULT_BUCKET_HASH_LENGTH));
    }

    /**
     * Read a page of data into buffer.  Exists to properly distinguish exceptions.
     *
     * @param inputStream Stream to read from.
     * @param buffer      Buffer of data to read into.
     * @return int bytes read, or -1 when finished.
     *
     * @throws ReadException For any reading I/O errors.
     */
    private int readNextPage(final InputStream inputStream, final byte[] buffer) throws ReadException {
        try {
            return inputStream.read(buffer, 0, BUFFER_SIZE_BYTES);
        } catch (IOException e) {
            throw new ReadException(e.getMessage(), e);
        }
    }

    /**
     * Write a page of data from a buffer into a stream.  Exists to properly distinguish exceptions.
     *
     * @param outputStream The Stream to write to.
     * @param buffer       The buffer of data to read from.
     * @param length       The length of data to write.
     * @throws WriteException For any writing I/O errors.
     */
    private void writeNextPage(final OutputStream outputStream, final byte[] buffer, final int length)
            throws WriteException {
        try {
            outputStream.write(buffer, 0, length);
        } catch (IOException e) {
            throw new WriteException(e.getMessage(), e);
        }
    }

    private void transferInputStreamTo(final InputStream inputStream, final OutputStream outputStream)
            throws ReadException, WriteException {
        final byte[] buffer = new byte[BUFFER_SIZE_BYTES];
        int read;
        while ((read = readNextPage(inputStream, buffer)) >= 0) {
            writeNextPage(outputStream, buffer, read);
        }
    }

    /**
     * Obtain the InputStream for the given object.  Tests can override this method.
     *
     * @param storageLocation The Storage Location of the desired object.
     * @return InputStream to the object.
     */
    InputStream toObjectInputStream(final StorageLocation storageLocation) {
        return s3Client.getObject(GetObjectRequest.builder()
                                                  .bucket(storageLocation.storageBucket)
                                                  .key(storageLocation.getStorageID().getSchemeSpecificPart())
                                                  .build());
    }

    /**
     * Reusable way to create a StorageMetadata object.
     *
     * @param storageID     The URI storage ID.
     * @param bucket        The bucket name for later use.
     * @param artifactURI   The Artifact URI as it is in the inventory system.
     * @param md5           The MD5 URI.
     * @param contentLength The content length in bytes.
     * @return StorageMetadata instance; never null.
     */
    StorageMetadata toStorageMetadata(final URI storageID, final String bucket, final URI artifactURI,
                                      final URI md5, final long contentLength) {
        final StorageLocation storageLocation = new StorageLocation(storageID);
        storageLocation.storageBucket = bucket;

        final StorageMetadata storageMetadata = new StorageMetadata(storageLocation, md5, contentLength);
        storageMetadata.artifactURI = artifactURI;

        return storageMetadata;
    }

    /**
     * Get from storage the artifact identified by storageLocation.
     *
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest            The destination stream.
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException             If the storage system failed to stream.
     * @throws WriteException            If writing failed.
     * @throws StorageEngageException    If the adapter failed to interact with storage.
     */
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest)
            throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException {
        try (final InputStream inputStream = toObjectInputStream(storageLocation)) {
            transferInputStreamTo(inputStream, dest);
        } catch (NoSuchKeyException e) {
            throw new ResourceNotFoundException(e.getMessage(), e);
        } catch (S3Exception | SdkClientException e) {
            throw new StorageEngageException(e.getMessage(), e);
        } catch (ReadException | WriteException e) {
            // Handle before the IOException below so it's not wrapped into that catch.
            throw e;
        } catch (IOException e) {
            throw new ReadException(e.getMessage(), e);
        }
    }

    /**
     * Get from storage the artifact identified by storageLocation with cutout specifications.  Currently needs full
     * implementation.
     * TODO
     * TODO Complete the implementation to check for an astronomy file (i.e. FITS, HDF5), and determine what cutout
     * TODO system to use.
     * TODO
     * TODO jenkinsd 2020.01.09
     * TODO
     *
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest            The destination stream.
     * @param cutouts         Cutouts to be applied to the artifact
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException             If the storage system failed to stream.
     * @throws WriteException            If writing failed.
     * @throws StorageEngageException    If the adapter failed to interact with storage.
     */
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest, Set<String> cutouts)
            throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException {
        if ((cutouts == null) || cutouts.isEmpty()) {
            get(storageLocation, dest);
        } else {
            final long start = System.currentTimeMillis();
            try (final InputStream inputStream = toObjectInputStream(storageLocation)) {
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
     * Create a new bucket in the system.
     *
     * @param bucket The bucket name.
     * @throws ResourceAlreadyExistsException The bucket already exists.
     * @throws SdkClientException             If any client side error occurs such as an IO related failure, failure
     *                                        to get credentials, etc.
     * @throws S3Exception                    Base class for all service exceptions. Unknown exceptions will be
     *                                        thrown as an instance of this type.
     */
    void createBucket(final String bucket) throws ResourceAlreadyExistsException, SdkClientException, S3Exception {
        final CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucket).build();

        try {
            s3Client.createBucket(createBucketRequest);
        } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException e) {
            throw new ResourceAlreadyExistsException(String.format("Bucket with name %s already exists.\n%s\n",
                                                                   bucket, e.getMessage()), e);
        }
    }

    /**
     * Ensure the bucket for the given Object ID exists and return it.
     *
     * @param storageID The Storage ID to ensure a bucket for.
     * @return String name of the given Object's bucket.
     *
     * @throws ResourceAlreadyExistsException If the bucket needed to be created but already exists.  Should never
     *                                        really happen, but here for completeness.
     * @throws SdkClientException             If any client side error occurs such as an IO related failure, failure
     *                                        to get credentials, etc.
     * @throws S3Exception                    Base class for all service exceptions. Unknown exceptions will be
     *                                        thrown as an instance of this type.
     */
    String ensureBucket(final URI storageID) throws ResourceAlreadyExistsException, SdkClientException, S3Exception {
        final String bucket = InventoryUtil.computeBucket(storageID, getBucketNameLength());
        final HeadBucketRequest headBucketRequest = HeadBucketRequest.builder().bucket(bucket).build();

        try {
            s3Client.headBucket(headBucketRequest);
        } catch (NoSuchBucketException e) {
            // Bucket does not exist, so create it.
            createBucket(bucket);
        }

        return bucket;
    }

    void ensureChecksum(final URI artifactChecksumURI) throws UnsupportedOperationException {
        if (!artifactChecksumURI.getScheme().equals(DEFAULT_CHECKSUM_ALGORITHM)) {
            throw new UnsupportedOperationException(String.format("Only %s is supported, but found %s.",
                                                                  DEFAULT_CHECKSUM_ALGORITHM,
                                                                  artifactChecksumURI.getScheme()));
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
     * @throws IncorrectContentChecksumException If the checksum did not match what was written, or was not valid.
     * @throws IncorrectContentLengthException   If the bytes read did not match the bytes set in the Content-Length
     *                                           header.
     * @throws ReadException                     If the client failed to read the stream.
     * @throws WriteException                    If the storage system failed to stream.
     * @throws StorageEngageException            If the adapter failed to interact with storage.
     */
    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source)
            throws IncorrectContentChecksumException, IncorrectContentLengthException, ReadException, WriteException,
                   StorageEngageException {
        final URI storageID = generateStorageID();

        String checksum = "N/A";

        try {
            final String bucket = ensureBucket(storageID);

            final PutObjectRequest.Builder putObjectRequestBuilder =
                    PutObjectRequest.builder()
                                    .bucket(bucket)
                                    .key(storageID.getSchemeSpecificPart());

            final Map<String, String> metadata = new HashMap<>();
            metadata.put(ARTIFACT_URI_KEY, newArtifact.getArtifactURI().toASCIIString().trim());

            final URI newArtifactChecksum = newArtifact.contentChecksum;

            if (newArtifactChecksum != null) {
                ensureChecksum(newArtifactChecksum);
                final String md5SumValue = newArtifactChecksum.getSchemeSpecificPart();

                // Reconvert to Hex bytes before Base64 encoding it as per what S3 expects.
                final byte[] value = HexUtil.toBytes(md5SumValue);
                checksum = new String(Base64.getEncoder().encode(value));
                putObjectRequestBuilder.contentMD5(checksum);
                metadata.put(DEFAULT_CHECKSUM_ALGORITHM, md5SumValue);
            }

            /*
            TODO: Is this necessary?  It's being sent with the RequestBody.fromInputStream() below already.  S3 will
            TODO: throw an exception if it's missing.
            TODO: jenkinsd 2020.01.20
             */
            putObjectRequestBuilder.contentLength(newArtifact.contentLength);

            // Metadata are extended attributes.  So far this is "uri" and "md5".
            putObjectRequestBuilder.metadata(metadata);

            s3Client.putObject(putObjectRequestBuilder.build(), RequestBody.fromInputStream(source,
                                                                                            newArtifact.contentLength));

            return toStorageMetadata(storageID, bucket, newArtifact.getArtifactURI(), newArtifactChecksum,
                                     newArtifact.contentLength);
        } catch (S3Exception e) {
            final AwsErrorDetails awsErrorDetails = e.awsErrorDetails();
            if ((awsErrorDetails != null) && StringUtil.hasLength(awsErrorDetails.errorCode())) {
                final String errorCode = awsErrorDetails.errorCode();
                switch (errorCode) {
                    case "BadDigest": {
                        throw new IncorrectContentChecksumException(
                                String.format("Checksums do not match what was received.  Expected %s.", checksum));
                    }
                    case "IncompleteBody": {
                        throw new IncorrectContentLengthException(
                                String.format("Content length does not match bytes written.  Expected %d bytes.",
                                              newArtifact.contentLength));
                    }
                    case "InternalError": {
                        /*
                        TODO: Documentation says to Retry the request in this case.  Throwing a StorageEngageException
                        TODO: for now...
                        TODO: jenkinsd 2020.01.20
                         */
                        throw new StorageEngageException("Unknown error from the server.");
                    }
                    case "InvalidDigest": {
                        throw new IncorrectContentChecksumException(
                                String.format("Checksum (%s) is invalid.", checksum));
                    }
                    case "MissingContentLength": {
                        throw new IncorrectContentLengthException("Content length is missing but is mandatory.");
                    }
                    case "SlowDown":
                    case "ServiceUnavailable": {
                        throw new StorageEngageException(
                                String.format("Volume is too great for service to handle.  Reduce traffic (%s).",
                                              errorCode));
                    }
                    default: {
                        throw new WriteException(String.format("Error Code: %s\nMessage from server: %s\n", errorCode,
                                                               e.getMessage()), e);
                    }
                }
            } else {
                throw new WriteException(String.format("Unknown Internal Error:\nMessage from server: %s\n",
                                                       e.getMessage()), e);
            }
        } catch (SdkClientException e) {
            // Based on testing by reading from the stream of a URL, and killing the server part way through the
            // read while doing a pubObject().  The SdkClientException is thrown in that case.
            // jenkinsd 2020.01.20
            if (e.getMessage().toLowerCase().contains("read")) {
                throw new ReadException(String.format("Error while reading from stream.\n%s\n.", e.getMessage()), e);
            } else {
                throw new WriteException(
                        String.format("Unknown client side error:\nMessage from server: %s\n", e.getMessage()), e);
            }
        } catch (ResourceAlreadyExistsException e) {
            // Message from ResourceAlreadyExistsException is descriptive.  It will only ever get here if the hashcode
            // bucket already exists but we tried to create it anyway.  It should not happen.
            // jenkinsd 2020.01.20
            throw new WriteException(e.getMessage(), e);
        }
    }

    /**
     * Perform a head (headObject) request for the given key in the given bucket.  This is used to translate into a
     * StorageMetadata object to verify after a PUT and to translate S3Objects for a LIST function.
     *
     * @param bucket The bucket to look.
     * @param key    The key to look for.
     * @return StorageMetadata instance.  Never null
     */
    StorageMetadata head(final String bucket, final String key) {
        final HeadObjectRequest.Builder headBuilder = HeadObjectRequest.builder().key(key);
        final HeadObjectResponse headResponse = s3Client.headObject(StringUtil.hasLength(bucket)
                                                                    ? headBuilder.bucket(bucket).build()
                                                                    : headBuilder.build());
        final Map<String, String> objectMetadata = headResponse.metadata();
        final URI artifactURI = objectMetadata.containsKey(ARTIFACT_URI_KEY)
                                ? URI.create(objectMetadata.get(ARTIFACT_URI_KEY)) : null;

        final URI storageID = URI.create(String.format(S3StorageAdapter.STORAGE_ID_URI_TEMPLATE, key));

        // TODO: The MD5 value should always be present.  Do we need a null check here?
        final URI md5 = URI.create(String.format(CHECKSUM_URI_TEMPLATE,
                                                 objectMetadata.get(DEFAULT_CHECKSUM_ALGORITHM)));

        return toStorageMetadata(storageID, bucket, artifactURI, md5, headResponse.contentLength());
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
        final URI storageID = storageLocation.getStorageID();
        final DeleteObjectRequest.Builder deleteObjectRequestBuilder =
                DeleteObjectRequest.builder()
                                   .bucket(storageLocation.storageBucket)
                                   .key(storageID.getSchemeSpecificPart());
        try {
            final DeleteObjectResponse deleteObjectResponse = s3Client.deleteObject(deleteObjectRequestBuilder.build());

            // TODO: Figure out how a delete of a missing Key is handled.
            if (!Optional.ofNullable(deleteObjectResponse.deleteMarker()).orElse(Boolean.FALSE)) {
                throw new ResourceNotFoundException(String.format("No such key to delete \"%s\"", storageID));
            }
        } catch (S3Exception e) {
            throw new StorageEngageException(e.getMessage(), e);
        } catch (SdkClientException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Obtain a list of objects from the S3 server.  This will use the nextMarkerKey value to start listing the next
     * page of data from.  This is hard-coded to 1000 objects by default, but is modifiable to something smaller
     * using the <code>maxKeys(int)</code> call.
     * This is used by a StorageMetadata Iterator to list all objects from a bucket.
     *
     * @param storageBucket The bucket to pull from.
     * @param nextMarkerKey The optional key from which to start listing the next page of objects.
     * @return ListObjectsResponse instance.
     */
    ListObjectsResponse listObjects(final String storageBucket, final String nextMarkerKey) {
        final ListObjectsRequest.Builder listBuilder = ListObjectsRequest.builder();
        final Optional<String> optionalNextMarkerKey = Optional.ofNullable(nextMarkerKey);

        listBuilder.bucket(storageBucket);
        optionalNextMarkerKey.ifPresent(listBuilder::marker);

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

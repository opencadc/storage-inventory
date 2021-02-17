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

import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.MultiBufferIO;
import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.IncorrectContentChecksumException;
import ca.nrc.cadc.net.IncorrectContentLengthException;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.StringUtil;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Base implementation of a Storage Adapter using the Amazon S3 API. 
 */
abstract class S3StorageAdapter implements StorageAdapter {

    private static final Logger LOGGER = Logger.getLogger(S3StorageAdapterSB.class);
    
    private static final String CONFIG_FILENAME = "cadc-storage-adapter-s3.properties";

    private static final Region REGION = Region.of("default");

    // additional system properties required by subclass
    private static final String CONF_ENDPOINT = S3StorageAdapter.class.getName() + ".endpoint";
    private static final String CONF_SBLEN = S3StorageAdapter.class.getName() + ".bucketLength";
    private static final String CONF_S3BUCKET = S3StorageAdapter.class.getName() + ".s3bucket";

    static final int BUFFER_SIZE_BYTES = 8192;
    static final String DEFAULT_CHECKSUM_ALGORITHM = "md5";
    static final String CHECKSUM_KEY = "checksum";
    static final String ARTIFACT_URI_KEY = "uri";

    protected final URI s3endpoint;
    protected final int storageBucketLength;
    protected final String s3bucket;
    protected final S3Client s3client; // S3Client is thread safe
    
    // ctor for unit tests that do not connect to an S3 backend
    protected S3StorageAdapter(String s3bucket, int storageBucketLength) {
        this.s3bucket = s3bucket;
        this.storageBucketLength = storageBucketLength;
        this.s3endpoint = null;
        this.s3client = null;
    }
    
    protected S3StorageAdapter() {
        try {
            File config = new File(System.getProperty("user.home") + "/config/" + CONFIG_FILENAME);
            Properties props = new Properties();
            props.load(new FileReader(config));
            
            StringBuilder sb = new StringBuilder();
            sb.append("incomplete config: ");
            boolean ok = true;
            
            String suri = props.getProperty(CONF_ENDPOINT);
            sb.append("\n\t" + CONF_ENDPOINT + ": ");
            if (suri == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }

            String sbl = props.getProperty(CONF_SBLEN);
            sb.append("\n\t" + CONF_SBLEN + ": ");
            if (sbl == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }

            String s3b = props.getProperty(CONF_S3BUCKET);
            sb.append("\n\t" + CONF_S3BUCKET + ": ");
            if (s3b == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }
            
            final String accessKey = props.getProperty("aws.accessKeyId");
            sb.append("\n\taws.accessKeyId: ");
            if (accessKey == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }

            final String secretKey = props.getProperty("aws.secretAccessKey");
            sb.append("\n\taws.secretAccessKey: ");
            if (secretKey == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }
        
            if (!ok) {
                throw new IllegalStateException(sb.toString());
            }
           
            this.s3endpoint = new URI(suri);
            this.storageBucketLength = Integer.parseInt(sbl);
            this.s3bucket = s3b;

            S3ClientBuilder builder = S3Client.builder();
            // since we require the system properties used by the aws library this is not strictly necessary
            builder.credentialsProvider(new AwsCredentialsProvider() {
                @Override
                public AwsCredentials resolveCredentials() {
                    return new AwsCredentials() {
                        @Override
                        public String accessKeyId() {
                            return accessKey;
                        }

                        @Override
                        public String secretAccessKey() {
                            return secretKey;
                        }
                    };
                }
            });
            builder.endpointOverride(s3endpoint);
            builder.region(REGION);
            this.s3client = builder.build();
            
            checkConnectivity();
        } catch (Exception fatal) {
            throw new RuntimeException("invalid config", fatal);
        }
    }

    private void checkConnectivity() {
        ListBucketsResponse resp = s3client.listBuckets();
        LOGGER.debug("checkConfig: bucket owner is " + resp.owner());
    }
    
    static class InternalBucket {
        String name;
        
        InternalBucket(String name) {
            this.name = name; 
        }

        @Override
        public String toString() {
            return "InternalBucket[" + name + "]";
        }
    }
    
    protected abstract StorageLocation generateStorageLocation();

    protected abstract InternalBucket toInternalBucket(StorageLocation loc);
    
    protected abstract StorageLocation toExternal(InternalBucket bucket, String key);
    
    /**
     * Obtain the InputStream for the given object. Tests can override this method.
     *
     * @param storageLocation The Storage Location of the desired object.
     * @return InputStream to the object.
     */
    InputStream toObjectInputStream(final StorageLocation storageLocation) {
        return s3client.getObject(GetObjectRequest.builder()
                .bucket(toInternalBucket(storageLocation).name)
                .key(storageLocation.getStorageID().toASCIIString())
                .build());
    }

    /**
     * Reusable way to create a StorageMetadata object.
     *
     * @param storageID The URI storage ID.
     * @param bucket The bucket name for later use.
     * @param artifactURI The Artifact URI as it is in the inventory system.
     * @param md5 The MD5 URI.
     * @param contentLength The content length in bytes.
     * @return StorageMetadata instance; never null.
     */
    StorageMetadata toStorageMetadata(StorageLocation loc, URI md5, long contentLength, 
            final URI artifactURI, Date lastModified) {
        StorageMetadata storageMetadata = new StorageMetadata(loc, md5, contentLength);
        storageMetadata.artifactURI = artifactURI;
        storageMetadata.contentLastModified = lastModified;
        return storageMetadata;
    }

    /**
     * Get from storage the artifact identified by storageLocation.
     *
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest The destination stream.
     * 
     * @throws java.lang.InterruptedException if thread receives an interrupt
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If writing failed.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     */
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest)
            throws InterruptedException, ResourceNotFoundException, ReadException, WriteException, StorageEngageException {
        LOGGER.debug("get: " + storageLocation);
        try (final InputStream inputStream = toObjectInputStream(storageLocation)) {
            MultiBufferIO tio = new MultiBufferIO();
            tio.copy(inputStream, dest);
        } catch (NoSuchBucketException | NoSuchKeyException e) {
            throw new ResourceNotFoundException("not found: " + storageLocation);
        } catch (S3Exception | SdkClientException e) {
            throw new StorageEngageException(e.getMessage(), e);
        } catch (ReadException | WriteException e) {
            // Handle before the IOException below so it's not wrapped into that catch.
            throw e;
        } catch (IOException e) {
            throw new ReadException(e.getMessage(), e);
        }
    }

    @Override
    public void get(StorageLocation storageLocation, OutputStream dest, SortedSet<ByteRange> byteRanges) 
        throws InterruptedException, ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    // internal bucket management used for init, dynamic buckets, and intTest cleanup
    void createBucket(InternalBucket bucket) throws ResourceAlreadyExistsException, SdkClientException, S3Exception {
        final CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucket.name).build();

        try {
            s3client.createBucket(createBucketRequest);
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
     * @throws ResourceAlreadyExistsException If the bucket needed to be created but already exists
     * @throws SdkClientException If any client side error occurs such as an IO related failure, failure to get credentials, etc.
     * @throws S3Exception Base class for all service exceptions. Unknown exceptions will be thrown as an instance of this type.
     */
    void ensureBucket(InternalBucket bucket) throws ResourceAlreadyExistsException, SdkClientException, S3Exception {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder().bucket(bucket.name).build();
        try {
            s3client.headBucket(headBucketRequest);
        } catch (NoSuchBucketException e) {
            createBucket(bucket);
        }
    }
    
    void deleteBucket(InternalBucket bucket) throws ResourceNotFoundException {
        final DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket.name).build();
        try {
            s3client.deleteBucket(deleteBucketRequest);
        } catch (NoSuchBucketException e) {
            throw new ResourceNotFoundException("not found bucket " + bucket);
        }
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
     * @param newArtifact known information about the incoming artifact
     * @param source stream from which to read
     * @param transactionID null for auto-commit or existing transactionID
     * @return storage metadata after write
     *
     * @throws IncorrectContentChecksumException checksum of the data stream did not match the value in newArtifact
     * @throws IncorrectContentLengthException number bytes read did not match the value in newArtifact
     * @throws ReadException If the client failed to read the stream.
     * @throws WriteException If the storage system failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     */
    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source, String transactionID)
            throws ByteLimitExceededException, 
            IncorrectContentChecksumException, IncorrectContentLengthException, 
            ReadException, WriteException,
            StorageEngageException {
        InventoryUtil.assertNotNull(S3StorageAdapter.class, "newArtifact", newArtifact);
        LOGGER.debug("put: " + newArtifact);
        
        if (newArtifact.contentChecksum == null || newArtifact.contentLength == null) {
            throw new UnsupportedOperationException("put requires contentChecksum and contentLength");
        }
        
        if (transactionID != null) {
            throw new UnsupportedOperationException("put with transaction");
        }
        
        final StorageLocation loc = generateStorageLocation();

        String checksum = "N/A";

        try {
            InternalBucket b = toInternalBucket(loc);
            ensureBucket(b);

            final PutObjectRequest.Builder putObjectRequestBuilder
                    = PutObjectRequest.builder()
                    .bucket(b.name)
                    .key(loc.getStorageID().toASCIIString());

            final Map<String, String> metadata = new HashMap<>();
            metadata.put(ARTIFACT_URI_KEY, newArtifact.getArtifactURI().toASCIIString().trim());

            if (newArtifact.contentChecksum != null) {
                metadata.put(CHECKSUM_KEY, newArtifact.contentChecksum.toASCIIString());
                
                ensureChecksum(newArtifact.contentChecksum);
                final String md5SumValue = newArtifact.contentChecksum.getSchemeSpecificPart();
                // Reconvert to Hex bytes before Base64 encoding it as per what S3 expects.
                final byte[] value = HexUtil.toBytes(md5SumValue);
                checksum = new String(Base64.getEncoder().encode(value));
                putObjectRequestBuilder.contentMD5(checksum);
            } else { 
                // exception thrown above makes this not reachable (see TODO below)
                try {
                    MessageDigest md = MessageDigest.getInstance(DEFAULT_CHECKSUM_ALGORITHM);
                    DigestInputStream dis = new DigestInputStream(source, md);
                    source = dis;
                } catch (NoSuchAlgorithmException ex) {
                    throw new RuntimeException("failed to create MessageDigest: " + DEFAULT_CHECKSUM_ALGORITHM);
                }
            }

            putObjectRequestBuilder.contentLength(newArtifact.contentLength);
            putObjectRequestBuilder.metadata(metadata);
            PutObjectResponse putResponse = s3client.putObject(putObjectRequestBuilder.build(), RequestBody.fromInputStream(source, newArtifact.contentLength));
            
            URI s3checksum = URI.create("md5:" + putResponse.eTag().replaceAll("\"", ""));
            
            URI contentChecksum = newArtifact.contentChecksum;
            if (source instanceof DigestInputStream) {
                // newArtifact.contentChecksum was null
                DigestInputStream dis = (DigestInputStream) source;
                MessageDigest md = dis.getMessageDigest();
                contentChecksum = URI.create(DEFAULT_CHECKSUM_ALGORITHM + ":" + HexUtil.toHex(md.digest()));
                if (!contentChecksum.equals(s3checksum)) {
                    throw new RuntimeException("checksum mismatch (S3StorageAdapter vs S3): " + contentChecksum + " != " + s3checksum);
                }
            }
            if (newArtifact.contentChecksum != null && !newArtifact.contentChecksum.equals(s3checksum)) {
                throw new RuntimeException("checksum mismatch (client vs S3) undetected: " + newArtifact.contentChecksum + " != " + s3checksum);
            }
            if (newArtifact.contentChecksum == null) {
                // TODO: update the object with metadata we did not know up-front?
                // TODO: get the S3Object or GetObjectResponse and see if the etag is an MD5 checksum?
            }
            
            // TODO: head request to get the lastModified?
            Date lastModified = null;
            return toStorageMetadata(loc, contentChecksum, newArtifact.contentLength, newArtifact.getArtifactURI(), lastModified);
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
                    case "EntityTooLarge": {
                        throw new ByteLimitExceededException("content length too large for single upload stream",
                                newArtifact.contentLength);
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

    @Override
    public String startTransaction(URI uri) throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageMetadata commitTransaction(String string) throws ResourceNotFoundException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abortTransaction(String string) throws ResourceNotFoundException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageMetadata getTransactionStatus(String string) throws ResourceNotFoundException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    // used by intTest
    public boolean exists(StorageLocation loc) {
        try {
            head(toInternalBucket(loc), loc.getStorageID().toASCIIString());
        } catch (NoSuchBucketException | NoSuchKeyException e) {
            return false;
        }
        return true;
    }
    
    /**
     * Perform a head (headObject) request for the given key in the given bucket. This is used to translate into a
     * StorageMetadata object to verify after a PUT and to translate S3Objects for a LIST function.
     *
     * @param bucket The bucket to look.
     * @param key The key to look for.
     * @return StorageMetadata instance. Never null
     */
    StorageMetadata head(InternalBucket bucket, String key) {
        HeadObjectRequest.Builder headBuilder = HeadObjectRequest.builder().key(key);
        HeadObjectResponse headResponse = s3client.headObject(headBuilder.bucket(bucket.name).build());

        Map<String, String> objectMetadata = headResponse.metadata();
        LOGGER.debug("object: " + bucket + " " + key);
        for (Map.Entry<String,String> me : objectMetadata.entrySet()) {
            LOGGER.debug("meta: " + me.getKey() + " = " + me.getValue());
        }
        URI artifactURI = objectMetadata.containsKey(ARTIFACT_URI_KEY)
                ? URI.create(objectMetadata.get(ARTIFACT_URI_KEY)) : null;

        StorageLocation loc = toExternal(bucket, key);

        URI md5 = URI.create(objectMetadata.get(CHECKSUM_KEY));
        //URI md5 = URI.create("md5:unknown");

        Instant t = headResponse.lastModified();
        Date lastModified = new Date(1000 * t.getEpochSecond() + t.getNano() / 1000000);
        return toStorageMetadata(loc, md5, headResponse.contentLength(), artifactURI, lastModified);
    }

    /**
     * Delete from storage the artifact identified by storageLocation.
     *
     * @param storageLocation Identifies the artifact to delete.
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException If an unrecoverable error occurred.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred.
     */
    public void delete(StorageLocation storageLocation)
            throws ResourceNotFoundException, IOException, StorageEngageException, TransientException {
        LOGGER.debug("delete: " + storageLocation);
        
        try {
            final DeleteObjectRequest.Builder deleteObjectRequestBuilder 
                = DeleteObjectRequest.builder()
                .bucket(toInternalBucket(storageLocation).name)
                .key(storageLocation.getStorageID().toASCIIString());
            s3client.deleteObject(deleteObjectRequestBuilder.build());
        } catch (NoSuchKeyException e) {
            throw new ResourceNotFoundException(e.getMessage(), e);
        } catch (S3Exception e) {
            throw new StorageEngageException(e.getMessage(), e);
        } catch (SdkClientException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Obtain a list of objects from the S3 server. This will use the nextMarkerKey value to start listing the next
     * page of data from. This is hard-coded to 1000 objects by default, but is modifiable to something smaller
     * using the <code>maxKeys(int)</code> call.
     * This is used by a StorageMetadata Iterator to list all objects from a bucket.
     *
     * @param storageBucket The bucket to pull from.
     * @param nextMarkerKey The optional key from which to start listing the next page of objects.
     * @return ListObjectsResponse instance.
     */
    ListObjectsResponse listObjects(InternalBucket bucket, final String nextMarkerKey) {
        LOGGER.debug("listObjects: " + bucket + " marker: " + nextMarkerKey);
        final ListObjectsRequest.Builder listBuilder = ListObjectsRequest.builder();
        
        listBuilder.bucket(bucket.name);
        if (nextMarkerKey != null) {
            listBuilder.marker(nextMarkerKey);
        }
        //final Optional<String> optionalNextMarkerKey = Optional.ofNullable(nextMarkerKey);
        //optionalNextMarkerKey.ifPresent(listBuilder::marker);

        return s3client.listObjects(listBuilder.build());
    }
}

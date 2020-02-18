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

import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.io.ThreadedIO;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
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
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Implementation of a Storage Adapter using the Amazon S3 API.
 */
public class S3StorageAdapter implements StorageAdapter {

    private static final Logger LOGGER = Logger.getLogger(S3StorageAdapter.class);

    private static final Region REGION = Region.of("default");
    
    private static final int BUFFER_SIZE_BYTES = 8192;
    private static final String DEFAULT_CHECKSUM_ALGORITHM = "md5";
    private static final String CHECKSUM_KEY = "checksum";
    private static final String ARTIFACT_URI_KEY = "uri";

    private final URI s3Endpoint;
    private int storageBucketLength;
    private final S3Client s3Client; // S3Client is thread safe
    
    
    // used for developer isolation only
    private String bucketNamespace;
    
    public S3StorageAdapter() {
        try {
            String suri = System.getProperty("org.opencadc.inventory.storage.s3.S3StorageAdapter.endpoint");
            String sbl = System.getProperty("org.opencadc.inventory.storage.s3.S3StorageAdapter.bucketLength");
            String accessKey = System.getProperty("aws.accessKeyId");
            String secretKey = System.getProperty("aws.secretAccessKey");
            if (suri == null || sbl == null || accessKey == null || secretKey == null) {
                StringBuilder sb = new StringBuilder();
                sb.append("incomplete config: ");
                sb.append("\n\torg.opencadc.inventory.storage.s3.S3StorageAdapter.endpoint: ");
                if (suri == null) {
                    sb.append("MISSING");
                } else {
                    sb.append("OK");
                }
                
                sb.append("\n\torg.opencadc.inventory.storage.s3.S3StorageAdapter.bucketLength: ");
                if (sbl == null) {
                    sb.append("MISSING");
                } else {
                    sb.append("OK");
                }
                
                sb.append("\n\taws.accessKeyId: ");
                if (accessKey == null) {
                    sb.append("MISSING");
                } else {
                    sb.append("OK");
                }
                
                sb.append("\n\taws.secretAccessKey: ");
                if (secretKey == null) {
                    sb.append("MISSING");
                } else {
                    sb.append("OK");
                }
                throw new IllegalStateException(sb.toString());
            }
           
            this.s3Endpoint = new URI(suri);
            this.storageBucketLength = Integer.parseInt(sbl);

            S3ClientBuilder s3b = S3Client.builder();
            /*
            // since we checked the standard system properties above this is not strictly necessary
            s3b.credentialsProvider(new AwsCredentialsProvider() {
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
            */
            s3b.endpointOverride(s3Endpoint);
            s3b.region(REGION);
            this.s3Client = s3b.build();
            
            checkConfig();
        } catch (Exception fatal) {
            throw new RuntimeException("invalid config", fatal);
        }
    }

    // test ctor to not need config in known location
    // usage: -Daws.accessKeyId=  -Daws.secretAccessKey=
    S3StorageAdapter(URI endpoint, int storageBucketLength) {
        try {
            String accessKey = System.getProperty("aws.accessKeyId");
            String secretKey = System.getProperty("aws.secretAccessKey");
            if (accessKey == null || secretKey == null) {
                StringBuilder sb = new StringBuilder();
                sb.append("incomplete config:");
                sb.append("\n\taws.accessKeyId: ");
                if (accessKey == null) {
                    sb.append("MISSING");
                } else {
                    sb.append("OK");
                }
                
                sb.append("\n\taws.secretAccessKey: ");
                if (secretKey == null) {
                    sb.append("MISSING");
                } else {
                    sb.append("OK");
                }
                throw new IllegalStateException(sb.toString());
            }
            this.s3Endpoint = endpoint;
            S3ClientBuilder s3b = S3Client.builder();
            s3b.endpointOverride(endpoint);
            s3b.region(REGION);
            this.s3Client = s3b.build();
            this.storageBucketLength = storageBucketLength;
            
            checkConfig();
        } catch (Exception fail) {
            throw new RuntimeException("INIT FAILED", fail);
        }
    }

    private void checkConfig() {
        ListBucketsResponse resp = s3Client.listBuckets();
        LOGGER.info("checkConfig: bucket owner is " + resp.owner());
    }
    
    /**
     * Static name space that is prepended to all generated buckets. This is intended
     * to partition developers using a shared S3 back end.
     * 
     * @param pre 
     */
    public void setBucketNamespace(String pre) {
        this.bucketNamespace = pre;
    }
    
    URI generateStorageID() {
        return URI.create("uuid:" +  UUID.randomUUID().toString());
    }

    String toInternalBucket(String bucket) {
        String ret = bucket;
        if (bucketNamespace != null) {
            ret = bucketNamespace + "-" + bucket;
        }
        return ret;
    }
    
    String toExternalBucket(String bucket) {
        String ret = bucket;
        if (bucketNamespace != null && bucket.startsWith(bucketNamespace)) {
            ret = bucket.substring(bucketNamespace.length() + 1);
        }
        return ret;
    }
    
    /**
     * Obtain the InputStream for the given object. Tests can override this method.
     *
     * @param storageLocation The Storage Location of the desired object.
     * @return InputStream to the object.
     */
    InputStream toObjectInputStream(final StorageLocation storageLocation) {
        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(toInternalBucket(storageLocation.storageBucket))
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
    StorageMetadata toStorageMetadata(final URI storageID, final String bucket, final URI artifactURI,
            final URI md5, final long contentLength) {
        StorageLocation storageLocation = new StorageLocation(storageID);
        storageLocation.storageBucket = bucket;

        StorageMetadata storageMetadata = new StorageMetadata(storageLocation, md5, contentLength);
        storageMetadata.artifactURI = artifactURI;

        return storageMetadata;
    }

    /**
     * Get from storage the artifact identified by storageLocation.
     *
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest The destination stream.
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If writing failed.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     */
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest)
            throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException {
        LOGGER.debug("get: " + storageLocation);
        try (final InputStream inputStream = toObjectInputStream(storageLocation)) {
            ThreadedIO tio = new ThreadedIO(BUFFER_SIZE_BYTES, 8);
            tio.ioLoop(dest, inputStream);
        } catch (NoSuchBucketException e) {
            throw new ResourceNotFoundException("not found: bucket " + storageLocation.storageBucket);
        } catch (NoSuchKeyException e) {
            throw new ResourceNotFoundException("not found: key " + storageLocation.getStorageID());
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
     * Get from storage the artifact identified by storageLocation with cutout specifications. Currently needs full
     * implementation.
     * TODO
     * TODO Complete the implementation to check for an astronomy file (i.e. FITS, HDF5), and determine what cutout
     * TODO system to use.
     * TODO
     * TODO jenkinsd 2020.01.09
     * TODO
     *
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest The destination stream.
     * @param cutouts Cutouts to be applied to the artifact
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If writing failed.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     */
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest, Set<String> cutouts)
            throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException {
        if ((cutouts == null) || cutouts.isEmpty()) {
            get(storageLocation, dest);
            return;
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
     * @throws SdkClientException If any client side error occurs such as an IO related failure, failure to get credentials, etc.
     * @throws S3Exception Base class for all service exceptions. Unknown exceptions will be thrown as an instance of this type.
     */
    void createBucket(final String bucket) throws ResourceAlreadyExistsException, SdkClientException, S3Exception {
        final CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(toInternalBucket(bucket)).build();

        try {
            s3Client.createBucket(createBucketRequest);
        } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException e) {
            throw new ResourceAlreadyExistsException(String.format("Bucket with name %s already exists.\n%s\n",
                    bucket, e.getMessage()), e);
        }
    }
    
    public void deleteBucket(String bucket) throws ResourceNotFoundException {
        final DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(toInternalBucket(bucket)).build();
        try {
            s3Client.deleteBucket(deleteBucketRequest);
        } catch (NoSuchBucketException e) {
            throw new ResourceNotFoundException("not found bucket " + bucket);
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
    String ensureBucket(final URI storageID) throws ResourceAlreadyExistsException, SdkClientException, S3Exception {
        String bucket = InventoryUtil.computeBucket(storageID, storageBucketLength);
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder().bucket(toInternalBucket(bucket)).build();

        try {
            s3Client.headBucket(headBucketRequest);
        } catch (NoSuchBucketException e) {
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
     * @param newArtifact known information about the incoming artifact
     * @param source stream from which to read
     * @return storage metadata after write
     *
     * @throws IncorrectContentChecksumException checksum of the data stream did not match the value in newArtifact
     * @throws IncorrectContentLengthException number bytes read did not match the value in newArtifact
     * @throws ReadException If the client failed to read the stream.
     * @throws WriteException If the storage system failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     */
    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source)
            throws IncorrectContentChecksumException, IncorrectContentLengthException, ReadException, WriteException,
            StorageEngageException {
        LOGGER.debug("put: " + newArtifact);
        
        final URI storageID = generateStorageID();

        String checksum = "N/A";

        try {
            final String bucket = ensureBucket(storageID);

            final PutObjectRequest.Builder putObjectRequestBuilder
                    = PutObjectRequest.builder()
                    .bucket(toInternalBucket(bucket))
                    .key(storageID.toASCIIString());

            final Map<String, String> metadata = new HashMap<>();
            metadata.put(ARTIFACT_URI_KEY, newArtifact.getArtifactURI().toASCIIString().trim());

            if (newArtifact.contentChecksum != null) {
                ensureChecksum(newArtifact.contentChecksum);
                final String md5SumValue = newArtifact.contentChecksum.getSchemeSpecificPart();

                // Reconvert to Hex bytes before Base64 encoding it as per what S3 expects.
                final byte[] value = HexUtil.toBytes(md5SumValue);
                checksum = new String(Base64.getEncoder().encode(value));
                putObjectRequestBuilder.contentMD5(checksum);
                metadata.put(CHECKSUM_KEY, newArtifact.contentChecksum.toASCIIString());
            } else { 
                try {
                    MessageDigest md = MessageDigest.getInstance(DEFAULT_CHECKSUM_ALGORITHM);
                    DigestInputStream dis = new DigestInputStream(source, md);
                    source = dis;
                } catch (NoSuchAlgorithmException ex) {
                    throw new RuntimeException("failed to create MessageDigest: " + DEFAULT_CHECKSUM_ALGORITHM);
                }
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
            
            URI contentChecksum = newArtifact.contentChecksum;
            if (source instanceof DigestInputStream) {
                // newArtifact.contentChecksum was null
                DigestInputStream dis = (DigestInputStream) source;
                MessageDigest md = dis.getMessageDigest();
                contentChecksum = URI.create(DEFAULT_CHECKSUM_ALGORITHM + ":" + HexUtil.toHex(md.digest()));
            }
            return toStorageMetadata(storageID, bucket, newArtifact.getArtifactURI(), contentChecksum, newArtifact.contentLength);
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

    // used by intTest
    public boolean exists(StorageLocation loc) {
        try {
            head(loc.storageBucket, loc.getStorageID().toASCIIString());
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
    StorageMetadata head(final String bucket, final String key) {
        final HeadObjectRequest.Builder headBuilder = HeadObjectRequest.builder().key(key);
        final HeadObjectResponse headResponse = s3Client.headObject(StringUtil.hasLength(bucket)
                ? headBuilder.bucket(toInternalBucket(bucket)).build()
                : headBuilder.build());
        final Map<String, String> objectMetadata = headResponse.metadata();
        LOGGER.debug("object: " + bucket + " " + key);
        for (Map.Entry<String,String> me : objectMetadata.entrySet()) {
            LOGGER.debug("meta: " + me.getKey() + " = " + me.getValue());
        }
        final URI artifactURI = objectMetadata.containsKey(ARTIFACT_URI_KEY)
                ? URI.create(objectMetadata.get(ARTIFACT_URI_KEY)) : null;

        final URI storageID = URI.create(key);

        // TODO: The MD5 value must always be present.  Do we need a null check here?
        final URI md5 = URI.create(objectMetadata.get(CHECKSUM_KEY));

        return toStorageMetadata(storageID, bucket, artifactURI, md5, headResponse.contentLength());
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
                .bucket(toInternalBucket(storageLocation.storageBucket))
                .key(storageLocation.getStorageID().toASCIIString());
            final DeleteObjectResponse deleteObjectResponse = s3Client.deleteObject(deleteObjectRequestBuilder.build());
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
    ListObjectsResponse listObjects(final String storageBucket, final String nextMarkerKey) {
        LOGGER.debug("listObjects: " + storageBucket);
        final ListObjectsRequest.Builder listBuilder = ListObjectsRequest.builder();
        final Optional<String> optionalNextMarkerKey = Optional.ofNullable(nextMarkerKey);

        listBuilder.bucket(toInternalBucket(storageBucket));
        optionalNextMarkerKey.ifPresent(listBuilder::marker);

        return s3Client.listObjects(listBuilder.build());
    }

    /**
     * Iterator of items ordered by storage locations.
     *
     * @return iterator ordered by storage locations
     */
    @Override
    public Iterator<StorageMetadata> iterator() {
        return new S3StorageMetadataIterator(this, null);
    }

    /**
     * Iterator of items ordered by storage locations in matching bucket(s).
     *
     * @param bucketPrefix bucket constraint
     * @return iterator ordered by storage locations
     */
    @Override
    public Iterator<StorageMetadata> iterator(String bucketPrefix) {
        return new S3StorageMetadataIterator(this, bucketPrefix);
    }

    /**
     * Get set of items in the given bucket.
     *
     * @param storageBucket Only iterate over items in this bucket.
     * @return An iterator over an ordered list of items in this storage bucket.
     *
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred.
     */
    public SortedSet<StorageMetadata> list(String storageBucket)
            throws StorageEngageException, TransientException {
        SortedSet<StorageMetadata> ret = new TreeSet<StorageMetadata>();
        Iterator<StorageMetadata> i = iterator(storageBucket);
        while (i.hasNext()) {
            ret.add(i.next());
        }
        return ret;
    }
    
    public Iterator<String> bucketIterator(String bucketPrefix) {
        return new BucketIterator(bucketPrefix);
    }
    
    private class BucketIterator implements Iterator<String> {
        
        final String bucketPrefix;
        Iterator<String> iter;
        
        BucketIterator(String bucketPrefix) {
            this.bucketPrefix = bucketPrefix;
            init();
        }
        
        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public String next() {
            return iter.next();
        }
    
        
        private void init() {
            List<Bucket> buckets = s3Client.listBuckets().buckets();
            Iterator<Bucket> i = buckets.iterator();
            List<String> keep = new ArrayList<>();
            while (i.hasNext()) {
                Bucket b = i.next();
                String bname = toExternalBucket(b.name());
                
                if (bucketPrefix == null || bname.startsWith(bucketPrefix)) {
                    //LOGGER.debug("BucketIterator keep: " + b.name() + " aka " + bname + " vs " + bucketPrefix);
                    keep.add(bname);
                } //else {
                    //LOGGER.debug("BucketIterator discard: " + b.name() + " aka " + bname + " vs " + bucketPrefix);
                //}
            }
            //LOGGER.debug("BucketIterator: " + keep.size() + " matching buckets");
            this.iter = keep.iterator();
        }
    }
}

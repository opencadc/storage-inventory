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

import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Implementation of a Storage Adapter using the Amazon S3 API. This implementation
 * puts objects into multiple generated S3 buckets that ~matches the StorageLocation.storageBucket
 * value.  Keys are also randomly generated (UUID) strings and are never re-used.
 * 
 */
public class S3StorageAdapterSB extends S3StorageAdapter {
    private static final Logger LOGGER = Logger.getLogger(S3StorageAdapterSB.class);

    private static final String KEY_SCHEME = "sb";
    private final InternalBucket is3bucket;
    
    public S3StorageAdapterSB() {
        super();
        this.is3bucket = new InternalBucket(s3bucket);
        try {
            initBucket();
        } catch (ResourceAlreadyExistsException ex) {
            throw new IllegalStateException("failed to find or create bucket: " + s3bucket, ex);
        }
    }

    // ctor for unit tests that do not connect to S3 backend
    S3StorageAdapterSB(String s3bucket, int storageBucketLength) {
        super(s3bucket, storageBucketLength);
        this.is3bucket = new InternalBucket(s3bucket);
    }
    
    // S3 bucket and key strategy:
    // - use a single configured bucket
    // - randomly generated key is sb:{StorageLocation.storageBucket}:{uuid}
    // - make use of StorageLocation.storageBucket prefixes when listing bucket contents
    
    @Override
    protected StorageLocation generateStorageLocation() {
        String id = UUID.randomUUID().toString();
        String pre = InventoryUtil.computeBucket(URI.create(id), storageBucketLength);
        StorageLocation ret = new StorageLocation(URI.create(KEY_SCHEME + ":" + pre + ":" + id));
        ret.storageBucket = pre;
        return ret;
    }
    
    @Override
    protected InternalBucket toInternalBucket(StorageLocation loc) {
        return is3bucket;
    }
    
    
    @Override
    protected StorageLocation toExternal(InternalBucket bucket, String key) {
        // ignore bucket
        // extract scheme from the key
        String[] parts = key.split(":");
        if (parts.length != 3 || !KEY_SCHEME.equals(parts[0])) {
            throw new IllegalStateException("invalid object key: " + key + " from bucket: " + bucket);
        }
        StorageLocation ret = new StorageLocation(URI.create(key));
        ret.storageBucket = parts[1];
        return ret;
    }

    @Override
    void ensureBucket(InternalBucket ib) throws ResourceAlreadyExistsException, SdkClientException, S3Exception {
        // no -op
    }

    InternalBucket getDataBucket() {
        return is3bucket;
    }
    
    void initBucket() throws ResourceAlreadyExistsException, SdkClientException, S3Exception {
        try {
            HeadBucketRequest headBucketRequest = HeadBucketRequest.builder().bucket(s3bucket).build();
            HeadBucketResponse resp = s3client.headBucket(headBucketRequest);
            LOGGER.warn("found s3bucket: " + s3bucket);
            // TODO: compare current config from the config when the bucket was created
        } catch (NoSuchBucketException nbe) {
            try {
                CreateBucketRequest.Builder cb = CreateBucketRequest.builder();
                cb.bucket(s3bucket);
                CreateBucketRequest createBucketRequest = cb.build();
                CreateBucketResponse resp = s3client.createBucket(createBucketRequest);
                LOGGER.warn("created s3bucket: " + s3bucket);
                // TODO: store fresh new config
            } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ex) {
                throw new ResourceAlreadyExistsException("Bucket already exists: " + s3bucket, ex);
            }
        }
    }
    
    // list objects method with prefix pattern
    ListObjectsResponse listObjects(InternalBucket bucket, String nextMarkerKey, String prefix) {
        LOGGER.debug("listObjects: " + bucket.name + " prefix: " + prefix + " marker: " + nextMarkerKey);
        final ListObjectsRequest.Builder listBuilder = ListObjectsRequest.builder();
        listBuilder.bucket(bucket.name);
        if (nextMarkerKey != null) {
            listBuilder.marker(nextMarkerKey);
        }
        if (prefix != null) {
            listBuilder.prefix(KEY_SCHEME + ":" + prefix);
        }

        return s3client.listObjects(listBuilder.build());
    }
    
    
    /**
     * Iterator of items ordered by storage locations.
     *
     * @return iterator ordered by storage locations
     */
    @Override
    public Iterator<StorageMetadata> iterator() {
        return new StorageMetadataIterator(null);
    }

    /**
     * Iterator of items ordered by storage locations in matching bucket(s).
     *
     * @param bucketPrefix bucket constraint
     * @return iterator ordered by storage locations
     */
    @Override
    public Iterator<StorageMetadata> iterator(String bucketPrefix) {
        return new StorageMetadataIterator(bucketPrefix);
    }
    
    private class StorageMetadataIterator implements Iterator<StorageMetadata> {
        private Iterator<S3Object> objectIterator;

        //S3 Key of where to start listing the next page of objects.  Will be null for the first call.
        private final String bucketPrefix;
        private String nextMarkerKey;
        private boolean done = false;

        public StorageMetadataIterator(String bucketPrefix) {
            this.bucketPrefix = bucketPrefix;
        }


        @Override
        public boolean hasNext() {
            //if (done) {
            //    return false;
            //}
            
            if (objectIterator == null || (!objectIterator.hasNext() && nextMarkerKey != null)) {
                ListObjectsResponse listObjectsResponse = listObjects(is3bucket, nextMarkerKey, bucketPrefix);
                LOGGER.debug("StorageMetadataIterator bucket: " + is3bucket.name + " size: " + listObjectsResponse.contents().size() 
                        + " marker: " + listObjectsResponse.nextMarker());
                if (listObjectsResponse.hasContents()) {
                    objectIterator = listObjectsResponse.contents().iterator();
                    nextMarkerKey = listObjectsResponse.nextMarker();
                } else {
                    objectIterator = null;
                    nextMarkerKey = null;
                    done = true;
                    LOGGER.debug("StorageMetadataIterator: done");
                }
            }

            if (objectIterator == null) {
                return false;
            }

            return objectIterator.hasNext();
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         *
         * @throws java.util.NoSuchElementException if the iteration has no more elements
         */
        @Override
        public StorageMetadata next() {
            if (objectIterator == null || !objectIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            S3Object o = objectIterator.next();
            LOGGER.debug("next: " + o);
            final StorageMetadata storageMetadata = head(is3bucket, o.key());
        
            // etag is am md5 for small objects and ceph cluster circa Q1 2020
            //String etag = o.eTag().replaceAll("\"", "");
            //URI s3checksum = URI.create("md5:" + etag);
            //if (!storageMetadata.getContentChecksum().equals(s3checksum)) {
            //    throw new IllegalStateException("checksum mismatch (uri attribute vs S3 etag): " 
            //        + storageMetadata.getContentChecksum() + " != " + s3checksum);
            //}
        
            return storageMetadata;
        }
    }
}

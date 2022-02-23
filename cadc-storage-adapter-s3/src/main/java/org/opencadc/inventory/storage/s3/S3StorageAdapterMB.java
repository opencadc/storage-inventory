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

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;
import software.amazon.awssdk.services.s3.model.Bucket;

/**
 * Implementation of a Storage Adapter using the Amazon S3 API. This implementation
 * puts objects into multiple generated S3 buckets that ~matches the StorageLocation.storageBucket
 * value.  Keys are also randomly generated (UUID) strings and are never re-used.
 * 
 */
public class S3StorageAdapterMB extends S3StorageAdapter {

    private static final Logger LOGGER = Logger.getLogger(S3StorageAdapterMB.class);
    
    private static final String KEY_SCHEME = "mb";

    public S3StorageAdapterMB() {
        super();
    }

    // ctor for unit tests that do not connect to S3 backend
    public S3StorageAdapterMB(String s3bucket, int storageBucketLength) {
        super(s3bucket, storageBucketLength);
    }

    // bucket and key strategy: 
    // S3 buckets use bucket name + randomly generated StorageLocation.storageBucket
    // randomly generated S3 keys
    
    @Override
    protected StorageLocation generateStorageLocation() {
        StorageLocation ret = new StorageLocation(URI.create(KEY_SCHEME + ":" + UUID.randomUUID().toString()));
        ret.storageBucket = InventoryUtil.computeBucket(ret.getStorageID(), storageBucketLength);
        return ret;
    }

    
    private String toInternalBucket(String bucket) {
        return s3bucket + "-" + bucket;
    }

    @Override
    protected InternalBucket toInternalBucket(StorageLocation loc) {
        return  new InternalBucket(toInternalBucket(loc.storageBucket));
    }
    
    @Override
    protected StorageLocation toExternal(InternalBucket bucket, String key) {
        // map S3 bucket name to external StorageLocation.storageBucket
        // ignore key
        //LOGGER.warn("toExternal: " + bucket);
        if (!isInternalBucket(bucket)) {
            throw new IllegalStateException("invalid bucket: " + bucket.name);
        }
        StorageLocation ret = new StorageLocation(URI.create(key));
        ret.storageBucket = bucket.name.substring(s3bucket.length() + 1);
        return ret;
    }
    
    private boolean isInternalBucket(InternalBucket ib) {
        String bucket = ib.name;
        return (bucket.length() == s3bucket.length() + storageBucketLength + 1)
            && bucket.startsWith(s3bucket + "-");
    }
    
    /**
     * Iterator of items ordered by storage locations.
     *
     * @return iterator ordered by storage locations
     */
    @Override
    public Iterator<StorageMetadata> iterator() {
        return new S3StorageMetadataIteratorMB(this, null);
    }

    /**
     * Iterator of items ordered by storage locations in matching bucket(s).
     *
     * @param bucketPrefix bucket constraint
     * @return iterator ordered by storage locations
     */
    @Override
    public Iterator<StorageMetadata> iterator(String bucketPrefix) {
        return new S3StorageMetadataIteratorMB(this, bucketPrefix);
    }
    
    // iterate over S3 buckets that match the specified prefix
    Iterator<InternalBucket> bucketIterator(String bucketPrefix) {
        return new BucketIterator(bucketPrefix);
    }
    
    private class BucketIterator implements Iterator<InternalBucket> {
        
        String internalBucketPrefix;
        Iterator<InternalBucket> iter;
        
        BucketIterator(String bucketPrefix) {
            this.internalBucketPrefix = s3bucket + "-";
            if (bucketPrefix != null) {
                this.internalBucketPrefix += bucketPrefix;
            }
            init();
        }
        
        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public InternalBucket next() {
            return iter.next();
        }
        
        private void init() {
            List<Bucket> buckets = s3client.listBuckets().buckets();
            Iterator<Bucket> i = buckets.iterator();
            List<InternalBucket> keep = new ArrayList<>();
            while (i.hasNext()) {
                Bucket b = i.next();
                InternalBucket ib = new InternalBucket(b.name());
                if (isInternalBucket(ib)) {
                    //LOGGER.warn("BucketIterator: " + ib);
                    if (internalBucketPrefix == null || ib.name.startsWith(internalBucketPrefix)) {
                        keep.add(ib);
                    }
                } else {
                    //LOGGER.warn("BucketIterator: skip " + b.name());
                }
            }
            //LOGGER.warn("BucketIterator: " + bucketPrefix + " " + keep.size());
            this.iter = keep.iterator();
        }
    }
}

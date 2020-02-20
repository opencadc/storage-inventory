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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.log4j.Logger;
import org.opencadc.inventory.storage.StorageMetadata;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;


/**
 * Main Iterator over StorageMetadata objects.  This will use the S3 Storage Adapter to obtain the metadata via a head
 * call, and to obtain the next page of data via a listObjects call.
 */
public class S3StorageMetadataIteratorMB implements Iterator<StorageMetadata> {
    private static final Logger log = Logger.getLogger(S3StorageMetadataIteratorMB.class);
    
    private final S3StorageAdapterMB storageAdapter;

    private Iterator<String> bucketIterator;
    private String currentBucket;
    
    private Iterator<S3Object> objectIterator;

    //S3 Key of where to start listing the next page of objects.  Will be null for the first call.
    private String nextMarkerKey;

    public S3StorageMetadataIteratorMB(S3StorageAdapterMB storageAdapter, String bucketPrefix) {
        this.storageAdapter = storageAdapter;
        this.bucketIterator = storageAdapter.bucketIterator(bucketPrefix);
        if (bucketIterator.hasNext()) {
            this.currentBucket = bucketIterator.next();
        } else {
            bucketIterator = null; // no matching buckets
        }
    }


    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        //log.debug("hasNext: " + currentBucket + "," + (objectIterator != null) + "," + (objectIterator != null && objectIterator.hasNext()));
        if (bucketIterator == null) {
            return false;
        }
        if (objectIterator == null || (!objectIterator.hasNext())) { 
            //log.debug("get batch in bucket: " + currentBucket);
            if (objectIterator == null || nextMarkerKey != null) {
                // multi-batch bucket
                ListObjectsResponse listObjectsResponse = storageAdapter.listObjects(currentBucket, nextMarkerKey);
                //log.debug("bucket: " + currentBucket + " " + listObjectsResponse.contents().size() + "/" + listObjectsResponse.nextMarker());
                if (listObjectsResponse.hasContents()) {
                    objectIterator = listObjectsResponse.contents().iterator();
                    nextMarkerKey = listObjectsResponse.nextMarker();
                } else {
                    objectIterator = null;
                    nextMarkerKey = null;
                }
            } else {
                // finished single batch bucket
                objectIterator = null;
            }
        }
        if (objectIterator == null) {
            if (bucketIterator.hasNext()) {
                currentBucket = bucketIterator.next();
                //log.debug("next bucket: " + currentBucket);
                return hasNext();
            } else {
                //log.debug("next bucket: null");
                bucketIterator = null; // done
                return false;
            }
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
        //log.debug("next: " + o);
        // the S3Object contains almost all the metadata needed so could avoid the HEAD request
        // ETag has correct MD5 value for small objects but not verified for larger 
        // missing a way to restore the artifact URI
        final StorageMetadata storageMetadata = storageAdapter.head(currentBucket, o.key());
        return storageMetadata;
    }
}

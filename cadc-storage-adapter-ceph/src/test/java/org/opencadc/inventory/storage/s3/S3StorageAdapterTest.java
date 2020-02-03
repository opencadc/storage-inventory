
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

import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageMetadata;

import ca.nrc.cadc.net.ResourceNotFoundException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import static org.opencadc.inventory.storage.s3.S3StorageAdapter.*;


public class S3StorageAdapterTest {

    static final String ARTIFACT_URI_TEMPLATE = "cadctest:%s/%s";

    @Test
    public void ensureBucketCreate() throws Exception {
        final TestS3Client testS3Client = new TestS3Client();
        final S3StorageAdapter testSubject = new S3StorageAdapter(testS3Client);
        final URI storageID = URI.create("test:myobjectkey");

        testS3Client.headBucketShouldFail = true;

        final String bucket = testSubject.ensureBucket(storageID);

        Assert.assertEquals("Wrong bucket name.", InventoryUtil.computeBucket(storageID, 5), bucket);
        Assert.assertTrue("Head bucket should have been called.", testS3Client.headBucketCalled);
        Assert.assertTrue("Create bucket should have been called.", testS3Client.createBucketCalled);
    }

    @Test
    public void ensureBucketExists() throws Exception {
        final TestS3Client testS3Client = new TestS3Client();
        final S3StorageAdapter testSubject = new S3StorageAdapter(testS3Client);
        final URI storageID = URI.create("test:myobjectkey");

        testS3Client.headBucketShouldFail = false;

        final String bucket = testSubject.ensureBucket(storageID);

        Assert.assertEquals("Wrong bucket name.", InventoryUtil.computeBucket(storageID, 5), bucket);
        Assert.assertTrue("Head bucket should have been called.", testS3Client.headBucketCalled);
        Assert.assertFalse("Create bucket should not have been called.", testS3Client.createBucketCalled);
    }

    @Test
    public void putObjectBucketCreate() throws Exception {
        final TestS3Client testS3Client = new TestS3Client();
        final String objectID = "newobjectkey";
        final URI storageID = URI.create("test:mynewgeneratedkey");
        final String expectedBucket = InventoryUtil.computeBucket(storageID, 5);
        final S3StorageAdapter testSubject = new S3StorageAdapter(testS3Client) {
            @Override
            URI generateStorageID() {
                return storageID;
            }
        };

        final NewArtifact newArtifact = new NewArtifact(
                URI.create(String.format(ARTIFACT_URI_TEMPLATE, "TEST", objectID)));
        newArtifact.contentChecksum = URI.create("md5:878787");
        newArtifact.contentLength = 88L;

        final String testData = "somedata";
        final InputStream inputStream = new ByteArrayInputStream(testData.getBytes());

        // Force a create bucket.
        testS3Client.headBucketShouldFail = true;

        final StorageMetadata resultStorageMetadata = testSubject.put(newArtifact, inputStream);

        Assert.assertEquals("Wrong bucket name.", expectedBucket,
                            resultStorageMetadata.getStorageLocation().storageBucket);
        Assert.assertTrue("Put Object should have been called.", testS3Client.putObjectCalled);
        Assert.assertTrue("Head bucket should have been called.", testS3Client.headBucketCalled);
        Assert.assertTrue("Create bucket should have been called.", testS3Client.createBucketCalled);
        Assert.assertFalse("Head object should not have been called.", testS3Client.headObjectCalled);
        Assert.assertEquals("Should have expected Storage ID.",
                            storageID.toASCIIString(),
                            resultStorageMetadata.getStorageLocation().getStorageID().toASCIIString());
    }

    @Test
    public void putObjectBucketExists() throws Exception {
        final TestS3Client testS3Client = new TestS3Client();
        final String objectID = "newobjectkey";
        final URI storageID = URI.create("test:mynewgeneratedkey");
        final String expectedBucket = InventoryUtil.computeBucket(storageID, 5);
        final S3StorageAdapter testSubject = new S3StorageAdapter(testS3Client) {
            @Override
            URI generateStorageID() {
                return storageID;
            }
        };

        final NewArtifact newArtifact = new NewArtifact(
                URI.create(String.format(ARTIFACT_URI_TEMPLATE, "TEST", objectID)));
        newArtifact.contentChecksum = URI.create("md5:878787");
        newArtifact.contentLength = 88L;

        final String testData = "somedata";
        final InputStream inputStream = new ByteArrayInputStream(testData.getBytes());

        // Use existing bucket.
        testS3Client.headBucketShouldFail = false;

        final StorageMetadata resultStorageMetadata = testSubject.put(newArtifact, inputStream);

        Assert.assertEquals("Wrong bucket name.", expectedBucket,
                            resultStorageMetadata.getStorageLocation().storageBucket);
        Assert.assertTrue("Put Object should have been called.", testS3Client.putObjectCalled);
        Assert.assertTrue("Head bucket should have been called.", testS3Client.headBucketCalled);
        Assert.assertFalse("Create bucket should not have been called.", testS3Client.createBucketCalled);
        Assert.assertFalse("Head object should not have been called.", testS3Client.headObjectCalled);
        Assert.assertEquals("Should have expected Storage ID.",
                            storageID.toASCIIString(),
                            resultStorageMetadata.getStorageLocation().getStorageID().toASCIIString());
    }

    @Test
    public void getObjectExists() throws Exception {
        final TestS3Client testS3Client = new TestS3Client();
        final String objectID = "getthiskey";
        final S3StorageAdapter testSubject = new S3StorageAdapter(testS3Client);
        final StorageLocation storageLocation = new StorageLocation(URI.create(
                String.format(STORAGE_ID_URI_TEMPLATE, objectID)));

        final OutputStream outputStream = new ByteArrayOutputStream();
        testSubject.get(storageLocation, outputStream);

        Assert.assertTrue("Get Object should have been called.", testS3Client.getObjectCalled);
        Assert.assertEquals("Wrong payload.", TestS3Client.DEFAULT_GET_PAYLOAD, outputStream.toString());
    }

    @Test
    public void getObjectNotExists() throws Exception {
        final TestS3Client testS3Client = new TestS3Client();
        final String objectID = "getthiskey";
        final S3StorageAdapter testSubject = new S3StorageAdapter(testS3Client);
        final StorageLocation storageLocation = new StorageLocation(URI.create(
                String.format(STORAGE_ID_URI_TEMPLATE, objectID)));

        testS3Client.getObjectShouldFailNotFound = true;

        final OutputStream outputStream = new ByteArrayOutputStream();

        try {
            testSubject.get(storageLocation, outputStream);
            Assert.fail("Should throw ResourceNotFoundException.");
        } catch (ResourceNotFoundException e) {
            // Good.
        }
    }

    @Test
    public void deleteObjectExists() throws Exception {
        final TestS3Client testS3Client = new TestS3Client();
        final String objectID = "deletethiskey";
        final S3StorageAdapter testSubject = new S3StorageAdapter(testS3Client);
        final StorageLocation storageLocation = new StorageLocation(URI.create(
                String.format(STORAGE_ID_URI_TEMPLATE, objectID)));

        testSubject.delete(storageLocation);

        Assert.assertTrue("Delete Object should have been called.", testS3Client.deleteObjectCalled);
    }

    @Test
    public void deleteObjectNotExists() throws Exception {
        final TestS3Client testS3Client = new TestS3Client();
        final String objectID = "deletethiskey";
        final S3StorageAdapter testSubject = new S3StorageAdapter(testS3Client);
        final StorageLocation storageLocation = new StorageLocation(URI.create(
                String.format(STORAGE_ID_URI_TEMPLATE, objectID)));

        testS3Client.deleteObjectShouldFailNotFound = true;

        try {
            testSubject.delete(storageLocation);
            Assert.fail("Should throw ResourceNotFoundException.");
        } catch (ResourceNotFoundException e) {
            // Good.
        }
    }
}


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

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;


@NotThreadSafe
public class TestS3Client implements S3Client {

    static final String DEFAULT_GET_PAYLOAD = "SOMETESTDATA";

    boolean createBucketCalled = false;
    boolean headBucketCalled = false;
    boolean deleteObjectCalled = false;
    boolean deleteObjectShouldFailNotFound = false;
    boolean headObjectCalled = false;
    boolean putObjectCalled = false;
    boolean getObjectCalled = false;
    boolean getObjectShouldFailNotFound = false;
    boolean headBucketShouldFail = false;

    @Override
    public String serviceName() {
        return "Test S3";
    }

    @Override
    public void close() {

    }

    @Override
    public CreateBucketResponse createBucket(CreateBucketRequest createBucketRequest)
            throws BucketAlreadyExistsException, BucketAlreadyOwnedByYouException, AwsServiceException,
                   SdkClientException, S3Exception {
        createBucketCalled = true;
        return null;
    }

    @Override
    public HeadBucketResponse headBucket(HeadBucketRequest headBucketRequest)
            throws NoSuchBucketException, AwsServiceException, SdkClientException, S3Exception {
        headBucketCalled = true;
        if (headBucketShouldFail) {
            throw NoSuchBucketException.builder().message("Bucket does not exist.").build();
        }
        return null;
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody)
            throws AwsServiceException, SdkClientException, S3Exception {
        putObjectCalled = true;
        return null;
    }

    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest)
            throws AwsServiceException, SdkClientException, S3Exception {
        deleteObjectCalled = true;

        return deleteObjectShouldFailNotFound ? DeleteObjectResponse.builder().deleteMarker(Boolean.FALSE).build()
                                              : DeleteObjectResponse.builder().deleteMarker(Boolean.TRUE).build();
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest getObjectRequest)
            throws NoSuchKeyException, AwsServiceException, SdkClientException, S3Exception {
        getObjectCalled = true;

        if (getObjectShouldFailNotFound) {
            throw NoSuchKeyException.builder().build();
        }

        final GetObjectResponse getObjectResponse = GetObjectResponse.builder().contentLength(88L).build();
        final AbortableInputStream abortableInputStream =
                AbortableInputStream.create(new ByteArrayInputStream(DEFAULT_GET_PAYLOAD.getBytes()));
        return new ResponseInputStream<>(getObjectResponse, abortableInputStream);
    }

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest)
            throws NoSuchKeyException, AwsServiceException, SdkClientException, S3Exception {
        headObjectCalled = true;

        final Map<String, String> metadata = new HashMap<>();

        metadata.put("uri", String.format("cadc:%s/%s", headObjectRequest.bucket(), headObjectRequest.key()));
        metadata.put("md5", "MD5CHECKSUM");

        return HeadObjectResponse.builder().metadata(metadata).contentLength(88L).build();
    }
}

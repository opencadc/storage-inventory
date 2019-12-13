
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

package org.opencadc.inventory.storage.ceph;


import com.ceph.rados.exceptions.RadosException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import ca.nrc.cadc.io.ByteCountInputStream;
import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.FileUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class CephStorageAdapterTest {

    static final String CLUSTER_NAME = "beta1";
    static final String USER_ID = System.getProperty("user.name");


    @Test
    @Ignore
    public void get() throws Exception {
        final CephStorageAdapter testSubject = new CephStorageAdapter(USER_ID, CLUSTER_NAME);

        final URI testURI = URI.create("site:jenkinsd/test-jcmt.fits");
        final long expectedByteCount = 3144960L;
        final URI expectedChecksum = URI.create("md5:9307240a34ed65a0a252b0046b6e87be");

        final OutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(CephStorageAdapter.DIGEST_ALGORITHM));
        final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
        final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

        testSubject.get(new StorageLocation(testURI), byteCountOutputStream);

        Assert.assertEquals("Wrong byte count.", expectedByteCount, byteCountOutputStream.getByteCount());
        Assert.assertEquals("Wrong checksum.", expectedChecksum,
                            URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                                                     new BigInteger(1, messageDigest.digest()).toString(16))));
    }

    @Test
    public void getHeaders() throws Exception {
        final CephStorageAdapter testSubject = new CephStorageAdapter(USER_ID, CLUSTER_NAME);
        final String fileName = System.getProperty("file.name");
        final URI testURI = URI.create(
                String.format("cadc:jenkinsd/%s", fileName == null ? "test-megaprime-rados.fits.fz" : fileName));

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(CephStorageAdapter.DIGEST_ALGORITHM));

        final Set<String> cutouts = new HashSet<>();
        cutouts.add("fhead");

        try (final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream)) {
            testSubject.get(new StorageLocation(testURI), byteCountOutputStream, cutouts);
        }
    }

    @Test
    @Ignore
    public void put() throws Exception {
        final URI testURI = URI.create("site:jenkinsd/test-jcmt.fits");
        try {
            final CephStorageAdapter deleteTestSubject = new CephStorageAdapter(USER_ID, CLUSTER_NAME);
            deleteTestSubject.delete(new StorageLocation(testURI));
            System.out.println("DELETE SUCCESSFUL!");
        } catch (ResourceNotFoundException e) {
            System.out.println(String.format("DELETE UNSUCCESSFUL > \n\n%s\n", e.getMessage()));
            // Doesn't exist.  Good!
        }

        final CephStorageAdapter putTestSubject = new CephStorageAdapter(USER_ID, CLUSTER_NAME);
        final File file = FileUtil.getFileFromResource("test-jcmt.fits", CephStorageAdapterTest.class);
        final NewArtifact artifact = new NewArtifact(testURI);

        artifact.contentChecksum = URI.create("md5:9307240a34ed65a0a252b0046b6e87be");
        artifact.contentLength = 3144960L;

        final InputStream fileInputStream = new FileInputStream(file);

        final StorageMetadata storageMetadata = putTestSubject.put(artifact, fileInputStream);
        fileInputStream.close();

        final URI resultChecksum = storageMetadata.getContentChecksum();
        final long resultLength = storageMetadata.getContentLength();

        Assert.assertEquals("Checksum does not match.", artifact.contentChecksum, resultChecksum);
        Assert.assertEquals("Lengths do not match.", artifact.contentLength.longValue(), resultLength);

        // Get it out again.
        final CephStorageAdapter getTestSubject = new CephStorageAdapter(USER_ID, CLUSTER_NAME);

        final OutputStream outputStream = new ByteArrayOutputStream();
        final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(CephStorageAdapter.DIGEST_ALGORITHM));
        final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
        final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

        getTestSubject.get(new StorageLocation(testURI), byteCountOutputStream);
        Assert.assertEquals("Retrieved file is not the same.", artifact.contentLength.longValue(),
                            byteCountOutputStream.getByteCount());
        Assert.assertEquals("Wrong checksum.", artifact.contentChecksum,
                            URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                                                     new BigInteger(1, messageDigest.digest()).toString(16))));
    }
}


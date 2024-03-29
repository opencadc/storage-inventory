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

package org.opencadc.inventory.storage.ad;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;

public class AdStorageAdapterGetTest {
    private static final Logger log = Logger.getLogger(AdStorageAdapterGetTest.class);
    private static final String DIGEST_ALGORITHM = "MD5";
    
    private static final URI TEST_URI = URI.create("ad:TEST/public_iris.fits");
    // the archive URI below corresponds to
    // vos://cadc.nrc.ca~vault/CADCRegtest1/vospace-int-test/linktargets/batchtargets/file1
    private static final URI TEST_VOSPAC_URI = URI.create("ad:VOSpac/231950057");

    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage", Level.INFO);
    }

    private static Subject testSubject;

    @Before
    public void initSSL() {
        // This file should be a copy of or pointer to an individual's cadcproxy.pem file
        String certFilename = System.getProperty("user.name") + ".pem";
        File pem = FileUtil.getFileFromResource(certFilename, AdStorageAdapterIteratorTest.class);
        testSubject = SSLUtil.createSubject(pem);
    }

    @Test
    public void testGetValid() {
        final AdStorageAdapter testSubject = new AdStorageAdapter();

        // IRIS
        final URI expectedIrisChecksum = URI.create("md5:e3922d47243563529f387ebdf00b66da");
        try {
            final OutputStream outputStream = new ByteArrayOutputStream();
            final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(AdStorageAdapterGetTest.DIGEST_ALGORITHM));
            final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
            final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

            final StorageLocation storageLocation = new StorageLocation(TEST_URI);
            storageLocation.storageBucket = "IRIS";
            storageLocation.expectedContentChecksum = expectedIrisChecksum;

            testSubject.get(storageLocation, byteCountOutputStream);

            Assert.assertEquals("Wrong checksum.", expectedIrisChecksum,
                URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                    new BigInteger(1, messageDigest.digest()).toString(16))));

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("Unexpected exception");
        }
    }
    
    @Test
    public void testGetPreconditionFail() {
        final AdStorageAdapter testSubject = new AdStorageAdapter();

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            

            final StorageLocation storageLocation = new StorageLocation(TEST_URI);
            storageLocation.storageBucket = "IRIS";
            storageLocation.expectedContentChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");

            testSubject.get(storageLocation, bos);

            Assert.fail("expected PreconditionFailedException, got: " + bos.toByteArray().length + " bytes");
        } catch (PreconditionFailedException expected) {
            log.info("caught expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("Unexpected exception");
        }
    }

    @Test
    public void testGetIgnoresStorageBucket() {
        final AdStorageAdapter testSubject = new AdStorageAdapter();

        // IRIS
        final URI expectedIrisChecksum = URI.create("md5:e3922d47243563529f387ebdf00b66da");
        try {
            final OutputStream outputStream = new ByteArrayOutputStream();
            final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(AdStorageAdapterGetTest.DIGEST_ALGORITHM));
            final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
            final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

            final StorageLocation storageLocation = new StorageLocation(TEST_URI);
            storageLocation.storageBucket = AdStorageQuery.DISAMBIGUATE_PREFIX + "NoBucket";

            testSubject.get(storageLocation, byteCountOutputStream);

            Assert.assertEquals("Wrong checksum.", expectedIrisChecksum,
                URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                    new BigInteger(1, messageDigest.digest()).toString(16))));

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("Unexpected exception");
        }
    }

    @Test
    public void testGetVOSpac() throws Exception {
        final AdStorageAdapter testAdapter = new AdStorageAdapter();

        final URI expectedVOSpacChecksum = URI.create("md5:b05403212c66bdc8ccc597fedf6cd5fe");
        Subject.doAs(testSubject, (PrivilegedExceptionAction<Object>) () -> {
                final OutputStream outputStream = new ByteArrayOutputStream();
                final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                        .getInstance(AdStorageAdapterGetTest.DIGEST_ALGORITHM));
                final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
                final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

                final StorageLocation storageLocation = new StorageLocation(TEST_VOSPAC_URI);
                storageLocation.storageBucket = "VOSpac:b054";

                testAdapter.get(storageLocation, byteCountOutputStream);

                Assert.assertEquals("Wrong checksum.", expectedVOSpacChecksum,
                        URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                                new BigInteger(1, messageDigest.digest()).toString(16))));
                return null;
        });
    }
    
    @Test
    public void testGetByteRange() {
        final AdStorageAdapter testSubject = new AdStorageAdapter();

        try {
            final ByteArrayOutputStream bostream = new ByteArrayOutputStream();
            final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(bostream);

            final StorageLocation storageLocation = new StorageLocation(TEST_URI);
            storageLocation.storageBucket = "IRIS";

            ByteRange range = new ByteRange(0, 2880); // one FITS header block
            testSubject.get(storageLocation, byteCountOutputStream, range);
            int len = bostream.toByteArray().length;
            log.info("result 1: " + len + " bytes");
            Assert.assertEquals(2880, byteCountOutputStream.getByteCount());
            Assert.assertEquals(2880, bostream.toByteArray().length);
            
            range = new ByteRange(2881, 2880);
            testSubject.get(storageLocation, bostream, range);
            len = bostream.toByteArray().length;
            log.info("result 2: " + len + " bytes");
            Assert.assertEquals(2880, byteCountOutputStream.getByteCount());
            Assert.assertEquals(2 * 2880, bostream.toByteArray().length); // both ranges
            
            StorageLocation storageLocation2 = new StorageLocation(URI.create("ad:TEST/not-found"));
            storageLocation2.storageBucket = "TEST";
            try {
                testSubject.get(storageLocation2, bostream, range);
                Assert.fail("expected get() to fail, but it re-used cached URL for a different StorageLocation");
            } catch (ResourceNotFoundException expected) {
                log.info("caught expected: " + expected);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("Unexpected exception");
        }
    }
}

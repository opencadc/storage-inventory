
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

package org.opencadc.inventory.storage.ad.integration;

import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.net.ResourceNotFoundException;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.ad.AdStorageAdapter;

public class AdStorageAdapterGetTest {

    private static final Logger log = Logger.getLogger(AdStorageAdapterGetTest.class);
    private static final String DIGEST_ALGORITHM = "MD5";

    @Test
    public void testGetValid() throws Exception {
        final AdStorageAdapter testSubject = new AdStorageAdapter();
        final URI testIrisUri = URI.create("ad:IRIS/I429B4H0.fits");

        // IRIS
        final URI expectedIrisChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
        try {
            final OutputStream outputStream = new ByteArrayOutputStream();
            final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(AdStorageAdapterGetTest.DIGEST_ALGORITHM));
            final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
            final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

            final StorageLocation storageLocation = new StorageLocation(testIrisUri);
            storageLocation.storageBucket = "ad";

            testSubject.get(storageLocation, byteCountOutputStream);

            Assert.assertEquals("Wrong checksum.", expectedIrisChecksum,
                URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                    new BigInteger(1, messageDigest.digest()).toString(16))));

        } catch (Exception unexpected) {
            Assert.fail("Unexpected exception");
        }

        // GEMINI
        final URI testGeminiUri = URI.create("gemini:GEM/S20191208S0019.jpg");
        final URI expectedGeminiChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
        try {
            final OutputStream outputStream = new ByteArrayOutputStream();
            final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
                .getInstance(AdStorageAdapterGetTest.DIGEST_ALGORITHM));
            final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
            final MessageDigest messageDigest = digestOutputStream.getMessageDigest();

            final StorageLocation storageLocation = new StorageLocation(testGeminiUri);
            storageLocation.storageBucket = "ad";

            testSubject.get(storageLocation, byteCountOutputStream);

            Assert.assertEquals("Wrong checksum.", expectedGeminiChecksum,
                URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
                    new BigInteger(1, messageDigest.digest()).toString(16))));

        } catch (Exception unexpected) {
            Assert.fail("Unexpected exception");
        }

        // ALMA:  should have a test for this in the long run. Currenty path elements in ALMA URIs
        // aren't supported by the data web service, so calls using them throw a 501. Exluded from
        // valid test suite for now.
        //        final URI testAlmaUri = URI.create("alma:ALMA/A001_X1320_X9a/2017.A.00056.S_uid___A001_X1320_X9a_auxiliary.tar");
        //        final URI expectedAlmaChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
        //        try {
        //            final OutputStream outputStream = new ByteArrayOutputStream();
        //            final DigestOutputStream digestOutputStream = new DigestOutputStream(outputStream, MessageDigest
        //                .getInstance(AdStorageAdapterTest.DIGEST_ALGORITHM));
        //            final ByteCountOutputStream byteCountOutputStream = new ByteCountOutputStream(digestOutputStream);
        //            final MessageDigest messageDigest = digestOutputStream.getMessageDigest();
        //
        //            final StorageLocation storageLocation = new StorageLocation(testAlmaUri);
        //            storageLocation.storageBucket = "ad";
        //
        //            testSubject.get(storageLocation, byteCountOutputStream);
        //
        //            Assert.assertEquals("Wrong checksum.", expectedAlmaChecksum,
        //                URI.create(String.format("%s:%s", messageDigest.getAlgorithm().toLowerCase(),
        //                    new BigInteger(1, messageDigest.digest()).toString(16))));
        //
        //        } catch (Exception unexpected) {
        //            Assert.fail("Unexpected exception: " + unexpected.toString());
        //        }
    }

    @Test
    public void testGetInvalid() throws Exception {
        final AdStorageAdapter testSubject = new AdStorageAdapter();

        // Invalid file in valid archive
        final URI invalidIrisUri = URI.create("ad:IRIS/BAD_FILE.fits");
        try {
            final OutputStream outputStream = new ByteArrayOutputStream();
            final StorageLocation storageLocation = new StorageLocation(invalidIrisUri);
            storageLocation.storageBucket = "ad";
            testSubject.get(storageLocation, outputStream);
            Assert.fail("ResourceNotFoundException expected");
        } catch (ResourceNotFoundException rnfe) {
            // expected
        } catch (Exception unexpected) {
            Assert.fail("Unexpected exception: " + unexpected.getMessage());
        }


        // BAD ARCHIVE - throws a 500
        final URI testAlmaUri = URI.create("badArchive:BADARCHIVE/A.00056.S_uid___A001_X1320_X9a_auxiliary.tar");
        try {
            final OutputStream outputStream = new ByteArrayOutputStream();
            final StorageLocation storageLocation = new StorageLocation(testAlmaUri);
            storageLocation.storageBucket = "ad";

            testSubject.get(storageLocation, outputStream);
            Assert.fail("StorageEngageException expected");
        } catch (StorageEngageException see) {
            // expected
            System.out.println("expected exception: " + see.getMessage());
        } catch (Exception unexpected) {
            Assert.fail("Unexpected exception: " + unexpected.toString());
        }


    }

    // TODO: failure tests where no permission
    // TODO: failure where Read and/or WriteException are triggered
}

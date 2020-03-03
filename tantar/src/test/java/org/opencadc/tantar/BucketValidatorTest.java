
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

package org.opencadc.tantar;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.junit.Assert;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;


public class BucketValidatorTest {

    @Test
    public void validateGood() throws Exception {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final PrintStream printStream = new PrintStream(byteArrayOutputStream);

        final List<Artifact> testArtifactList = new ArrayList<>();
        testArtifactList.add(
                new Artifact(URI.create("cadc:TESTBUCKET/myfile88.fits"), URI.create("md5:8899"), new Date(), 88L));
        testArtifactList.add(
                new Artifact(URI.create("cadc:TESTBUCKET/myfile99.fits"), URI.create("md5:9900"), new Date(), 99L));
        testArtifactList.add(
                new Artifact(URI.create("cadc:TESTBUCKET/myfile100.fits"), URI.create("md5:10001"), new Date(), 100L));

        final List<StorageMetadata> testStorageMetadataList = new ArrayList<>();
        final StorageMetadata storageMetadata1 =
                new StorageMetadata(new StorageLocation(URI.create("cadc:TESTBUCKET/myfile88.fits")),
                                    URI.create("md5:8899"), 88L);
        storageMetadata1.artifactURI = URI.create("cadc:TESTBUCKET/myfile88.fits");
        testStorageMetadataList.add(storageMetadata1);

        final StorageMetadata storageMetadata2 =
                new StorageMetadata(new StorageLocation(URI.create("cadc:TESTBUCKET/myfile99.fits")),
                                    URI.create("md5:9900"), 99L);
        storageMetadata2.artifactURI = URI.create("cadc:TESTBUCKET/myfile99.fits");
        testStorageMetadataList.add(storageMetadata2);

        final StorageMetadata storageMetadata3 =
                new StorageMetadata(new StorageLocation(URI.create("cadc:TESTBUCKET/myfile100.fits")),
                                    URI.create("md5:10001"), 100L);
        storageMetadata3.artifactURI = URI.create("cadc:TESTBUCKET/myfile100.fits");
        testStorageMetadataList.add(storageMetadata3);

        final BucketValidator testSubject = new BucketValidator(null, printStream) {
            @Override
            Iterator<StorageMetadata> iterateStorage(String bucket) {
                return testStorageMetadataList.iterator();
            }

            @Override
            Iterator<Artifact> iterateInventory(String bucket) {
                return testArtifactList.iterator();
            }
        };

        testSubject.validate("TESTBUCKET");

        final byte[] resultArray = byteArrayOutputStream.toByteArray();
        Assert.assertArrayEquals(String.format("Report should be empty but was %s.", new String(resultArray)),
                                 new byte[0], resultArray);
    }

    @Test
    public void validateBad() throws Exception {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final PrintStream printStream = new PrintStream(byteArrayOutputStream);

        final List<Artifact> testArtifactList = new ArrayList<>();
        testArtifactList.add(
                new Artifact(URI.create("cadc:TESTBUCKET/myfile88.fits"), URI.create("md5:8899"), new Date(), 88L));
        testArtifactList.add(
                new Artifact(URI.create("cadc:TESTBUCKET/myfile99.fits"), URI.create("md5:99001"), new Date(), 99L));
        testArtifactList.add(
                new Artifact(URI.create("cadc:TESTBUCKET/myfile100.fits"), URI.create("md5:10001"), new Date(), 100L));

        final List<StorageMetadata> testStorageMetadataList = new ArrayList<>();
        final StorageMetadata storageMetadata1 =
                new StorageMetadata(new StorageLocation(URI.create("cadc:TESTBUCKET/myfile88.fits")),
                                    URI.create("md5:8899"), 88L);
        storageMetadata1.artifactURI = URI.create("cadc:TESTBUCKET/myfile88.fits");
        testStorageMetadataList.add(storageMetadata1);

        final StorageMetadata storageMetadata2 =
                new StorageMetadata(new StorageLocation(URI.create("cadc:TESTBUCKET/myfile99.fits")),
                                    URI.create("md5:9900"), 99L);
        storageMetadata2.artifactURI = URI.create("cadc:TESTBUCKET/myfile99.fits");
        testStorageMetadataList.add(storageMetadata2);

        final StorageMetadata storageMetadata3 =
                new StorageMetadata(new StorageLocation(URI.create("cadc:TESTBUCKET/myfile100.fits")),
                                    URI.create("md5:10001"), 100L);
        storageMetadata3.artifactURI = URI.create("cadc:TESTBUCKET/myfile100.fits");
        testStorageMetadataList.add(storageMetadata3);

        final BucketValidator testSubject = new BucketValidator(null, printStream) {
            @Override
            Iterator<StorageMetadata> iterateStorage(String bucket) {
                return testStorageMetadataList.iterator();
            }

            @Override
            Iterator<Artifact> iterateInventory(String bucket) {
                return testArtifactList.iterator();
            }
        };

        testSubject.validate("TESTBUCKET");

        final byte[] resultArray = byteArrayOutputStream.toByteArray();
        final String expectedMessage = "Content checksum for cadc:TESTBUCKET/myfile99.fits is different.\n\n"
                                       + "Storage Metadata Checksum: md5:9900\n"
                                       + "Iventory Checksum: md5:99001\n\n";
        Assert.assertEquals(String.format("Report should be \n\n%s\n but was \n\n%s\n.", expectedMessage,
                                          new String(resultArray)),
                            expectedMessage, new String(resultArray));
    }
}

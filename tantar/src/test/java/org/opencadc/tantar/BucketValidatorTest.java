
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
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Test;
import org.junit.Assert;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.tantar.policy.ResolutionPolicyFactory;
import org.opencadc.tantar.policy.ResolutionPolicyStrategy;
import ca.nrc.cadc.util.Log4jInit;


public class BucketValidatorTest {

    static {
        Log4jInit.setLevel("org.opencadc.tantar", Level.DEBUG);
    }

    /**
     * Ensure the INVENTORY_IS_ALWAYS_RIGHT policy will maintain the integrity of the Inventory (Artifacts) over that
     * of the Storage Adapter (StorageMetadata).
     *
     * @throws Exception For anything unexpected.
     */
    @Test
    public void validateInventoryIsAlwaysRight() throws Exception {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final Reporter reporter = new Reporter(getTestLogger(byteArrayOutputStream));

        // **** Create the Inventory content.
        final List<Artifact> testArtifactList = new ArrayList<>();
        final Artifact artifactOne =
                new Artifact(URI.create("cadc:TESTBUCKET/myfile88.fits"), URI.create("md5:8899"), new Date(), 88L);
        artifactOne.storageLocation = new StorageLocation(URI.create("ad:123456"));
        testArtifactList.add(artifactOne);

        final Artifact artifactTwo =
                new Artifact(URI.create("cadc:TESTBUCKET/myfile99.fits"), URI.create("md5:9900"), new Date(), 99L);
        artifactTwo.storageLocation = new StorageLocation(URI.create("ceph:7890AB"));
        testArtifactList.add(artifactTwo);

        final Artifact artifactThree =
                new Artifact(URI.create("cadc:TESTBUCKET/myfile100.fits"), URI.create("md5:10001"), new Date(),
                             100L);
        artifactThree.storageLocation = new StorageLocation(URI.create("s3:CDEF00"));
        testArtifactList.add(artifactThree);

        testArtifactList.sort(Comparator.comparing(o -> o.storageLocation));
        // **** End Create the Storage Adapter content.

        // **** Create the Storage Adapter content.
        final List<StorageMetadata> testStorageMetadataList = new ArrayList<>();
        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ad:123456")),
                                                        URI.create("md5:8899"), 88L));
        testStorageMetadataList.get(0).contentLastModified = artifactOne.getLastModified();

        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ceph:78787878")),
                                                        URI.create("md5:0055998"), 99L));

        testStorageMetadataList.add(
                new StorageMetadata(new StorageLocation(URI.create("s3:CDEF00")),
                                    URI.create("md5:2222"), 100L));
        testStorageMetadataList.sort(Comparator.comparing(StorageMetadata::getStorageLocation));
        // **** End Create the Storage Adapter content.

        final BucketValidator testSubject =
                new BucketValidator("TESTBUCKET", null,
                                    ResolutionPolicyFactory.createPolicy(
                                            ResolutionPolicyStrategy.INVENTORY_IS_ALWAYS_RIGHT.name(),
                                            reporter, true)) {
                    @Override
                    Iterator<StorageMetadata> iterateStorage() {
                        return testStorageMetadataList.iterator();
                    }

                    @Override
                    Iterator<Artifact> iterateInventory() {
                        return testArtifactList.iterator();
                    }
                };

        testSubject.validate();

        /*
        The two iterators only share a single file, which is located at ad:123456.  As a result, it should be left
        alone.
         */

        final List<String> outputLines =
                Arrays.asList(new String(byteArrayOutputStream.toByteArray()).split("\n"));
        System.out.println(String.format("Message lines are \n\n%s\n\n", outputLines));
        assertListContainsMessage(outputLines,
                                  "Removing Unknown File StorageLocation[ceph:78787878] as per policy.");
        assertListContainsMessage(outputLines,
                                  "Retrieving File StorageLocation[ceph:7890AB] as per policy.");
        assertListContainsMessage(outputLines,
                                  "Replacing File StorageLocation[s3:CDEF00] as per policy.");
        assertListContainsMessage(outputLines, "Artifact StorageLocation[ad:123456] is valid as per policy.");
    }

    /**
     * Ensure the INVENTORY_IS_ALWAYS_RIGHT policy will maintain the integrity of the Inventory (Artifacts) over that
     * of the Storage Adapter (StorageMetadata).
     *
     * @throws Exception For anything unexpected.
     */
    @Test
    public void validateInventoryIsAlwaysRightEmptyStorage() throws Exception {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final Reporter reporter = new Reporter(getTestLogger(byteArrayOutputStream));

        // **** Create the Inventory content.
        final List<Artifact> testArtifactList = new ArrayList<>();
        final Artifact artifactOne =
                new Artifact(URI.create("cadc:TESTBUCKET/myfile88.fits"), URI.create("md5:8899"), new Date(), 88L);
        artifactOne.storageLocation = new StorageLocation(URI.create("ad:123456"));
        testArtifactList.add(artifactOne);

        final Artifact artifactTwo =
                new Artifact(URI.create("cadc:TESTBUCKET/myfile99.fits"), URI.create("md5:9900"), new Date(), 99L);
        artifactTwo.storageLocation = new StorageLocation(URI.create("ceph:7890AB"));
        testArtifactList.add(artifactTwo);

        final Artifact artifactThree =
                new Artifact(URI.create("cadc:TESTBUCKET/myfile100.fits"), URI.create("md5:10001"), new Date(),
                             100L);
        artifactThree.storageLocation = new StorageLocation(URI.create("s3:CDEF00"));
        testArtifactList.add(artifactThree);

        testArtifactList.sort(Comparator.comparing(o -> o.storageLocation));
        // **** End Create the Storage Adapter content.

        final BucketValidator testSubject =
                new BucketValidator("TESTBUCKET", null,
                                    ResolutionPolicyFactory.createPolicy(
                                            ResolutionPolicyStrategy.INVENTORY_IS_ALWAYS_RIGHT.name(),
                                            reporter, true)) {
                    @Override
                    Iterator<StorageMetadata> iterateStorage() {
                        return Collections.emptyIterator();
                    }

                    @Override
                    Iterator<Artifact> iterateInventory() {
                        return testArtifactList.iterator();
                    }
                };

        testSubject.validate();

        final List<String> outputLines =
                Arrays.asList(new String(byteArrayOutputStream.toByteArray()).split("\n"));
        System.out.println(String.format("Message lines are \n\n%s\n\n", outputLines));
        assertListContainsMessage(outputLines,
                                  "Retrieving File StorageLocation[ad:123456] as per policy.");
        assertListContainsMessage(outputLines,
                                  "Retrieving File StorageLocation[ceph:7890AB] as per policy.");
        assertListContainsMessage(outputLines,
                                  "Retrieving File StorageLocation[s3:CDEF00] as per policy.");
    }

    /**
     * Ensure the STORAGE_IS_ALWAYS_RIGHT policy will maintain the integrity of the Storage Adapter (StorageMetadata)
     * over that of the Inventory (Artifact).
     *
     * @throws Exception For anything unexpected.
     */
    @Test
    public void validateStorageIsAlwaysRight() throws Exception {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final Reporter reporter = new Reporter(getTestLogger(byteArrayOutputStream));

        // **** Create the Inventory content.
        final List<Artifact> testArtifactList = new ArrayList<>();
        final Artifact artifactOne =
                new Artifact(URI.create("cadc:TESTBUCKET/myfile88.fits"), URI.create("md5:8899"), new Date(), 88L);
        artifactOne.storageLocation = new StorageLocation(URI.create("ad:123456"));
        testArtifactList.add(artifactOne);

        final Artifact artifactTwo =
                new Artifact(URI.create("cadc:TESTBUCKET/myfile99.fits"), URI.create("md5:9900"), new Date(), 99L);
        artifactTwo.storageLocation = new StorageLocation(URI.create("ceph:7890AB"));
        testArtifactList.add(artifactTwo);

        final Artifact artifactThree =
                new Artifact(URI.create("cadc:TESTBUCKET/myfile100.fits"), URI.create("md5:10001"), new Date(),
                             100L);
        artifactThree.storageLocation = new StorageLocation(URI.create("s3:CDEF00"));
        testArtifactList.add(artifactThree);

        testArtifactList.sort(Comparator.comparing(o -> o.storageLocation));
        // **** End Create the Storage Adapter content.

        // **** Create the Storage Adapter content.
        final List<StorageMetadata> testStorageMetadataList = new ArrayList<>();
        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ad:123456")),
                                                        URI.create("md5:8899"), 88L));
        testStorageMetadataList.get(0).contentLastModified = artifactOne.getLastModified();

        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ceph:78787878")),
                                                        URI.create("md5:0055998"), 99L));

        testStorageMetadataList.add(
                new StorageMetadata(new StorageLocation(URI.create("s3:CDEF00")),
                                    URI.create("md5:2222"), 100L));
        testStorageMetadataList.sort(Comparator.comparing(StorageMetadata::getStorageLocation));
        // **** End Create the Storage Adapter content.

        final BucketValidator testSubject =
                new BucketValidator("TESTBUCKET", null,
                                    ResolutionPolicyFactory.createPolicy(
                                            ResolutionPolicyStrategy.STORAGE_IS_ALWAYS_RIGHT.name(),
                                            reporter, true)) {
                    @Override
                    Iterator<StorageMetadata> iterateStorage() {
                        return testStorageMetadataList.iterator();
                    }

                    @Override
                    Iterator<Artifact> iterateInventory() {
                        return testArtifactList.iterator();
                    }
                };

        testSubject.validate();

        /*
        The two iterators only share a single file, which is located at ad:123456.  As a result, it should be left
        alone.
         */

        final List<String> outputLines =
                Arrays.asList(new String(byteArrayOutputStream.toByteArray()).split("\n"));
        System.out.println(String.format("Message lines are \n\n%s\n\n", outputLines));
        assertListContainsMessage(outputLines,
                                  "Adding Artifact StorageLocation[ceph:78787878] as per policy.");
        assertListContainsMessage(outputLines,
                                  "Removing Unknown Artifact StorageLocation[ceph:7890AB] as per policy.");
        assertListContainsMessage(outputLines,
                                  "Replacing Artifact StorageLocation[s3:CDEF00] as per policy.");
        assertListContainsMessage(outputLines, "Storage Metadata StorageLocation[ad:123456] is valid as per policy.");
    }

    /**
     * Ensure the STORAGE_IS_ALWAYS_RIGHT policy will maintain the integrity of the Storage Adapter (StorageMetadata)
     * over that of the Inventory (Artifact).  This will use an empty inventory to simulate a first run.
     *
     * @throws Exception For anything unexpected.
     */
    @Test
    public void validateStorageIsAlwaysRightEmptyInventory() throws Exception {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final Reporter reporter = new Reporter(getTestLogger(byteArrayOutputStream));

        // **** Create the Storage Adapter content.
        final List<StorageMetadata> testStorageMetadataList = new ArrayList<>();
        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ad:123456")),
                                                        URI.create("md5:8899"), 88L));

        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ad:78787878")),
                                                        URI.create("md5:0055998"), 99L));

        testStorageMetadataList.add(
                new StorageMetadata(new StorageLocation(URI.create("ad:CDEF00")),
                                    URI.create("md5:2222"), 100L));
        testStorageMetadataList.sort(Comparator.comparing(StorageMetadata::getStorageLocation));
        // **** End Create the Storage Adapter content.

        final BucketValidator testSubject =
                new BucketValidator("TESTBUCKET", null,
                                    ResolutionPolicyFactory.createPolicy(
                                            ResolutionPolicyStrategy.STORAGE_IS_ALWAYS_RIGHT.name(),
                                            reporter, true)) {
                    @Override
                    Iterator<StorageMetadata> iterateStorage() {
                        return testStorageMetadataList.iterator();
                    }

                    @Override
                    Iterator<Artifact> iterateInventory() {
                        return Collections.emptyIterator();
                    }
                };

        testSubject.validate();

        final List<String> outputLines =
                Arrays.asList(new String(byteArrayOutputStream.toByteArray()).split("\n"));
        System.out.println(String.format("Message lines are \n\n%s\n\n", outputLines));
        assertListContainsMessage(outputLines,
                                  "Adding Artifact StorageLocation[ad:123456] as per policy.");
        assertListContainsMessage(outputLines,
                                  "Adding Artifact StorageLocation[ad:78787878] as per policy.");
        assertListContainsMessage(outputLines,
                                  "Adding Artifact StorageLocation[ad:CDEF00] as per policy.");
    }

    private void assertListContainsMessage(final List<String> outputLines, final String message) {
        Assert.assertTrue(String.format("Output does not contain %s", message),
                          outputLines.stream().anyMatch(s -> s.contains(message)));
    }

    private Logger getTestLogger(final OutputStream outputStream) {
        final Logger logger = Logger.getLogger(BucketValidatorTest.class);
        final WriterAppender testAppender = new WriterAppender(new SimpleLayout(), outputStream);
        testAppender.setName("Test Writer Appender");

        logger.removeAllAppenders();
        logger.addAppender(testAppender);

        return logger;
    }
}

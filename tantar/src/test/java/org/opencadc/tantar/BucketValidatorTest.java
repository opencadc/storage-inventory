
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

import ca.nrc.cadc.util.Log4jInit;
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
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.tantar.policy.InventoryIsAlwaysRight;
import org.opencadc.tantar.policy.RecoverFromStorage;
import org.opencadc.tantar.policy.StorageIsAlwaysRight;

/**
 * Test that ResolutionPolicy implementations log the correct events.
 * 
 * @author pdowler
 */
public class BucketValidatorTest {
    private static final Logger log = Logger.getLogger(BucketValidatorTest.class);
    
    static {
        Log4jInit.setLevel("org.opencadc.tantar", Level.INFO);
    }

    /**
     * Ensure the INVENTORY_IS_ALWAYS_RIGHT policy will maintain the integrity of the Inventory (Artifacts) over that
     * of the Storage Adapter (StorageMetadata).
     *
     * @throws Exception For anything unexpected.
     */
    @Test
    public void validateInventoryIsAlwaysRight() throws Exception {
        final Reporter reporter = new Reporter(log);

        // **** Create the Inventory content.
        final List<Artifact> testArtifactList = new ArrayList<>();
        final URI artifactOneContentChecksum = URI.create("md5:" + random16Bytes());
        final Artifact artifactOne =
                new Artifact(URI.create("cadc:a/myfile88.fits"), artifactOneContentChecksum, new Date(), 88L);
        artifactOne.storageLocation = new StorageLocation(URI.create("ad:123456"));
        testArtifactList.add(artifactOne);

        final Artifact artifactTwo =
                new Artifact(URI.create("cadc:b/myfile99.fits"), URI.create("md5:" + random16Bytes()), new Date(), 99L);
        artifactTwo.storageLocation = new StorageLocation(URI.create("ceph:7890AB"));
        testArtifactList.add(artifactTwo);

        final Artifact artifactThree =
                new Artifact(URI.create("cadc:c/myfile100.fits"), URI.create("md5:" + random16Bytes()), new Date(),
                             100L);
        artifactThree.storageLocation = new StorageLocation(URI.create("s3:CDEF00"));
        testArtifactList.add(artifactThree);

        testArtifactList.sort(Comparator.comparing(o -> o.storageLocation));
        // **** End Create the Storage Adapter content.

        // **** Create the Storage Adapter content.
        final List<StorageMetadata> testStorageMetadataList = new ArrayList<>();
        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ad:123456")),
                                                        artifactOneContentChecksum, 88L, artifactOne.getContentLastModified()));

        StorageMetadata d1 = new StorageMetadata(new StorageLocation(URI.create("ceph:78787878")),
                                                        URI.create("md5:" + random16Bytes()), 99L, new Date());
        testStorageMetadataList.add(d1);

        StorageMetadata d2 = new StorageMetadata(new StorageLocation(URI.create("s3:CDEF00")),
                                    URI.create("md5:" + random16Bytes()), 100L, new Date());
        testStorageMetadataList.add(d2);
        testStorageMetadataList.sort(Comparator.comparing(StorageMetadata::getStorageLocation));
        // **** End Create the Storage Adapter content.

        final TestEventListener testEventListener = new TestEventListener();

        final BucketValidator testSubject =
                new BucketValidator(Arrays.asList("a", "b", "c"), null,
                                    new Subject(), true, reporter, new InventoryIsAlwaysRight(testEventListener, reporter),
                                    null, null, null) {
                    @Override
                    Iterator<StorageMetadata> getStorageMetadataIterator() {
                        return testStorageMetadataList.iterator();
                    }

                    @Override
                    Iterator<Artifact> iterateInventory() {
                        return testArtifactList.iterator();
                    }

                    @Override
                    boolean isObsoleteStorageLocation(StorageMetadata storageMetadata) {
                        return false;
                    }

                };

        testSubject.validate();

        Assert.assertTrue(testEventListener.deletedStorage.contains(d1));
        Assert.assertTrue(testEventListener.deletedStorage.contains(d2));
        Assert.assertTrue(testEventListener.cleared.contains(artifactTwo));
        Assert.assertTrue(testEventListener.cleared.contains(artifactThree));
        
        Assert.assertFalse(testEventListener.deleted.contains(artifactOne));
        Assert.assertFalse(testEventListener.replaced.contains(artifactOne));
        Assert.assertFalse(testEventListener.cleared.contains(artifactOne));
        Assert.assertFalse(testEventListener.updated.contains(artifactOne));
    }

    /**
     * Ensure the INVENTORY_IS_ALWAYS_RIGHT policy will maintain the integrity of the Inventory (Artifacts) over that
     * of the Storage Adapter (StorageMetadata).
     *
     * @throws Exception For anything unexpected.
     */
    @Test
    public void validateInventoryIsAlwaysRightEmptyStorage() throws Exception {
        final Reporter reporter = new Reporter(log);

        // **** Create the Inventory content.
        final List<Artifact> testArtifactList = new ArrayList<>();
        final Artifact artifactOne =
                new Artifact(URI.create("cadc:a/myfile88.fits"), URI.create("md5:" + random16Bytes()), new Date(), 88L);
        artifactOne.storageLocation = new StorageLocation(URI.create("ad:123456"));
        testArtifactList.add(artifactOne);

        final Artifact artifactTwo =
                new Artifact(URI.create("cadc:c/myfile99.fits"), URI.create("md5:" + random16Bytes()), new Date(), 99L);
        artifactTwo.storageLocation = new StorageLocation(URI.create("ceph:7890AB"));
        testArtifactList.add(artifactTwo);

        final Artifact artifactThree =
                new Artifact(URI.create("cadc:b/myfile100.fits"), URI.create("md5:" + random16Bytes()), new Date(),
                             100L);
        artifactThree.storageLocation = new StorageLocation(URI.create("s3:CDEF00"));
        testArtifactList.add(artifactThree);

        testArtifactList.sort(Comparator.comparing(o -> o.storageLocation));
        // **** End Create the Storage Adapter content.

        final TestEventListener testEventListener = new TestEventListener();

        final BucketValidator testSubject =
                new BucketValidator(Arrays.asList("a", "b", "c"), null,
                                    new Subject(), true, reporter, new InventoryIsAlwaysRight(testEventListener, reporter),
                                    null, null, null) {
                    @Override
                    Iterator<StorageMetadata> getStorageMetadataIterator() {
                        return Collections.emptyIterator();
                    }

                    @Override
                    Iterator<Artifact> iterateInventory() {
                        return testArtifactList.iterator();
                    }

                    @Override
                    boolean isObsoleteStorageLocation(StorageMetadata storageMetadata) {
                        return false;
                    }
                };

        testSubject.validate();

        Assert.assertTrue(testEventListener.cleared.contains(artifactOne));
        Assert.assertTrue(testEventListener.cleared.contains(artifactTwo));
        Assert.assertTrue(testEventListener.cleared.contains(artifactThree));
    }

    /**
     * Ensure the STORAGE_IS_ALWAYS_RIGHT policy will maintain the integrity of the Storage Adapter (StorageMetadata)
     * over that of the Inventory (Artifact).
     *
     * @throws Exception For anything unexpected.
     */
    @Test
    public void validateStorageIsAlwaysRight() throws Exception {
        final Reporter reporter = new Reporter(log);

        // **** Create the Inventory content.
        final List<Artifact> testArtifactList = new ArrayList<>();
        final URI artifactOneContentChecksum = URI.create("md5:" + random16Bytes());
        final Artifact artifactOne =
                new Artifact(URI.create("cadc:c/myfile88.fits"), artifactOneContentChecksum, new Date(), 88L);
        artifactOne.storageLocation = new StorageLocation(URI.create("ad:123456"));
        testArtifactList.add(artifactOne);

        final Artifact artifactTwo =
                new Artifact(URI.create("cadc:a/myfile99.fits"), URI.create("md5:" + random16Bytes()), new Date(), 99L);
        artifactTwo.storageLocation = new StorageLocation(URI.create("ceph:7890AB"));
        testArtifactList.add(artifactTwo);

        final Artifact artifactThree =
                new Artifact(URI.create("cadc:b/myfile100.fits"), URI.create("md5:" + random16Bytes()), new Date(),
                             100L);
        artifactThree.storageLocation = new StorageLocation(URI.create("s3:CDEF00"));
        testArtifactList.add(artifactThree);

        testArtifactList.sort(Comparator.comparing(o -> o.storageLocation));
        // **** End Create the Storage Adapter content.

        // **** Create the Storage Adapter content.
        final List<StorageMetadata> testStorageMetadataList = new ArrayList<>();
        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ad:123456")),
                                                        artifactOneContentChecksum, 88L, artifactOne.getContentLastModified()));

        StorageMetadata c1 = new StorageMetadata(new StorageLocation(URI.create("ceph:78787878")),
                                                        URI.create("md5:" + random16Bytes()), 99L, new Date());
        testStorageMetadataList.add(c1);

        testStorageMetadataList.add(
                new StorageMetadata(new StorageLocation(URI.create("s3:CDEF00")),
                                    URI.create("md5:" + random16Bytes()), 100L, new Date()));
        testStorageMetadataList.sort(Comparator.comparing(StorageMetadata::getStorageLocation));
        // **** End Create the Storage Adapter content.

        final TestEventListener testEventListener = new TestEventListener();

        final BucketValidator testSubject =
                new BucketValidator(Arrays.asList("a", "b", "c"), null,
                                    new Subject(), true, reporter, new StorageIsAlwaysRight(testEventListener, reporter),
                                    null, null, null) {
                    @Override
                    Iterator<StorageMetadata> getStorageMetadataIterator() {
                        return testStorageMetadataList.iterator();
                    }

                    @Override
                    Iterator<Artifact> iterateInventory() {
                        return testArtifactList.iterator();
                    }

                    @Override
                    boolean isObsoleteStorageLocation(StorageMetadata storageMetadata) {
                        return false;
                    }
                };

        testSubject.validate();

        Assert.assertTrue(testEventListener.created.contains(c1));
        Assert.assertTrue(testEventListener.deleted.contains(artifactTwo));
        Assert.assertTrue(testEventListener.replaced.contains(artifactThree));

        // valid
        Assert.assertFalse(testEventListener.deleted.contains(artifactOne));
        Assert.assertFalse(testEventListener.replaced.contains(artifactOne));
        Assert.assertFalse(testEventListener.cleared.contains(artifactOne));
        Assert.assertFalse(testEventListener.updated.contains(artifactOne));
    }

    /**
     * Ensure the STORAGE_IS_ALWAYS_RIGHT policy will maintain the integrity of the Storage Adapter (StorageMetadata)
     * over that of the Inventory (Artifact).  This will use an empty inventory to simulate a first run.
     *
     * @throws Exception For anything unexpected.
     */
    @Test
    public void validateStorageIsAlwaysRightEmptyInventory() throws Exception {
        final Reporter reporter = new Reporter(log);

        // **** Create the Storage Adapter content.
        final List<StorageMetadata> testStorageMetadataList = new ArrayList<>();
        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ad:123456")),
                                                        URI.create("md5:" + random16Bytes()), 88L, new Date()));

        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ad:78787878")),
                                                        URI.create("md5:" + random16Bytes()), 99L, new Date()));

        testStorageMetadataList.add(
                new StorageMetadata(new StorageLocation(URI.create("ad:CDEF00")),
                                    URI.create("md5:" + random16Bytes()), 100L, new Date()));
        testStorageMetadataList.sort(Comparator.comparing(StorageMetadata::getStorageLocation));
        // **** End Create the Storage Adapter content.

        final TestEventListener testEventListener = new TestEventListener();

        final BucketValidator testSubject =
                new BucketValidator(Arrays.asList("a", "b", "c"), null,
                                    new Subject(), true, reporter, new StorageIsAlwaysRight(testEventListener, reporter),
                                    null, null, null) {
                    @Override
                    Iterator<StorageMetadata> getStorageMetadataIterator() {
                        return testStorageMetadataList.iterator();
                    }

                    @Override
                    Iterator<Artifact> iterateInventory() {
                        return Collections.emptyIterator();
                    }

                    @Override
                    boolean isObsoleteStorageLocation(StorageMetadata storageMetadata) {
                        return false;
                    }
                };

        testSubject.validate();

        for (StorageMetadata sm : testStorageMetadataList) {
            Assert.assertTrue(testEventListener.created.contains(sm));
        }
    }

    /**
     * Ensure the RecoverFromStoragePolicy policy will maintain the integrity of the Storage Adapter (StorageMetadata)
     * over that of the Inventory (Artifact).  This will use an empty inventory to simulate a first run.
     *
     * @throws Exception For anything unexpected.
     */
    //@Test
    public void validateRecoverFromStoragePolicyEmptyInventory() throws Exception {
        final Reporter reporter = new Reporter(log);

        // **** Create the Storage Adapter content.
        final List<StorageMetadata> testStorageMetadataList = new ArrayList<>();
        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ad:123456")),
                                                        URI.create("md5:" + random16Bytes()), 88L, new Date()));

        testStorageMetadataList.add(new StorageMetadata(new StorageLocation(URI.create("ad:78787878")),
                                                        URI.create("md5:" + random16Bytes()), 99L, new Date()));

        testStorageMetadataList.add(
                new StorageMetadata(new StorageLocation(URI.create("ad:CDEF00")),
                                    URI.create("md5:" + random16Bytes()), 100L, new Date()));
        testStorageMetadataList.sort(Comparator.comparing(StorageMetadata::getStorageLocation));
        // **** End Create the Storage Adapter content.

        final TestEventListener testEventListener = new TestEventListener();

        final BucketValidator testSubject =
                new BucketValidator(Arrays.asList("a", "b", "c"), null,
                                    new Subject(), true, reporter, new RecoverFromStorage(testEventListener, reporter),
                                    null, null, null) {
                    @Override
                    Iterator<StorageMetadata> getStorageMetadataIterator() {
                        return testStorageMetadataList.iterator();
                    }

                    @Override
                    Iterator<Artifact> iterateInventory() {
                        return Collections.emptyIterator();
                    }

                    @Override
                    boolean isObsoleteStorageLocation(StorageMetadata storageMetadata) {
                        return false;
                    }
                };

        testSubject.validate();

        for (StorageMetadata sm : testStorageMetadataList) {
            Assert.assertTrue(testEventListener.created.contains(sm));
        }
    }

    String random16Bytes() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }
}

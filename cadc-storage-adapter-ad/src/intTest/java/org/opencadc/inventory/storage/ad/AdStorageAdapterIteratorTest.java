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
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.storage.StorageMetadata;

public class AdStorageAdapterIteratorTest {

    private static final Logger log = Logger.getLogger(AdStorageAdapterIteratorTest.class);

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
    public void testEmptyIterator() throws Exception {
        Subject.doAs(testSubject, (PrivilegedExceptionAction<Object>) () -> {
            final AdStorageAdapter adStorageAdapter = new AdStorageAdapter();

            // query should come back with 0
            String archiveName = "NOT_IN_ARCHIVE";
            try {
                Iterator<StorageMetadata> storageMetaIterator = adStorageAdapter.iterator(archiveName);
                Assert.assertTrue("iterator is not empty.", storageMetaIterator.hasNext() == false);
            } catch (Exception unexpected) {
                log.error("unexpected exception", unexpected);
                Assert.fail("unexpected exception: " + unexpected.getMessage());
            }

            return null;
        });
    }

    @Test
    public void testGetIteratorInvalidArchive() throws Exception {
        Subject.doAs(testSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                final AdStorageAdapter adStorageAdapter = new AdStorageAdapter();
                String archiveName = "";

                try {
                    Iterator<StorageMetadata> storageMetaIterator = adStorageAdapter.iterator(archiveName);
                    Assert.fail("iterator call should have failed but did not.");
                } catch (IllegalArgumentException expected) {
                    log.info("caught expected: " + expected);
                } catch (Exception unexpected) {
                    log.error("unexpected exception", unexpected);
                    Assert.fail("unexpected exception: " + unexpected.getMessage());
                }
                return null;
            }
        });
    }


    @Test
    public void testIteratorOrder() throws Exception {
        Subject.doAs(testSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                final AdStorageAdapter adStorageAdapter = new AdStorageAdapter();
                String archiveName = "IRIS";

                // Get first version of iterator
                SortedSet<StorageMetadata> sortedMeta = new TreeSet();
                try {
                    Iterator<StorageMetadata>  storageMetaIterator = adStorageAdapter.iterator(archiveName);
                    Assert.assertTrue("iterator has records", storageMetaIterator.hasNext());

                    // Create SortedSet for comparison
                    while (storageMetaIterator.hasNext()) {
                        StorageMetadata sm = storageMetaIterator.next();
                        // check that for IRIS storageID and artifact URI are different in scheme (ad vs cadc)
                        Assert.assertTrue("cadc".equals(sm.artifactURI.getScheme()));
                        Assert.assertTrue("ad".equals(sm.getStorageLocation().getStorageID().getScheme()));
                        Assert.assertTrue(
                                sm.artifactURI.getSchemeSpecificPart().equals(
                                        sm.getStorageLocation().getStorageID().getSchemeSpecificPart()));
                        sortedMeta.add(sm);
                    }
                } catch (Exception unexpected) {
                    log.error("unexpected exception", unexpected);
                    Assert.fail("unexpected exception: " + unexpected.getMessage());
                }
                
                Assert.assertFalse("found some records to compare", sortedMeta.isEmpty());
                
                log.info("found: " + sortedMeta.size() + " in " + archiveName);

                // Get second version of iterator
                Iterator<StorageMetadata> storageMeta = null;
                try {
                    storageMeta = adStorageAdapter.iterator(archiveName);
                    Assert.assertTrue("iterator has records", storageMeta.hasNext());
                } catch (Exception unexpected) {
                    log.error("unexpected exception getting iterator", unexpected);
                    Assert.fail("unexpected exception getting iterator: " + unexpected.getMessage());
                }

                // Compare relative ordering of StorageMetadata objects
                Iterator<StorageMetadata> sortedSetMeta = sortedMeta.iterator();

                while (sortedSetMeta.hasNext()) {
                    StorageMetadata expected = sortedSetMeta.next();
                    StorageMetadata actual = storageMeta.next();
                    log.info("compare: " + expected.getStorageLocation() + " vs " + actual.getStorageLocation());
                    if (!expected.equals(actual)) {
                        Assert.fail("ordering not correct.");
                    }
                }
                Assert.assertFalse("AdStorageAdapter.iterator now empty", storageMeta.hasNext());
                
                return null;
            }
        });
    }

    @Test
    public void testIteratorVOSpac() throws Exception {
        Subject.doAs(testSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                final AdStorageAdapter adStorageAdapter = new AdStorageAdapter();
                String storageBucket = "VOSpac:abcd";

                // Get first version of iterator
                SortedSet<StorageMetadata> sortedMeta = new TreeSet();
                try {
                    Iterator<StorageMetadata>  storageMetaIterator = adStorageAdapter.iterator(storageBucket);
                    Assert.assertTrue("iterator has records", storageMetaIterator.hasNext());

                    // Create SortedSet for comparison
                    while (storageMetaIterator.hasNext()) {
                        StorageMetadata sm = storageMetaIterator.next();
                        Assert.assertTrue("cadc".equals(sm.artifactURI.getScheme()));
                        Assert.assertTrue("ad".equals(sm.getStorageLocation().getStorageID().getScheme()));
                        sortedMeta.add(sm);
                    }
                } catch (Exception unexpected) {
                    log.error("unexpected exception", unexpected);
                    Assert.fail("unexpected exception: " + unexpected.getMessage());
                }

                Assert.assertFalse("found some records to compare", sortedMeta.isEmpty());

                log.info("found: " + sortedMeta.size() + " in " + storageBucket);

                // Get second version of iterator
                Iterator<StorageMetadata> storageMeta = null;
                try {
                    storageMeta = adStorageAdapter.iterator(storageBucket);
                    Assert.assertTrue("iterator has records", storageMeta.hasNext());
                } catch (Exception unexpected) {
                    log.error("unexpected exception getting iterator", unexpected);
                    Assert.fail("unexpected exception getting iterator: " + unexpected.getMessage());
                }

                // Compare relative ordering of StorageMetadata objects
                Iterator<StorageMetadata> sortedSetMeta = sortedMeta.iterator();

                while (sortedSetMeta.hasNext()) {
                    StorageMetadata expected = sortedSetMeta.next();
                    StorageMetadata actual = storageMeta.next();
                    log.info("compare: " + expected.getStorageLocation() + " vs " + actual.getStorageLocation());
                    if (!expected.equals(actual)) {
                        Assert.fail("ordering not correct.");
                    }
                }
                Assert.assertFalse("AdStorageAdapter.iterator now empty", storageMeta.hasNext());

                return null;
            }
        });
    }

    //@Test
    public void testIteratorStream() throws Exception {
        Subject.doAs(testSubject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                final AdStorageAdapter adStorageAdapter = new AdStorageAdapter();

                // query should come back with 0
                String archiveName = "CFHT";
                log.info("iterator/query START: " + archiveName);
                try {
                    Iterator<StorageMetadata> storageMetaIterator = adStorageAdapter.iterator(archiveName);
                    long num = 0L;
                    while (storageMetaIterator.hasNext()) {
                        StorageMetadata sm = storageMetaIterator.next();
                        num++;
                        System.out.println(String.format("%12d %s", num, sm));
                    }
                } catch (Exception unexpected) {
                    log.error("unexpected exception", unexpected);
                    Assert.fail("unexpected exception: " + unexpected.getMessage());
                }

                return null;
            }
        });
    }

}

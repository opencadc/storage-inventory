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
************************************************************************
*/

package org.opencadc.inventory.storage.swift;

import ca.nrc.cadc.util.Log4jInit;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.test.StorageAdapterBasicTest;
import static org.opencadc.inventory.storage.test.StorageAdapterBasicTest.TEST_NAMESPACE;
import org.opencadc.inventory.storage.test.TestUtil;

/**
 *
 * @author pdowler
 */
public class SwiftPreserveNamespaceTest extends StorageAdapterBasicTest {
    private static final Logger log = Logger.getLogger(SwiftPreserveNamespaceTest.class);

    static final List<String> PRESERVE = new ArrayList<>();
    
    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.javaswift.joss.client", Level.INFO);
        
        PRESERVE.add(StorageAdapterBasicTest.TEST_NAMESPACE);
    }
    
    final SwiftStorageAdapter swiftAdapter;
    
    public SwiftPreserveNamespaceTest() throws Exception {
        super(new SwiftStorageAdapter(true, System.getProperty("user.name") + "-single-test", 2, false, PRESERVE));
        this.swiftAdapter = (SwiftStorageAdapter) super.adapter; 
    }

    @Override
    public void cleanupBefore() throws Exception {
        log.info("cleanupBefore: START");
        Iterator<StorageMetadata> sbi = swiftAdapter.iterator(null, true);
        while (sbi.hasNext()) {
            StorageLocation loc = sbi.next().getStorageLocation();
            swiftAdapter.delete(loc, true);
            log.info("\tdeleted: " + loc);
        }
        log.info("cleanupBefore: DONE");        
    }
    
    @Test
    public void testCleanupOnly() {
        log.info("testCleanupOnly: no-op");
    }
    
    @Test
    public void testIteratorOverPreserved() {
        int iterNum = 13;
        long datalen = 8192L;
        try {
            SortedSet<StorageMetadata> expected = new TreeSet<>();
            for (int i = 0; i < iterNum; i++) {
                URI artifactURI = URI.create(TEST_NAMESPACE + "TEST/testIterator-" + i);
                NewArtifact na = new NewArtifact(artifactURI);
                na.contentLength = (long) datalen;
                StorageMetadata sm = adapter.put(na, TestUtil.getInputStreamOfRandomBytes(datalen), null);
                log.debug("testIterator put: " + artifactURI + " to " + sm.getStorageLocation());
                expected.add(sm);
            }
            
            log.info("testIterator created: " + expected.size());
            
            // full iterator
            List<StorageMetadata> actual = new ArrayList<>();
            Iterator<StorageMetadata> iter = adapter.iterator();
            while (iter.hasNext()) {
                StorageMetadata sm = iter.next();
                log.debug("found: " + sm.getStorageLocation() + " " + sm.getContentLength() + " " + sm.getContentChecksum());
                actual.add(sm);
            }
            
            Assert.assertEquals("iterator.size", expected.size(), actual.size());
            Iterator<StorageMetadata> ei = expected.iterator();
            Iterator<StorageMetadata> ai = actual.iterator();
            while (ei.hasNext()) {
                StorageMetadata em = ei.next();
                StorageMetadata am = ai.next();
                log.debug("compare: " + em.getStorageLocation() + " vs " + am.getStorageLocation());
                Assert.assertEquals("order", em, am);
                Assert.assertTrue("valid", am.isValid());
                Assert.assertEquals("length", em.getContentLength(), am.getContentLength());
                Assert.assertEquals("checksum", em.getContentChecksum(), am.getContentChecksum());
                
                Assert.assertEquals("artifactURI", em.getArtifactURI(), am.getArtifactURI());
                
                Assert.assertNotNull("contentLastModified", am.getContentLastModified());
                Assert.assertEquals("contentLastModified", em.getContentLastModified(), am.getContentLastModified());
            }
            
            // delete half of the items
            boolean odd = true;
            int numPreserved = 0;
            for (StorageMetadata sm : expected) {
                if (odd) {
                    log.info("delete: " + sm.getStorageLocation());
                    adapter.delete(sm.getStorageLocation());
                    numPreserved++;
                } else {
                    log.info("keep: " + sm.getStorageLocation());
                }
                odd = !odd;
            }
            
            int foundPreserved = 0;
            ei = expected.iterator();
            ai = adapter.iterator(null, true);
            while (ei.hasNext()) {
                StorageMetadata em = ei.next();
                StorageMetadata am = ai.next();
                log.info("compare: " + em.getStorageLocation() + " vs " + am.getStorageLocation() + " dp=" + am.deletePreserved);
                if (am.deletePreserved) {
                    foundPreserved++;
                }
                Assert.assertEquals("order", em, am);
                Assert.assertTrue("valid", am.isValid());
                Assert.assertEquals("length", em.getContentLength(), am.getContentLength());
                Assert.assertEquals("checksum", em.getContentChecksum(), am.getContentChecksum());
                
                Assert.assertEquals("artifactURI", em.getArtifactURI(), am.getArtifactURI());
                
                Assert.assertNotNull("contentLastModified", am.getContentLastModified());
                
                // setting delete-preserved attr modifies this timestamp
                //Assert.assertEquals("contentLastModified", em.getContentLastModified(), am.getContentLastModified());
            }
            Assert.assertEquals(numPreserved, foundPreserved);
            
            // rely on cleanup()
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
 
}

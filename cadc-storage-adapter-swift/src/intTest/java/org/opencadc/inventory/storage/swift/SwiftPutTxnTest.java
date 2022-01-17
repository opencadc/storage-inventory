/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2021.                            (c) 2021.
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
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Iterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.test.StorageAdapterPutTxnTest;

/**
 *
 * @author pdowler
 */
public class SwiftPutTxnTest extends StorageAdapterPutTxnTest {
    private static final Logger log = Logger.getLogger(SwiftPutTxnTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage.swift", Level.INFO);
    }
    
    final SwiftStorageAdapter swiftAdapter;
    
    public SwiftPutTxnTest() throws Exception {
        super(new SwiftStorageAdapter(true, System.getProperty("user.name") + "-txn-test", 1, true));
        this.swiftAdapter = (SwiftStorageAdapter) super.adapter;
    }
    
    @Before
    public void cleanupBefore() throws Exception {
        log.info("cleanupBefore: START");
        Iterator<StorageMetadata> sbi = swiftAdapter.iterator();
        while (sbi.hasNext()) {
            StorageLocation loc = sbi.next().getStorageLocation();
            swiftAdapter.delete(loc);
            log.info("\tdeleted: " + loc);
        }
        Iterator<String> trans = swiftAdapter.listTransactions();
        while (trans.hasNext()) {
            String tid = trans.next();
            swiftAdapter.abortTransaction(tid);
            log.info("\tdeleted: txn " + tid);
        }
        log.info("cleanupBefore: DONE");        
    }
    
    @Test
    public void testCleanupOnly() {
        log.info("testCleanupOnly: no-op");
    }
    
    @Test
    public void testTransactionStatus() {
        try {
            URI uri = URI.create("cadc:TEST/testStartTransaction-null");
            PutTransaction txn = adapter.startTransaction(uri, null);
            log.info("null content length: " + txn);
            Assert.assertNotNull(txn);
            Assert.assertNull(txn.getMinSegmentSize());
            Assert.assertNull(txn.getMaxSegmentSize());
            Assert.assertFalse(((SwiftPutTransaction) txn).dynamicLargeObject);
            
            PutTransaction ts = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts);
            Assert.assertEquals(txn.getMinSegmentSize(), ts.getMinSegmentSize());
            Assert.assertEquals(txn.getMaxSegmentSize(), ts.getMaxSegmentSize());
            Assert.assertEquals(((SwiftPutTransaction) txn).dynamicLargeObject, ((SwiftPutTransaction) ts).dynamicLargeObject);
            
            adapter.abortTransaction(txn.getID());
            try {
                PutTransaction nf = adapter.getTransactionStatus(txn.getID());
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            uri = URI.create("cadc:TEST/testStartTransaction-small");
            Long len = 1024L;
            txn = adapter.startTransaction(uri, len);
            log.info("small content length: " + txn);
            Assert.assertNotNull(txn);
            Assert.assertNull(txn.getMinSegmentSize());
            Assert.assertNull(txn.getMaxSegmentSize());
            Assert.assertFalse(((SwiftPutTransaction) txn).dynamicLargeObject);
            
            ts = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts);
            Assert.assertEquals(txn.getMinSegmentSize(), ts.getMinSegmentSize());
            Assert.assertEquals(txn.getMaxSegmentSize(), ts.getMaxSegmentSize());
            Assert.assertEquals(((SwiftPutTransaction) txn).dynamicLargeObject, ((SwiftPutTransaction) ts).dynamicLargeObject);
            
            adapter.abortTransaction(txn.getID());
            try {
                PutTransaction nf = adapter.getTransactionStatus(txn.getID());
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            uri = URI.create("cadc:TEST/testStartTransaction-large");
            len = (long) 3L * swiftAdapter.segmentMaxBytes / 2L;
            txn = adapter.startTransaction(uri, len);
            log.info("large content length: " + txn);
            Assert.assertNotNull(txn);
            Assert.assertNotNull(txn.getMinSegmentSize());
            Assert.assertTrue(txn.getMinSegmentSize() < swiftAdapter.segmentMaxBytes);
            Assert.assertNotNull(txn.getMaxSegmentSize());
            Assert.assertEquals(swiftAdapter.segmentMaxBytes, txn.getMaxSegmentSize().longValue());
            Assert.assertTrue(((SwiftPutTransaction) txn).dynamicLargeObject);
            
            ts = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts);
            Assert.assertEquals(txn.getMinSegmentSize(), ts.getMinSegmentSize());
            Assert.assertEquals(txn.getMaxSegmentSize(), ts.getMaxSegmentSize());
            Assert.assertEquals(((SwiftPutTransaction) txn).dynamicLargeObject, ((SwiftPutTransaction) ts).dynamicLargeObject);
            
            adapter.abortTransaction(txn.getID());
            try {
                PutTransaction nf = adapter.getTransactionStatus(txn.getID());
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            uri = URI.create("cadc:TEST/testStartTransaction-dlo-adjust");
            long tmp = swiftAdapter.segmentMinBytes;
            swiftAdapter.segmentMinBytes = 1024L;
            len = (long) 8192L;
            txn = adapter.startTransaction(uri, len);
            log.info("large content length: " + txn);
            Assert.assertNotNull(txn);
            Assert.assertNotNull(txn.getMinSegmentSize());
            Assert.assertEquals(swiftAdapter.segmentMinBytes, txn.getMinSegmentSize().longValue());
            Assert.assertNotNull(txn.getMaxSegmentSize());
            Assert.assertEquals(swiftAdapter.segmentMaxBytes, txn.getMaxSegmentSize().longValue());
            Assert.assertTrue(((SwiftPutTransaction) txn).dynamicLargeObject);
            
            ts = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts);
            Assert.assertEquals(txn.getMinSegmentSize(), ts.getMinSegmentSize());
            Assert.assertEquals(txn.getMaxSegmentSize(), ts.getMaxSegmentSize());
            Assert.assertEquals(((SwiftPutTransaction) txn).dynamicLargeObject, ((SwiftPutTransaction) ts).dynamicLargeObject);
            
            adapter.abortTransaction(txn.getID());
            try {
                PutTransaction nf = adapter.getTransactionStatus(txn.getID());
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } 
        
    }
    
    @Test
    public void testPutTransactionIterator() {
        int numArtifacts = 20;
        int numCommitted = 0;
        try {
            String dataString = "abcdefghijklmnopqrstuvwxyz\n";
            byte[] data = dataString.getBytes();
            
            for (int i = 0; i < numArtifacts; i++) {
                boolean commit = (i % 2 == 0);
                URI uri = URI.create("cadc:TEST/testPutTransactionIterator-" + i + "-" + commit);
                final NewArtifact newArtifact = new NewArtifact(uri);
                final ByteArrayInputStream source = new ByteArrayInputStream(data);

                PutTransaction txn = adapter.startTransaction(uri, null);
                StorageMetadata meta = adapter.put(newArtifact, source, txn.getID());
                Assert.assertNotNull(meta);
                log.info("put: " + meta);
                if (i % 2 == 0) {
                    log.info("commit: " + uri);
                    adapter.commitTransaction(txn.getID());
                    numCommitted++;
                }
            }
            
            int numFound = 0;
            Iterator<StorageMetadata> iter = adapter.iterator();
            while (iter.hasNext()) {
                StorageMetadata sm = iter.next();
                log.info("found: " + sm);
                Assert.assertTrue(sm.artifactURI.toASCIIString().endsWith("true"));
                numFound++;
            }
            Assert.assertEquals(numCommitted, numFound);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Override
    public void testPutFailResumeCommit() {
        long tmp = swiftAdapter.segmentMinBytes;
        swiftAdapter.segmentMinBytes = 10L;
        try {
            super.testPutFailResumeCommit();
        } finally {
            swiftAdapter.segmentMinBytes = tmp;
        }
    }

    @Override
    public void testPutResumeCommit() {
        long tmp = swiftAdapter.segmentMinBytes;
        swiftAdapter.segmentMinBytes = 10L;
        try {
            super.testPutResumeCommit();
        } finally {
            swiftAdapter.segmentMinBytes = tmp;
        }
        
    }

    @Override
    public void testPutRevertPesumeCommit() {
        long tmp = swiftAdapter.segmentMinBytes;
        swiftAdapter.segmentMinBytes = 10L;
        try {
            super.testPutRevertPesumeCommit();
        } finally {
            swiftAdapter.segmentMinBytes = tmp;
        }
    }
    
    
    
}

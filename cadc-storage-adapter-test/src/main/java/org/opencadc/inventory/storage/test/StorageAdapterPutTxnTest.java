/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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

package org.opencadc.inventory.storage.test;

import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Iterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 *
 * @author pdowler
 */
public class StorageAdapterPutTxnTest {
    private static final Logger log = Logger.getLogger(StorageAdapterPutTxnTest.class);

    public static final String TEST_NAMESPACE = "test:";
    
    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage", Level.INFO);
    }
    
    protected StorageAdapter adapter;
    
    public StorageAdapterPutTxnTest(StorageAdapter impl) {
        this.adapter = impl;
    }
    
    @Test
    public void testIncorrectURI() {
        try {
            String dataString1 = "abcdefghijklmnopqrstuvwxyz\n";
            String dataString2 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ\n";
            String dataString = dataString1 + dataString2;
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] data = dataString.getBytes();
            md.update(data);
            URI expectedChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            long expectedLength = data.length;
            
            URI uri = URI.create(TEST_NAMESPACE + "TEST/testIncorrectURI");
            NewArtifact newArtifact = new NewArtifact(uri);
            newArtifact.contentChecksum = expectedChecksum;
            newArtifact.contentLength = expectedLength;
            
            log.info("init");
            PutTransaction txn = adapter.startTransaction(uri, expectedLength);
            Assert.assertNotNull(txn);
            log.info("startTransaction: " + txn);
            
            // write part 1
            data = dataString1.getBytes();
            ByteArrayInputStream source = new ByteArrayInputStream(data);
            StorageMetadata meta1 = adapter.put(newArtifact, source, txn.getID());
            log.info("meta1: " + meta1);
            Assert.assertNotNull(meta1);
            Assert.assertEquals("length", data.length, meta1.getContentLength().longValue());
            log.info("put 1");
            
            // check txn status
            PutTransaction ts1 = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts1.storageMetadata);
            StorageMetadata txnMeta1 = ts1.storageMetadata;
            log.info("after write part 1: " + txnMeta1 + " in " + txn.getID());
            Assert.assertNotNull(txnMeta1);
            Assert.assertTrue("valid", txnMeta1.isValid());
            Assert.assertEquals("length", data.length, txnMeta1.getContentLength().longValue());

            // write part 2  
            URI uri2 = URI.create(TEST_NAMESPACE + "TEST/testIncorrectURI-diff");
            NewArtifact newArtifact2 = new NewArtifact(uri2);
            newArtifact.contentChecksum = expectedChecksum;
            newArtifact.contentLength = expectedLength;
            
            data = dataString2.getBytes();
            source = new ByteArrayInputStream(data);
            try {
                StorageMetadata meta2 = adapter.put(newArtifact2, source, txn.getID());
                Assert.fail("put succeeded: " + meta2);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            adapter.abortTransaction(txn.getID());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPutTransactionCommit() {
        try {
            String dataString = "abcdefghijklmnopqrstuvwxyz\n";
            byte[] data = dataString.getBytes();
            URI uri = URI.create(TEST_NAMESPACE + "TEST/testPutTransactionCommit");
            final NewArtifact newArtifact = new NewArtifact(uri);
            newArtifact.contentLength = (long) data.length;
            final ByteArrayInputStream source = new ByteArrayInputStream(data);
            
            log.info("init");
            final Long contentLength = (long) data.length;
            PutTransaction txn = adapter.startTransaction(uri, contentLength);
            Assert.assertNotNull(txn);
            log.info("startTransaction: " + txn);
            
            StorageMetadata meta = adapter.put(newArtifact, source, txn.getID());
            log.info("put");

            Assert.assertNotNull(meta);
            
            Iterator<StorageMetadata> iter = adapter.iterator();
            Assert.assertFalse("content not committed", iter.hasNext());
            
            PutTransaction ts = adapter.getTransactionStatus(txn.getID());
            log.info("uncommitted: " + ts);
            Assert.assertNotNull(ts);
            Assert.assertNotNull(ts.storageMetadata);
            StorageMetadata meta2 = ts.storageMetadata;
            log.info("testPutTransactionCommit: " + meta2 + " in " + txn.getID());
            Assert.assertNotNull(meta2);
            Assert.assertTrue("valid", meta2.isValid());
            
            log.info("commit: " + txn);
            StorageMetadata meta3 = adapter.commitTransaction(txn.getID());
            Assert.assertNotNull(meta3);
            log.info("commit");
            
            try {
                PutTransaction oldtxn = adapter.getTransactionStatus(txn.getID());
                Assert.fail("expected IllegalArgumentException, got: " + oldtxn);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            // get to verify
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(meta3.getStorageLocation(), bos);
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] actual = bos.toByteArray();
            md.update(actual);
            URI actualChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("testPutTransactionCommit get: " + actual.length + " " + actualChecksum);
            Assert.assertEquals("length", (long) meta3.getContentLength(), actual.length);
            Assert.assertEquals("checksum", meta3.getContentChecksum(), actualChecksum);

            // delete
            adapter.delete(meta3.getStorageLocation());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPutTransactionAbort() {
        try {
            String dataString = "abcdefghijklmnopqrstuvwxyz\n";
            byte[] data = dataString.getBytes();
            URI uri = URI.create(TEST_NAMESPACE + "TEST/testPutTransactionAbort");
            final NewArtifact newArtifact = new NewArtifact(uri);
            newArtifact.contentLength = (long) data.length;
            final ByteArrayInputStream source = new ByteArrayInputStream(data);
            
            log.info("init");
            final Long contentLength = (long) data.length;
            PutTransaction txn = adapter.startTransaction(uri, contentLength);
            Assert.assertNotNull(txn);
            log.info("startTransaction: " + txn);
            
            StorageMetadata meta = adapter.put(newArtifact, source, txn.getID());
            log.info("put");
            
            Assert.assertNotNull(meta);
            
            Iterator<StorageMetadata> iter = adapter.iterator();
            Assert.assertFalse("content not committed", iter.hasNext());
            
            PutTransaction ts = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts.storageMetadata);
            StorageMetadata meta2 = ts.storageMetadata;
            log.info("testPutTransactionAbort: " + meta2 + " in " + txn.getID());
            Assert.assertNotNull(meta2);
            Assert.assertTrue("valid", meta2.isValid());
            
            adapter.abortTransaction(txn.getID());
            log.info("abort");

            try {
                PutTransaction oldtxn = adapter.getTransactionStatus(txn.getID());
                Assert.fail("expected IllegalArgumentException, got: " + oldtxn);
            } catch (IllegalArgumentException expected) {
                log.info("verify txn gone: caught expected: " + expected);
            }
            
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                adapter.get(meta2.getStorageLocation(), bos);
                Assert.fail("expected ResourceNotFoundException, get succeeded");
            } catch (ResourceNotFoundException expected) {
                log.info("verify file gone: caught expected: " + expected);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPutResumeCommit_ExpectedLength() {
        testPutResumeCommit(true);
    }
    
    @Test
    public void testPutResumeCommit_UnknownLength() {
        testPutResumeCommit(false);
    }
            
    void testPutResumeCommit(boolean knownLength) {
        try {
            String dataString1 = "abcdefghijklmnopqrstuvwxyz\n";
            String dataString2 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ\n";
            String dataString = dataString1 + dataString2;
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] data = dataString.getBytes();
            md.update(data);
            final URI expectedChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            final long expectedLength = data.length;
            
            final URI uri = URI.create(TEST_NAMESPACE + "TEST/testPutResumeCommit");
            Long knownLengthVal = null;
            if (knownLength) {
                knownLengthVal = expectedLength;
            }
            
            log.info("init");
            PutTransaction txn = adapter.startTransaction(uri, knownLengthVal);
            Assert.assertNotNull(txn);
            log.info("startTransaction: " + txn);
            
            // write part 1
            final byte[] data1 = dataString1.getBytes();
            NewArtifact p1 = new NewArtifact(uri);
            p1.contentLength = (long) data1.length;
            ByteArrayInputStream source = new ByteArrayInputStream(data1);
            StorageMetadata meta1 = adapter.put(p1, source, txn.getID());
            log.info("meta1: " + meta1);
            Assert.assertNotNull(meta1);
            Assert.assertEquals("length", data1.length, meta1.getContentLength().longValue());
            log.info("put 1");
            
            // check txn status
            PutTransaction ts1 = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts1.storageMetadata);
            StorageMetadata txnMeta1 = ts1.storageMetadata;
            log.info("after write part 1: " + txnMeta1 + " in " + txn.getID());
            Assert.assertNotNull(txnMeta1);
            Assert.assertTrue("valid", txnMeta1.isValid());
            Assert.assertEquals("length", data1.length, txnMeta1.getContentLength().longValue());

            // write part 2            
            final byte[] data2 = dataString2.getBytes();
            NewArtifact p2 = new NewArtifact(uri);
            p2.contentLength = (long) data2.length;
            source = new ByteArrayInputStream(data2);
            StorageMetadata meta2 = adapter.put(p1, source, txn.getID());
            log.info("meta2: " + meta2);
            Assert.assertNotNull(meta2);
            Assert.assertEquals("length", expectedLength, meta2.getContentLength().longValue());
            log.info("put 2");
            
            // check txn status
            PutTransaction ts2 = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts2.storageMetadata);
            StorageMetadata txnMeta2 = ts2.storageMetadata;
            log.info("after write part 2: " + txnMeta2 + " in " + txn.getID());
            Assert.assertNotNull(txnMeta2);
            Assert.assertTrue("valid", txnMeta2.isValid());
            Assert.assertEquals("length", expectedLength, txnMeta2.getContentLength().longValue());
            
            StorageMetadata finalMeta = adapter.commitTransaction(txn.getID());
            log.info("commit");
            
            try {
                PutTransaction oldtxn = adapter.getTransactionStatus(txn.getID());
                Assert.fail("expected IllegalArgumentException, got: " + oldtxn);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            // get to verify
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(finalMeta.getStorageLocation(), bos);
            byte[] actual = bos.toByteArray();
            md.reset();
            md.update(actual);
            URI actualChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("testPutTransactionCommit get: " + actual.length + " " + actualChecksum);
            Assert.assertEquals("length", expectedLength, actual.length);
            Assert.assertEquals("length", finalMeta.getContentLength().longValue(), actual.length);
            Assert.assertEquals("checksum", finalMeta.getContentChecksum(), actualChecksum);
            Assert.assertEquals("checksum", expectedChecksum, actualChecksum);

            // get bytes ranges and reassemble to verify -- get 3 segments so middle overlaps the boundary
            long i1 = expectedLength / 3;
            long i2 = expectedLength * 2 / 3;
            ByteRange[] parts = new ByteRange[] {
                new ByteRange(0, i1),
                new ByteRange(i1, (i2 - i1)),
                new ByteRange(i2, (expectedLength - i2))
            };
            bos = new ByteArrayOutputStream();
            log.info("get part: " + parts[0]);
            adapter.get(finalMeta.getStorageLocation(), bos, parts[0]);
            log.info("get part: " + parts[1]);
            adapter.get(finalMeta.getStorageLocation(), bos, parts[1]);
            log.info("get part: " + parts[2]);
            adapter.get(finalMeta.getStorageLocation(), bos, parts[2]);
            actual = bos.toByteArray();
            md.reset();
            md.update(actual);
            actualChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("testPutTransactionCommit get-parts: " + actual.length + " " + actualChecksum);
            Assert.assertEquals("length", expectedLength, actual.length);
            Assert.assertEquals("checksum", expectedChecksum, actualChecksum);
            
            // delete
            adapter.delete(finalMeta.getStorageLocation());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    
    @Test
    public void testPutFailResumeCommit() {
        try {
            String dataString1 = "abcdefghijklmnopqrstuvwxyz\n";
            String dataString2 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ\n";
            String dataString = dataString1 + dataString2;
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] data = dataString.getBytes();
            md.update(data);
            final URI expectedChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            final long expectedLength = data.length;
            log.info("testPutFailResumeCommit expected: " + expectedLength + " " + expectedChecksum + "\n" + dataString);
            
            URI uri = URI.create(TEST_NAMESPACE + "TEST/testPutFailResumeCommit");
            NewArtifact newArtifact = new NewArtifact(uri);
            newArtifact.contentChecksum = expectedChecksum;
            newArtifact.contentLength = expectedLength;
            
            log.info("init");
            PutTransaction txn = adapter.startTransaction(uri, expectedLength);
            Assert.assertNotNull(txn);
            log.info("startTransaction: " + txn);
            
            log.info("START put 1 ok");
            data = dataString1.getBytes();
            InputStream source = new ByteArrayInputStream(data);
            StorageMetadata meta1 = adapter.put(newArtifact, source, txn.getID());
            log.info("put 1: " + meta1 + " in " + txn.getID());
            Assert.assertNotNull(meta1);
            Assert.assertEquals("length", data.length, meta1.getContentLength().longValue());
            log.info("DONE put 1");
            
            // check txn status
            PutTransaction ts1 = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts1.storageMetadata);
            StorageMetadata tmeta1 = ts1.storageMetadata;
            log.info("txn status after write part 1: " + tmeta1 + " in " + txn.getID());
            Assert.assertNotNull(tmeta1);
            Assert.assertTrue("valid", tmeta1.isValid());
            Assert.assertEquals("length", data.length, tmeta1.getContentLength().longValue());

            log.info("START put 2 fail(0)");
            data = dataString2.getBytes();
            source = getFailingInput(0, data); // no need for rollback to work
            try {
                StorageMetadata failedPut = adapter.put(newArtifact, source, txn.getID());
                Assert.fail("expected ReadException, got: " + failedPut);
            } catch (ReadException expected) {
                log.info("caught expected: " + expected);
            }
            PutTransaction ts2 = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts2.storageMetadata);
            StorageMetadata tmeta2 = ts2.storageMetadata;
            log.info("after write part 2 fail (0 bytes): " + tmeta2 + " in " + txn.getID());
            Assert.assertNotNull(tmeta2);
            Assert.assertEquals("length", data.length, tmeta2.getContentLength().longValue());
            log.info("DONE put 2 fail(0)");
            
            log.info("START put 3 fail(20)");
            data = dataString2.getBytes();
            source = getFailingInput(20, data); // fail after 20 bytes: need rollback to work
            try {
                StorageMetadata failedPut = adapter.put(newArtifact, source, txn.getID());
                Assert.fail("expected ReadException, got: " + failedPut);
            } catch (ReadException expected) {
                log.info("caught expected: " + expected);
            }
            ts2 = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts2.storageMetadata);
            tmeta2 = ts2.storageMetadata;
            log.info("after write part 2 fail (20 bytes): " + tmeta2 + " in " + txn.getID());
            Assert.assertNotNull(tmeta2);
            Assert.assertEquals("length", data.length, tmeta2.getContentLength().longValue());
            log.info("DONE put 3 fail(20)");
            
            // write part 2       
            log.info("START put 4 ok");
            source = new ByteArrayInputStream(data);
            StorageMetadata meta3 = adapter.put(newArtifact, source, txn.getID());
            Assert.assertNotNull(meta3);
            Assert.assertEquals("length", expectedLength, meta3.getContentLength().longValue());
            log.info("DONE put 4");
            
            // check txn status
            PutTransaction ts3 = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts3.storageMetadata);
            StorageMetadata tmeta3 = ts3.storageMetadata;
            log.info("after write part 2: " + tmeta3 + " in " + txn.getID());
            Assert.assertNotNull(tmeta3);
            Assert.assertTrue("valid", tmeta3.isValid());
            Assert.assertEquals("length", expectedLength, tmeta3.getContentLength().longValue());
            Assert.assertEquals("checksum", expectedChecksum, tmeta3.getContentChecksum());
            
            StorageMetadata finalMeta = adapter.commitTransaction(txn.getID());
            log.info("commit");
            
            try {
                PutTransaction oldtxn = adapter.getTransactionStatus(txn.getID());
                Assert.fail("expected IllegalArgumentException, got: " + oldtxn);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            // get to verify
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(finalMeta.getStorageLocation(), bos);
            byte[] actual = bos.toByteArray();
            String str = new String(actual);
            md.reset();
            md.update(actual);
            URI actualChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("testPutFailResumeCommit actual: " + actual.length + " " + actualChecksum + "\n" + str);
            Assert.assertEquals("length", expectedLength, actual.length);
            Assert.assertEquals("length", finalMeta.getContentLength().longValue(), actual.length);
            log.info("meta: " + finalMeta.getContentChecksum() + " vs " + actualChecksum);
            Assert.assertEquals("checksum", finalMeta.getContentChecksum(), actualChecksum);
            log.info("data: " + expectedChecksum + " vs " + actualChecksum);
            Assert.assertEquals("checksum", expectedChecksum, actualChecksum);

            // delete
            adapter.delete(finalMeta.getStorageLocation());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPutRevertPesumeCommit() {
        try {
            String dataString1 = "abcdefghijklmnopqrstuvwxyz\n";
            String dataString2 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ\n";
            MessageDigest md = MessageDigest.getInstance("MD5");
            
            byte[] data = dataString1.getBytes();
            md.update(data);
            final URI expectedChecksum1 = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("expected1: " + data.length + " " + expectedChecksum1);
            
            String dataString = dataString1 + dataString2;
            data = dataString.getBytes();
            md.update(data);
            final URI expectedChecksum2 = URI.create("md5:" + HexUtil.toHex(md.digest()));
            final long expectedLength = data.length;
            log.info("expected2: " + data.length + " " + expectedChecksum2);
            
            URI uri = URI.create(TEST_NAMESPACE + "TEST/testPutRevertPesumeCommit");
            NewArtifact newArtifact = new NewArtifact(uri);
            newArtifact.contentChecksum = expectedChecksum2;
            newArtifact.contentLength = expectedLength;
            
            log.info("init");
            PutTransaction txn = adapter.startTransaction(uri, expectedLength);
            Assert.assertNotNull(txn);
            log.info("startTransaction: " + txn);
            
            // write part 1
            data = dataString1.getBytes();
            ByteArrayInputStream source = new ByteArrayInputStream(data);
            StorageMetadata meta1 = adapter.put(newArtifact, source, txn.getID());
            log.info("meta1: " + meta1);
            Assert.assertNotNull(meta1);
            Assert.assertEquals("length", data.length, meta1.getContentLength().longValue());
            Assert.assertEquals("checksum", expectedChecksum1, meta1.getContentChecksum());
            log.info("put 1");
            
            // write part 2            
            data = dataString2.getBytes();
            source = new ByteArrayInputStream(data);
            StorageMetadata meta2 = adapter.put(newArtifact, source, txn.getID());
            log.info("meta2: " + meta2);
            Assert.assertNotNull(meta2);
            Assert.assertEquals("length", expectedLength, meta2.getContentLength().longValue());
            Assert.assertEquals("checksum", expectedChecksum2, meta2.getContentChecksum());
            log.info("put 2");

            // check txn status
            PutTransaction ts2 = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts2.storageMetadata);
            StorageMetadata txnMeta2 = ts2.storageMetadata;
            log.info("after write part 2: " + txnMeta2 + " in " + txn.getID());
            Assert.assertNotNull(txnMeta2);
            Assert.assertTrue("valid", txnMeta2.isValid());
            Assert.assertEquals("length", expectedLength, txnMeta2.getContentLength().longValue());
            Assert.assertEquals("checksum", expectedChecksum2, txnMeta2.getContentChecksum());

            // revert to 1
            PutTransaction reverted = adapter.revertTransaction(txn.getID());
            Assert.assertNotNull(reverted.storageMetadata);
            StorageMetadata revMeta = reverted.storageMetadata;
            log.info("after revert part 2: " + revMeta + " in " + txn.getID());
            Assert.assertNotNull(revMeta);
            Assert.assertTrue("valid", revMeta.isValid());
            Assert.assertEquals("length", data.length, revMeta.getContentLength().longValue());
            Assert.assertEquals("checksum", expectedChecksum1, revMeta.getContentChecksum());
                   
            // check txn status is consistent
            PutTransaction ts1 = adapter.getTransactionStatus(txn.getID());
            Assert.assertNotNull(ts1.storageMetadata);
            StorageMetadata txnMeta1 = ts1.storageMetadata;
            log.info("after write part 1: " + txnMeta1 + " in " + txn.getID());
            Assert.assertNotNull(txnMeta1);
            Assert.assertTrue("valid", txnMeta1.isValid());
            Assert.assertEquals("length", data.length, txnMeta1.getContentLength().longValue());
            Assert.assertEquals("checksum", expectedChecksum1, txnMeta1.getContentChecksum());
            
            // write part 2 again
            data = dataString2.getBytes();
            source = new ByteArrayInputStream(data);
            meta2 = adapter.put(newArtifact, source, txn.getID());
            log.info("meta2: " + meta2);
            Assert.assertNotNull(meta2);
            Assert.assertEquals("length", expectedLength, meta2.getContentLength().longValue());
            log.info("put 2");
            
            StorageMetadata finalMeta = adapter.commitTransaction(txn.getID());
            log.info("commit");
            
            try {
                PutTransaction oldtxn = adapter.getTransactionStatus(txn.getID());
                Assert.fail("expected IllegalArgumentException, got: " + oldtxn);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            // get to verify
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(finalMeta.getStorageLocation(), bos);
            byte[] actual = bos.toByteArray();
            md.reset();
            md.update(actual);
            URI actualChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("testPutTransactionCommit get: " + actual.length + " " + actualChecksum);
            Assert.assertEquals("length", expectedLength, actual.length);
            Assert.assertEquals("length", finalMeta.getContentLength().longValue(), actual.length);
            Assert.assertEquals("checksum", finalMeta.getContentChecksum(), actualChecksum);
            Assert.assertEquals("checksum", expectedChecksum2, actualChecksum);

            // delete
            adapter.delete(finalMeta.getStorageLocation());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    private InputStream getFailingInput(final int failAfter, final byte[] data) {
        return new InputStream() {
            int num = 0;
            
            @Override
            public int read() throws IOException {
                if (num >= failAfter) {
                    throw new IOException("failAfter: " + failAfter);
                }
                return data[num++];
            }
        };
    }
}

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

package org.opencadc.inventory.storage.test;

import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * Integration tests for get, put, delete, iterator.
 * 
 * @author pdowler
 */
public abstract class StorageAdapterBasicTest {
    private static final Logger log = Logger.getLogger(StorageAdapterBasicTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage", Level.INFO);
    }
    
    protected StorageAdapter adapter;
            
    protected StorageAdapterBasicTest(StorageAdapter impl) { 
        this.adapter = impl;
    }
    
    @Before
    public abstract void cleanupBefore() throws Exception;
    
    @Test
    public void testPutGetDelete() {
        URI artifactURI = URI.create("cadc:TEST/testPutGetDelete");
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Random rnd = new Random();
            byte[] data = new byte[1024];
            rnd.nextBytes(data);
            
            NewArtifact newArtifact = new NewArtifact(artifactURI);
            md.update(data);
            URI expectedChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            newArtifact.contentChecksum = expectedChecksum;
            newArtifact.contentLength = (long) data.length;
            log.debug("testPutGetDelete random data: " + data.length + " " + expectedChecksum);
            
            log.debug("testPutGetDelete put: " + artifactURI);
            StorageMetadata storageMetadata = adapter.put(newArtifact, new ByteArrayInputStream(data));
            log.info("testPutGetDelete put: " + artifactURI + " to " + storageMetadata.getStorageLocation());
            log.info("put: " + storageMetadata.getStorageLocation());
            Assert.assertNotNull(storageMetadata);
            Assert.assertNotNull(storageMetadata.getStorageLocation());
            Assert.assertEquals(newArtifact.contentChecksum, storageMetadata.getContentChecksum());
            Assert.assertEquals(newArtifact.contentLength, storageMetadata.getContentLength());
            Assert.assertEquals("artifactURI",  artifactURI, storageMetadata.artifactURI);
            Assert.assertNotNull(storageMetadata.contentLastModified);
            
            // verify data stored
            log.debug("testPutGetDelete get: " + artifactURI);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(storageMetadata.getStorageLocation(), bos);
            md.reset();
            byte[] actual = bos.toByteArray();
            md.update(actual);
            URI actualChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("testPutGetDelete get: " + artifactURI + " " + actual.length + " " + actualChecksum);
            Assert.assertEquals("length", (long) newArtifact.contentLength, actual.length);
            Assert.assertEquals("checksum", newArtifact.contentChecksum, actualChecksum);

            adapter.delete(storageMetadata.getStorageLocation());
            try {
                adapter.get(storageMetadata.getStorageLocation(), new ByteArrayOutputStream());
                Assert.fail("Should have received resource not found exception");
            } catch (ResourceNotFoundException expected) {
                log.info("caught expected: " + expected);
            }
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testPutGetDeleteMinimal() {
        URI artifactURI = URI.create("cadc:TEST/testPutGetDeleteMinimal");
        
        try {
            Random rnd = new Random();
            byte[] data = new byte[1024];
            rnd.nextBytes(data);
            
            final NewArtifact na = new NewArtifact(artifactURI);
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(data);
            URI expectedChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            
            // test that we can store raw input stream data with no optional metadata
            
            log.debug("testPutGetDeleteMinimal random data: " + data.length + " " + expectedChecksum);
            
            log.debug("testPutGetDeleteMinimal put: " + artifactURI);
            StorageMetadata storageMetadata = adapter.put(na, new ByteArrayInputStream(data));
            log.info("testPutGetDeleteMinimal put: " + artifactURI + " to " + storageMetadata.getStorageLocation());
            
            // verify data
            log.debug("testPutGetDeleteMinimal get: " + artifactURI);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(storageMetadata.getStorageLocation(), bos);
            byte[] actual = bos.toByteArray();
            md.update(actual);
            URI actualChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("testPutGetDeleteMinimal get: " + artifactURI + " " + actual.length + " " + actualChecksum);
            Assert.assertEquals("length", data.length, actual.length);
            Assert.assertEquals("checksum", expectedChecksum, actualChecksum);
            
            adapter.delete(storageMetadata.getStorageLocation());
            try {
                adapter.get(storageMetadata.getStorageLocation(), new ByteArrayOutputStream());
                Assert.fail("Should have received resource not found exception");
            } catch (ResourceNotFoundException expected) {
                log.info("caught expected: " + expected);
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPutReadFail() {
        try {
            URI artifactURI = URI.create("cadc:TEST/testPutReadFail");
            NewArtifact na = new NewArtifact(artifactURI);
            adapter.put(na, TestUtil.getInputStreamThatFails(true)); // provoke IOException
            Assert.fail("expected ReadException: call succeeded");
        } catch (ReadException expected) {
            log.info("caught expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetWriteFail() {
        try {
            URI artifactURI = URI.create("cadc:TEST/testPutReadFail");
            NewArtifact na = new NewArtifact(artifactURI);
            StorageMetadata sm = adapter.put(na, TestUtil.getInputStreamOfRandomBytes(8192));
            Assert.assertNotNull(sm);
            
            adapter.get(sm.getStorageLocation(), TestUtil.getOutputStreamThatFails(true)); // provoke IOException
            Assert.fail("expected WriteException: call succeeded");
        } catch (WriteException expected) {
            log.info("caught expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPutRejected() {
        try {
            String dataString = "abcdefghijklmnopqrstuvwxyz";
            byte[] data = dataString.getBytes();
            
            URI artifactURI = URI.create("test:path/file");
            MessageDigest md = MessageDigest.getInstance("MD5");
            String md5Val = HexUtil.toHex(md.digest(data));
            NewArtifact newArtifact = new NewArtifact(artifactURI);
            
            final URI contentChecksum = URI.create("md5:" + md5Val);
            final Long contentLength = (long) data.length;
            
            final URI wrongChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"); // md5 of zero bytes
            
            
            try {
                newArtifact.contentLength = data.length - 1L;
                ByteArrayInputStream bis = new ByteArrayInputStream(data);
                StorageMetadata storageMetadata = adapter.put(newArtifact, bis);
                Assert.fail("expected fail - got : " + storageMetadata);
            } catch (PreconditionFailedException expected) {
                log.info("caught expected: " + expected);
            }
            newArtifact.contentLength = null;
            
            try {
                newArtifact.contentLength = data.length + 1L;
                ByteArrayInputStream bis = new ByteArrayInputStream(data);
                StorageMetadata storageMetadata = adapter.put(newArtifact, bis);
                Assert.fail("expected fail - got : " + storageMetadata);
            } catch (PreconditionFailedException expected) {
                log.info("caught expected: " + expected);
            }
            newArtifact.contentLength = null;
            
            try {
                newArtifact.contentChecksum = wrongChecksum;
                ByteArrayInputStream bis = new ByteArrayInputStream(data);
                StorageMetadata storageMetadata = adapter.put(newArtifact, bis);
                Assert.fail("expected fail - got : " + storageMetadata);
            } catch (PreconditionFailedException expected) {
                log.info("caught expected: " + expected);
            }
            newArtifact.contentChecksum = null;
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testIterator() {
        int iterNum = 13;
        long datalen = 8192L;
        try {
            SortedSet<StorageMetadata> expected = new TreeSet<>();
            for (int i = 0; i < iterNum; i++) {
                URI artifactURI = URI.create("cadc:TEST/testIterator-" + i);
                NewArtifact na = new NewArtifact(artifactURI);
                na.contentLength = (long) datalen;
                StorageMetadata sm = adapter.put(na, TestUtil.getInputStreamOfRandomBytes(datalen));
                Assert.assertNotNull(sm.artifactURI);
                //Assert.assertNotNull(sm.contentLastModified);
                log.debug("testIterator put: " + artifactURI + " to " + sm.getStorageLocation());
                expected.add(sm);
            }
            // put + delete could leave empty buckets behind: should be harmless
            for (int i = iterNum; i < 2 * iterNum; i++) {
                String suri = "test:FOO/bar" + i;
                URI uri = URI.create(suri);
                NewArtifact na = new NewArtifact(uri);
                na.contentLength = (long) datalen;
                StorageMetadata meta = adapter.put(na, TestUtil.getInputStreamOfRandomBytes(datalen));
                adapter.delete(meta.getStorageLocation());
                log.info("extra storageBucket: " + meta.getStorageLocation().storageBucket);
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
                
                Assert.assertNotNull("artifactURI", am.artifactURI);
                Assert.assertEquals("artifactURI", em.artifactURI, am.artifactURI);
                
                Assert.assertNotNull("contentLastModified", am.contentLastModified);
                //Assert.assertEquals("contentLastModified", em.contentLastModified, am.contentLastModified);
            }
            
            // rely on cleanup()
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testIteratorBucketPrefix() {
        int iterNum = 13;
        long datalen = 8192L;
        try {
            SortedSet<StorageMetadata> expected = new TreeSet<>();
            for (int i = 0; i < iterNum; i++) {
                URI artifactURI = URI.create("cadc:TEST/testIteratorBucketPrefix-" + i);
                NewArtifact na = new NewArtifact(artifactURI);
                na.contentLength = (long) datalen;
                StorageMetadata sm = adapter.put(na,  TestUtil.getInputStreamOfRandomBytes(datalen));
                log.debug("testList put: " + artifactURI + " to " + sm.getStorageLocation());
                expected.add(sm);
            }
            // put + delete could leave empty buckets behind: should be harmless
            for (int i = iterNum; i < 2 * iterNum; i++) {
                String suri = "test:FOO/bar" + i;
                URI uri = URI.create(suri);
                NewArtifact na = new NewArtifact(uri);
                na.contentLength = (long) datalen;
                StorageMetadata meta = adapter.put(na, TestUtil.getInputStreamOfRandomBytes(datalen));
                adapter.delete(meta.getStorageLocation());
                log.info("extra storageBucket: " + meta.getStorageLocation().storageBucket);
            }
            log.info("testIteratorBucketPrefix created: " + expected.size());
            
            int found = 0;
            for (byte b = 0; b < 16; b++) {
                String bpre = HexUtil.toHex(b).substring(1);
                log.debug("bucket prefix: " + bpre);
                Iterator<StorageMetadata> i = adapter.iterator(bpre);
                while (i.hasNext()) {
                    StorageMetadata sm = i.next();
                    Assert.assertTrue("prefix match", sm.getStorageLocation().storageBucket.startsWith(bpre));
                    found++;
                }
            }
            Assert.assertEquals("found with bucketPrefix", expected.size(), found);
            
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
}

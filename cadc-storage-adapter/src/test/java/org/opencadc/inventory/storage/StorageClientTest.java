/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2019.                            (c) 2019.
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

package org.opencadc.inventory.storage;

import ca.nrc.cadc.util.Log4jInit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;

/**
 * @author majorb
 *
 */
public class StorageClientTest {
    
    private static final Logger log = Logger.getLogger(StorageClientTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.DEBUG);
    }
    
    @Test
    public void testPutGet() {
        try {
            System.setProperty(StorageClient.STORAGE_ADPATER_CLASS_PROPERTY, TestStorageAdapter.class.getName());
            TestStorageAdapter.mode = TestStorageAdapter.Mode.NORMAL;
            // small buffer so iterator has a few loops
            StorageClient client = new StorageClient(TestStorageAdapter.BUF_SIZE, 3);
            
            URI artifactURI = URI.create("cadc:test/path");
            Artifact artifact = new Artifact(artifactURI,
                TestStorageAdapter.contentChecksum, new Date(), TestStorageAdapter.contentLength);
            ByteArrayInputStream in = new ByteArrayInputStream(TestStorageAdapter.data);
            log.info("sending data: " + TestStorageAdapter.dataString);
            StorageMetadata putMetadata = client.put(artifact, in);
            Assert.assertEquals("artifactURI", artifactURI, putMetadata.artifactURI);
            Assert.assertEquals("contentChecksum", TestStorageAdapter.contentChecksum, putMetadata.getContentChecksum());
            Assert.assertEquals("contentLength", TestStorageAdapter.contentLength, putMetadata.getContentLength());
            URI storageID = putMetadata.getStorageLocation().getStorageID();
            
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            StorageLocation storageLocation = new StorageLocation(storageID);
            client.get(storageLocation, out);
            Assert.assertEquals("data", new String(TestStorageAdapter.data), new String(out.toByteArray()));
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(StorageClient.STORAGE_ADPATER_CLASS_PROPERTY);
        }
    }
    
    @Test
    public void testInputStreamError() {
        
        try {
            
            TestStorageAdapter.Mode[] modes = new TestStorageAdapter.Mode[] {
                TestStorageAdapter.Mode.ERROR_ON_GET_0,
                TestStorageAdapter.Mode.ERROR_ON_GET_1,
                TestStorageAdapter.Mode.ERROR_ON_GET_2,
            };
            
            for (TestStorageAdapter.Mode mode : modes) {
                
                log.info("Testing input stream error mode: " + mode);
            
                System.setProperty(StorageClient.STORAGE_ADPATER_CLASS_PROPERTY, TestStorageAdapter.class.getName());
                TestStorageAdapter.mode = mode;
                StorageClient client = new StorageClient(TestStorageAdapter.BUF_SIZE, 3);
                
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try {
                    StorageLocation storageLocation = new StorageLocation(TestStorageAdapter.storageID);
                    client.get(storageLocation, out);
                    Assert.fail("Should have received exception on get in mode: " + mode);
                } catch (Exception e) {
                    // expected
                    Assert.assertTrue("error msg", e.getMessage().contains("failed reading from input stream"));
                }
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(StorageClient.STORAGE_ADPATER_CLASS_PROPERTY);
        }
    }
    
    @Test
    public void testOutputStreamError() {
        
        try {
            
            int[] failPoints = new int[] {
                0,
                1,
                2
            };
            
            for (int failPoint : failPoints) {
            
                log.info("Testing output stream error with fail on read number " + failPoint);
                
                System.setProperty(StorageClient.STORAGE_ADPATER_CLASS_PROPERTY, TestStorageAdapter.class.getName());
                TestStorageAdapter.mode = TestStorageAdapter.Mode.NORMAL;
                StorageClient client = new StorageClient(TestStorageAdapter.BUF_SIZE, 3);
                
                ByteArrayOutputStream out = new ErrorOutputStream(failPoint);
                try {
                    StorageLocation storageLocation = new StorageLocation(TestStorageAdapter.storageID);
                    client.get(storageLocation, out);
                    Assert.fail("Should have received exception on get");
                } catch (Exception e) {
                    // expected
                    Assert.assertTrue("error msg", e.getMessage().contains("failed writing to output stream"));
                }
                
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(StorageClient.STORAGE_ADPATER_CLASS_PROPERTY);
        }
    }
    
    private class ErrorOutputStream extends ByteArrayOutputStream {
        
        int failPoint;
        int count = 0;
        
        ErrorOutputStream(int failPoint) {
            this.failPoint = failPoint;
        }
        
        @Override
        public void write(byte[] buf, int off, int len) {
            if (failPoint == count) {
                throw new RuntimeException("test exception");
            }
            count++;
            super.write(buf, off, len);
        }
    }

}

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

package org.opencadc.inventory.storage.fs;

import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * @author majorb
 *
 */
public class FileSystemStorageAdapterTest {
    
    private static final Logger log = Logger.getLogger(FileSystemStorageAdapterTest.class);
    
    private static final String TEST_ROOT = "build/tmp/fsroot";
    
    private static final String dataString = "abcdefghijklmnopqrstuvwxyz";
    private static final byte[] data = dataString.getBytes();

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.DEBUG);
    }
    
    @BeforeClass
    public static void setup() {
        try {
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrw-");
            FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
            Files.createDirectories(Paths.get(TEST_ROOT), attr);
        } catch (Throwable t) {
            log.error("setup error", t);
        }
    }
    
    @Test
    public void testPutGetDelete() {
        try {
            
            URI uri = URI.create("test:path/file");
            MessageDigest md = MessageDigest.getInstance("MD5");
            String md5Val = HexUtil.toHex(md.digest(data));
            URI checksum = URI.create("md5:" + md5Val);
            log.info("expected md5sum: " + checksum);
            Date lastModified = new Date();
            long length = data.length;
            Artifact artifact = new Artifact(uri, checksum, lastModified, length);
            
            OutputStreamWrapper outWrapper = new OutputStreamWrapper() {
                public void write(OutputStream out) throws IOException {
                    out.write(data);
                }
            };
            
            FileSystemStorageAdapter fs = new FileSystemStorageAdapter(TEST_ROOT);
            StorageMetadata storageMetadata = fs.put(artifact, outWrapper, null);
            
            TestInputWrapper inWrapper = new TestInputWrapper();
            fs.get(storageMetadata.getStorageLocation().getStorageID(), inWrapper);
            
            String resultData = new String(inWrapper.data);
            log.info("result data: " + resultData);
            Assert.assertEquals("data", dataString, resultData);
            
            fs.delete(storageMetadata.getStorageLocation().getStorageID());
            
            try {
                fs.get(storageMetadata.getStorageLocation().getStorageID(), inWrapper);
                Assert.fail("Should have received resource not found exception");
            } catch (ResourceNotFoundException e) {
                // expected
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testIterator() {
        try {
            
            FileSystemStorageAdapter fs = new FileSystemStorageAdapter(TEST_ROOT);
            
            MessageDigest md = MessageDigest.getInstance("MD5");
            String md5Val = HexUtil.toHex(md.digest(data));
            URI checksum = URI.create("md5:" + md5Val);
            log.info("expected md5sum: " + checksum);
            Date lastModified = new Date();
            long length = data.length;
            
            String[] files = new String[] {
                "test:dir1/file1",
                "test:dir1/file2",
                "test:dir1/dir2/file3",
                "test:dir1/dir2/file4",
                "test:dir1/file5",
                "test:dir1/dir3/dir4/file6",
                "test:dir1/dir3/dir4/file7",
                "test:dir1/dir3/file8",
                "test:dir1/file9",
                "test:dir1/dir4/file10",
                "test:dir5/file11",
                "test:dir5/dir6/dir7/dir8/file12",
            };
            
            List<URI> storageIDs = new ArrayList<URI>();
            
            for (String file : files) {
                URI uri = URI.create(file);
                Artifact artifact = new Artifact(uri, checksum, lastModified, length);
                OutputStreamWrapper outWrapper = new OutputStreamWrapper() {
                    public void write(OutputStream out) throws IOException {
                        out.write(data);
                    }
                };

                StorageMetadata meta = fs.put(artifact, outWrapper, null);
                storageIDs.add(meta.getStorageLocation().getStorageID());
                log.debug("added " + meta.getStorageLocation().getStorageID());
            }
            
            Iterator<StorageMetadata> iterator = fs.iterator();
            StorageMetadata next = null;
            int count = 0;
            while (iterator.hasNext()) {
                next = iterator.next();
                if (!storageIDs.contains(next.getStorageLocation().getStorageID())) {
                    Assert.fail("encounted unknown file: " + next.getStorageLocation().getStorageID());
                }
                Assert.assertEquals("checksum", checksum, next.getContentChecksum());
                Assert.assertEquals("length", new Long(length), next.getContentLength());
                count++;
            }
            
            Assert.assertEquals("file count", storageIDs.size(), count);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    private class TestInputWrapper implements InputStreamWrapper {

        byte[] data;
        
        public void read(InputStream inputStream) throws IOException {
            byte[] buff = new byte[100];
            int bytesRead = inputStream.read(buff);
            data = new byte[bytesRead];
            System.arraycopy(buff, 0, data, 0, bytesRead);
        }
        
    }

}

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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.InvalidConfigException;
import org.opencadc.inventory.storage.StorageEngageException;

/**
 *
 * @author pdowler
 */
public class SwiftStorageAdapterTest {
    private static final Logger log = Logger.getLogger(SwiftStorageAdapterTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
    }
    
    private static final long GB = 1024L * 1024L * 1024L;
    private static final long HALF_GB = 512L * 1024L * 1024L;
    
    public SwiftStorageAdapterTest() { 
        
    }
    
    @Test
    public void testCalcMinSegmentSize() throws Exception {
        SwiftStorageAdapter ssa = new SwiftStorageAdapter("test-bucket", 0, false);
        
        // small object
        long contentLength = HALF_GB;
        long min = ssa.calcMinSegmentSize(contentLength);
        int num = 0;
        long rem = contentLength;
        while (rem >= min) {
            rem -= min;
            num++;
        }
        if (rem > 0) {
            num++;
        }
        log.info("contentLength " + contentLength + " min " + min + " last " + rem);
        Assert.assertEquals("small-object min", contentLength, min);
        Assert.assertEquals("small-object num", 1, num);
        Assert.assertEquals("small-object rem", 0, rem);
        
        // largest simple object
        contentLength = 5 * GB;
        min = ssa.calcMinSegmentSize(contentLength);
        rem = contentLength;
        num = 0;
        while (rem >= min) {
            rem -= min;
            num++;
        }
        if (rem > 0) {
            num++;
        }
        log.info("contentLength " + contentLength + " min " + min + " last " + rem);
        Assert.assertEquals("object-5G min", contentLength, min);
        Assert.assertEquals("object-5G num", 1, num);
        Assert.assertEquals("object-5G rem", 0, rem);
        
        // typical segmented object
        contentLength = 12 * GB;
        min = ssa.calcMinSegmentSize(contentLength);
        rem = contentLength;
        num = 0;
        while (rem >= min) {
            rem -= min;
            num++;
        }
        if (rem > 0) {
            num++;
        }
        log.info("contentLength " + contentLength + " min " + min + " last " + rem);
        Assert.assertEquals("object-12G min", 4 * GB, min);
        Assert.assertEquals("object-12G num", 3, num);
        Assert.assertEquals("object-12G rem", 0, rem);
        
        // smallest segmented object, odd size with rem
        contentLength = 5 * GB + 1;
        min = ssa.calcMinSegmentSize(contentLength);
        rem = contentLength;
        num = 0;
        while (rem >= min) {
            rem -= min;
            num++;
        }
        if (rem > 0) {
            num++;
        }
        log.info("contentLength " + contentLength + " min " + min + " last " + rem);
        Assert.assertEquals("object 5G+1 num", 2, num);
        Assert.assertEquals("object 5G+1 min", (2 * GB + HALF_GB + 1), min);
        Assert.assertEquals("object 5G+1 rem", (2 * GB + HALF_GB), rem);
        
        // first prime after 5GiB
        contentLength = 5368709131L;
        min = ssa.calcMinSegmentSize(contentLength);
        rem = contentLength;
        num = 0;
        while (rem >= min) {
            rem -= min;
            num++;
        }
        if (rem > 0) {
            num++;
        }
        log.info("contentLength " + contentLength + " min " + min + " last " + rem);
        Assert.assertEquals("object >5G prime num", 2, num);
        Assert.assertEquals("object >5G prime min+rem", contentLength, (min + rem));
    }
    
    @Test
    public void testGetNextPartName() throws Exception {
        SwiftStorageAdapter swiftAdapter = new SwiftStorageAdapter("test-bucket", 0, false);
        
        String prefix = "id:uuid:p:";
        
        String e1 = prefix + "0001";
        String a1 = swiftAdapter.getNextPartName(prefix, null);
        log.info(null + " -> " + a1);
        Assert.assertEquals(e1, a1);
        
        String e2 = prefix + "0002";
        String a2 = swiftAdapter.getNextPartName(prefix, a1);
        log.info(a1 + " -> " + a2);
        Assert.assertEquals(e2, a2);
        
        try {
            String a3 = prefix + "9999";
            String a4 = swiftAdapter.getNextPartName(prefix, a3);
            Assert.fail("expected IllegalArgumentException, got: " + a4);
        } catch (IllegalArgumentException ex) {
            log.info("caught expected: " + ex);
        }
    }
    
    @Test
    public void testGenerateID() throws Exception {
        SwiftStorageAdapter swiftAdapter = new SwiftStorageAdapter("test-bucket", 0, false);
        // enough to verify that randomness in the scheme doesn't create invalid URI?
        for (int i = 0; i < 20; i++) {
            StorageLocation loc = swiftAdapter.generateStorageLocation();
            log.info("testGenerateID (single-bucket): " + loc);
        }
        
        swiftAdapter = new SwiftStorageAdapter("test-bucket", 1, true);
        // enough to verify that randomness in the scheme doesn't create invalid URI?
        for (int i = 0; i < 20; i++) {
            StorageLocation loc = swiftAdapter.generateStorageLocation();
            log.info("testGenerateID (multi-bucket): " + loc);
        }
    }
    
    @Test
    public void testBucketeeringSingle() throws Exception {
        SwiftStorageAdapter swiftAdapter = new SwiftStorageAdapter("test-bucket", 0, false);
        StorageLocation loc = swiftAdapter.generateStorageLocation();
        log.info("testBucketeering created: " + loc);
        
        SwiftStorageAdapter.InternalBucket ibucket = swiftAdapter.toInternalBucket(loc);
        log.info("internal: " + ibucket);
        StorageLocation actual = swiftAdapter.toExternal(ibucket, loc.getStorageID().toASCIIString());
        log.info("single-bucket: " + loc + " -> " + ibucket + " -> " + actual);
        Assert.assertEquals(loc, actual);
    }
    
    @Test
    public void testBucketeeringMulti() throws Exception {
        SwiftStorageAdapter swiftAdapter = new SwiftStorageAdapter("test-bucket", 3, true);
        StorageLocation loc = swiftAdapter.generateStorageLocation();
        log.info("testBucketeering created: " + loc);
        
        SwiftStorageAdapter.InternalBucket ibucket = swiftAdapter.toInternalBucket(loc);
        log.info("internal: " + ibucket);
        StorageLocation actual = swiftAdapter.toExternal(ibucket, loc.getStorageID().toASCIIString());
        log.info("single-bucket: " + loc + " -> " + ibucket + " -> " + actual);
        Assert.assertEquals(loc, actual);
    }
    
    @Test
    public void testConfigParsing() throws Exception {
        List<String[]> config = new ArrayList<>();
        // verify whitespace handling on first three by added extra whitespace
        config.add(new String[] { SwiftStorageAdapter.CONF_BUCKET, " foo " });
        config.add(new String[] { SwiftStorageAdapter.CONF_SBLEN, " 3 " });
        config.add(new String[] { SwiftStorageAdapter.CONF_ENABLE_MULTI, " true " });
        config.add(new String[] { SwiftStorageAdapter.CONF_ENDPOINT, "https://example.net/v1/auth" });
        config.add(new String[] { SwiftStorageAdapter.CONF_USER, "somebody" });
        config.add(new String[] { SwiftStorageAdapter.CONF_KEY, "sombody-has-a-key" });
        
        String userHome = System.getProperty("user.home");
        try {
            
            File testHome = new File("build/tmp/test-home");
            if (!testHome.exists()) {
                testHome.mkdirs();
            }
            System.setProperty("user.home", testHome.getAbsolutePath());
            
            File testConfig = new File(testHome, "config");
            
            // missing config dir
            if (testConfig.exists()) {
                recursiveDelete(testConfig);
            }
            
            try {
                SwiftStorageAdapter swiftAdapter = new SwiftStorageAdapter();
                Assert.fail("expected InvalidConfigException - created " + swiftAdapter);
            } catch (InvalidConfigException expected) {
                log.info("missing config dir: " + expected);
            }
            
            testConfig.mkdir();
            
            // missing config file
            
            try {
                SwiftStorageAdapter swiftAdapter = new SwiftStorageAdapter();
                Assert.fail("expected InvalidConfigException - created " + swiftAdapter);
            } catch (InvalidConfigException expected) {
                log.info("missing config file: " + expected);
            }
            
            File cf = new File(testConfig, SwiftStorageAdapter.CONFIG_FILENAME);
            
            // missing config items
            for (int i = 0; i < config.size() - 1; i++) {
                writeProps(i, config, cf);
                try {
                    SwiftStorageAdapter sa = new SwiftStorageAdapter();
                    Assert.fail("expected InvalidConfigException - created " + sa);
                } catch (InvalidConfigException expected) {
                    log.info("missing config items: " + expected);
                }
                cf.delete();
            }

            // valid config to verify test
            writeProps(6, config, cf);
            try {
                SwiftStorageAdapter swiftAdapter = new SwiftStorageAdapter();
                Assert.assertEquals("storageBucket", swiftAdapter.storageBucket, "foo");
                Assert.assertEquals("bucketLength", swiftAdapter.storageBucketLength, 3);
                Assert.assertTrue("multiBucket", swiftAdapter.multiBucket);
            } catch (InvalidConfigException ex) {
                log.error("valid config failed", ex);
                Assert.fail("syntactically valid config failed: " + ex);
            } catch (StorageEngageException expected) {
                log.info("bogus connection info: " + expected);
            }
            //cf.delete();
            
            
        } finally {
            System.setProperty("user.home", userHome);
        }
    }
    
    private void recursiveDelete(File dir) {
        if (dir.exists()) {
            if (dir.isDirectory()) {
                File[] files = dir.listFiles();
                for (File f : files) {
                    recursiveDelete(f);
                }
            }
            dir.delete();
        }
    }
    
    private void writeProps(int num, List<String[]> props, File f) throws IOException {
        PrintWriter w = new PrintWriter(f);
        for (int i = 0; i < num; i++) {
            String[] ci = props.get(i);
            w.println(ci[0] + "=" + ci[1]);
        }
        w.flush();
        w.close();
    }
}

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

import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.util.Log4jInit;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.test.StorageAdapterBasicTest;
import org.opencadc.inventory.storage.test.TestUtil;

/**
 *
 * @author pdowler
 */
public class SingleBucketSwiftStorageAdapterTest extends StorageAdapterBasicTest {
    private static final Logger log = Logger.getLogger(SingleBucketSwiftStorageAdapterTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.javaswift.joss.client", Level.INFO);
    }
    
    final SwiftStorageAdapter swiftAdapter;
    
    public SingleBucketSwiftStorageAdapterTest() throws Exception {
        super(new SwiftStorageAdapter(true, System.getProperty("user.name") + "-SingleBucketSwiftStorageAdapterTest", 3, false));
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
        log.info("cleanupBefore: DONE");        
    }
    
    @Test
    public void testCleanupOnly() {
        log.info("testCleanupOnly: no-op");
    }
    
    @Test
    public void testPutLargeStreamReject() {
        URI artifactURI = URI.create("cadc:TEST/testPutLargeStreamReject");
        
        final NewArtifact na = new NewArtifact(artifactURI);
        
        // ceph limit of 5GiB
        long numBytes = (long) 6 * 1024 * 1024 * 1024; 
        na.contentLength = numBytes;
            
        try {
            InputStream istream = TestUtil.getInputStreamThatFails();
            log.info("testPutCheckDeleteLargeStreamReject put: " + artifactURI + " " + numBytes);
            StorageMetadata sm = swiftAdapter.put(na, istream);
            Assert.fail("expected ByteLimitExceededException, got: " + sm);
        } catch (ByteLimitExceededException expected) {
            log.info("caught: " + expected);
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    // normally disabled because this has to actually upload ~5GiB of garbage before it fails
    //@Test
    public void testPutLargeStreamFail() {
        URI artifactURI = URI.create("cadc:TEST/testPutLargeStreamFail");
        
        final NewArtifact na = new NewArtifact(artifactURI);
        
        // ceph limit of 5GiB
        long numBytes = (long) 6 * 1024 * 1024 * 1024; 
            
        try {
            InputStream istream = TestUtil.getInputStreamOfRandomBytes(numBytes);
            log.info("testPutCheckDeleteLargeStreamFail put: " + artifactURI + " " + numBytes);
            StorageMetadata sm = swiftAdapter.put(na, istream);
            
            Assert.assertFalse("put should have failed, but object exists", swiftAdapter.exists(sm.getStorageLocation()));
            
            Assert.fail("expected ByteLimitExceededException, got: " + sm);
        } catch (ByteLimitExceededException expected) {
            log.info("caught: " + expected);
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
}

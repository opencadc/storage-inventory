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

package org.opencadc.tantar;

import ca.nrc.cadc.io.ResourceIterator;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.tantar.policy.InventoryIsAlwaysRight;

/**
 * InventoryIsAlwaysRight integration test.
 * 
 * @author pdowler
 */
public class InventoryIsAlwaysRightTest extends TantarTest {
    private static final Logger log = Logger.getLogger(InventoryIsAlwaysRightTest.class);

    private static TestPolicy policy = new TestPolicy();
    
    public InventoryIsAlwaysRightTest() throws Exception {
        this(false);
    }
    
    protected InventoryIsAlwaysRightTest(boolean includeRecoverable) throws Exception {
        super(policy, includeRecoverable);
    }
    
    @Before
    public void doCleanup() throws Exception {
        super.cleanupBefore();
    }
    
    @Test
    public void testPolicy() throws Exception {
        doTestSetup(true);
        
        // make sure delayAction doesn't prevent actions
        Thread.sleep(10L);
        policy.resetDelayTimestamp();
        
        validator.setIncludeRecoverable(includeRecoverable);
        validator.validate();
        
        // a2 storagelocation cleared
        // a4->sm4 recovered
        // sm6 deleted
        // a7 storageLocation cleared, sm7 deleted
        // sm8 deleted
        
        // verify a4 was recovered rather than replaced
        Artifact actual = artifactDAO.get(URI.create("test:FOO/a4"));
        Assert.assertNotNull(actual);
        Assert.assertEquals(recoverableA4.getID(), actual.getID());
        Assert.assertNotNull(actual.storageLocation);
        Assert.assertEquals(recoverableSM4, actual.storageLocation);
        
        List<StorageLocation> locs = new ArrayList<>();
        Iterator<StorageMetadata> si = adapter.iterator();
        while (si.hasNext()) {
            StorageMetadata sm = si.next();
            log.info("data: " + sm.getArtifactURI() + " " + sm.getStorageLocation());
            locs.add(sm.getStorageLocation());
        }
        // in the first pass, we have an extra sm4 lying around
        // or: if recovery doesn't successfully recover the stored object
        Assert.assertEquals("locs", 5, locs.size());
        
        validator.validate();
        
        // re-check a4
        actual = artifactDAO.get(URI.create("test:FOO/a4"));
        Assert.assertNotNull(actual);
        Assert.assertEquals(recoverableA4.getID(), actual.getID());
        Assert.assertNotNull(actual.storageLocation);
        Assert.assertEquals(recoverableSM4, actual.storageLocation);
        
        // verify sm4 was recovered
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            adapter.get(recoverableSM4, bos);
        } catch (Exception ex) {
            log.error("failed to read recovered storageLocation: " + recoverableSM4, ex);
            Assert.fail("failed to read recovered storageLocation: " + recoverableSM4 + " cause: " + ex);
        }
        
        locs.clear();
        si = adapter.iterator();
        while (si.hasNext()) {
            StorageMetadata sm = si.next();
            log.info("data: " + sm.getArtifactURI() + " " + sm.getStorageLocation());
            locs.add(sm.getStorageLocation());
        }
        Assert.assertEquals("locs", 4, locs.size());
        
        List<StorageLocation> refs = new ArrayList<>();
        try (final ResourceIterator<Artifact> ai = artifactDAO.storedIterator(null)) {
            while (ai.hasNext()) {
                Artifact a = ai.next();
                log.info("stored: " + a.getURI() + " " + a.storageLocation);
                refs.add(a.storageLocation);
            }
        }
        Assert.assertEquals("refs", 4, refs.size());
        
        Assert.assertTrue("no broken refs", locs.containsAll(refs));
        Assert.assertTrue("no orphaned files", refs.containsAll(locs));
        
        List<URI> unstored = new ArrayList<>();
        try (final ResourceIterator<Artifact> ai = artifactDAO.unstoredIterator(null)) {
            while (ai.hasNext()) {
                Artifact a = ai.next();
                log.info("unstored: " + a.getURI() + " " + a.storageLocation);
                unstored.add(a.getURI());
            }
        }
        
        Assert.assertEquals("unstored", 3, unstored.size());
        Assert.assertTrue(unstored.contains(URI.create("test:FOO/a2")));
        Assert.assertTrue(unstored.contains(URI.create("test:FOO/a7")));
        Assert.assertTrue(unstored.contains(URI.create("test:FOO/a8")));
    }
    
    private static class TestPolicy extends InventoryIsAlwaysRight {
        @Override
        public void resetDelayTimestamp() {
            super.resetDelayTimestamp();
        }
        
    }
}

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

package org.opencadc.fenwick;

import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageLocationEvent;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.HarvestState;

/**
 *
 * @author pdowler
 */
public class StorageLocationEventSyncTest {
    private static final Logger log = Logger.getLogger(StorageLocationEventSyncTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.fenwick", Level.INFO);
    }

    private final InventoryEnvironment inventoryEnvironment = new InventoryEnvironment();
    private final LuskanEnvironment luskanEnvironment = new LuskanEnvironment();
    private final Subject testUser = TestUtil.getConfiguredSubject();
    
    public StorageLocationEventSyncTest() throws Exception { 
    }
    
    @Before
    public void beforeTest() throws Exception {
        inventoryEnvironment.cleanTestEnvironment();
        luskanEnvironment.cleanTestEnvironment();
    }
    
    @Test
    public void testGetEventStream() {
        try {
            log.info("testGetEventStream");
            
            Subject.doAs(testUser, new PrivilegedExceptionAction<Object>() {

                public Object run() throws Exception {

                    StorageSite site1 = new StorageSite(URI.create("cadc:TEST/siteone"), "Test Site", true, false);
                    final StorageLocationEventSync sync = new StorageLocationEventSync(
                        inventoryEnvironment.artifactDAO, TestUtil.LUSKAN_URI, 6, 6, site1);
                    
                    Calendar now = Calendar.getInstance();
                    now.add(Calendar.DAY_OF_MONTH, -1);
                    Date startTime = now.getTime();
                    
                    // query with no results
                    ResourceIterator<StorageLocationEvent> emptyIterator = sync.getEventStream(startTime, null);
                    Assert.assertNotNull(emptyIterator);
                    Assert.assertFalse(emptyIterator.hasNext());

                    StorageLocationEvent expected1 = new StorageLocationEvent(UUID.randomUUID());
                    StorageLocationEvent expected2 = new StorageLocationEvent(UUID.randomUUID());

                    luskanEnvironment.storageLocationEventDAO.put(expected1);
                    luskanEnvironment.storageLocationEventDAO.put(expected2);

                    // query with multiple results
                    ResourceIterator<StorageLocationEvent> iterator = sync.getEventStream(startTime, null);
                    Assert.assertNotNull(iterator);

                    Assert.assertTrue(iterator.hasNext());
                    StorageLocationEvent actual1 = iterator.next();
                    Date lastModified1 = actual1.getLastModified();

                    Assert.assertTrue(iterator.hasNext());
                    StorageLocationEvent actual2 = iterator.next();
                    Date lastModified2 = actual2.getLastModified();

                    // chronological order
                    Assert.assertTrue(lastModified1.before(lastModified2));
                    Assert.assertFalse(iterator.hasNext());

                    return null;
                }
            });
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testEventApplied() {
        try {
            URI md5 = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
            long len = 1024L;

            StorageSite site1 = new StorageSite(URI.create("cadc:TEST/siteone"), "Test Site", true, false);
            StorageSite site2 = new StorageSite(URI.create("cadc:TEST/sitetwo"), "Test Site", true, false);
            
            SiteLocation sloc1 = new SiteLocation(site1.getID());
            SiteLocation sloc2 = new SiteLocation(site2.getID());
            
            // this is a site -> global sync
            
            // insert 3 artifacts in local inventory
            Artifact a1 = new Artifact(URI.create("cadc:TEST/one"), md5, new Date(), len);
            a1.siteLocations.add(sloc1);
            a1.siteLocations.add(sloc2);
            inventoryEnvironment.artifactDAO.put(a1);
            
            Thread.sleep(10L);

            Artifact a2 = new Artifact(URI.create("cadc:TEST/two"), md5, new Date(), len);
            a2.siteLocations.add(sloc2);
            inventoryEnvironment.artifactDAO.put(a2);
            
            Thread.sleep(10L);
            
            Artifact a3 = new Artifact(URI.create("cadc:TEST/three"), md5, new Date(), len);
            a3.siteLocations.add(sloc2);
            inventoryEnvironment.artifactDAO.put(a3);
            Thread.sleep(10L);
            
            // insert 2 events in luskan
            StorageLocationEvent sle1 = new StorageLocationEvent(a1.getID());
            luskanEnvironment.storageLocationEventDAO.put(sle1);
            Thread.sleep(10L);
            StorageLocationEvent sle2 = new StorageLocationEvent(a2.getID());
            luskanEnvironment.storageLocationEventDAO.put(sle2);
            Thread.sleep(10L);
            StorageLocationEvent sle3 = new StorageLocationEvent(UUID.randomUUID());
            luskanEnvironment.storageLocationEventDAO.put(sle3);
            
            // doit
            StorageLocationEventSync sync = new StorageLocationEventSync(
                    inventoryEnvironment.artifactDAO, TestUtil.LUSKAN_URI, 6, 6, site1);
            Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
                sync.doit();
                return null;
            });

            // verify: 
            // artifact a1 no change because site1 copy already known
            // artifact a2: add site1
            // artifact a3: no change because no event
            Artifact r1 = inventoryEnvironment.artifactDAO.get(a1.getID());
            Assert.assertNotNull(r1);
            Assert.assertEquals(a1.siteLocations.size(), r1.siteLocations.size());
            Assert.assertTrue(r1.siteLocations.contains(sloc1));
            Assert.assertTrue(r1.siteLocations.contains(sloc2));
            Assert.assertEquals(a1.getLastModified(), r1.getLastModified());

            Artifact r2 = inventoryEnvironment.artifactDAO.get(a2.getID());
            Assert.assertNotNull(r2);
            Assert.assertTrue(r2.siteLocations.contains(sloc1));
            Assert.assertTrue(r2.siteLocations.contains(sloc2));
            // lastModified updated only on first copy
            Assert.assertEquals(a2.getLastModified(), r2.getLastModified());

            Artifact r3 = inventoryEnvironment.artifactDAO.get(a3.getID());
            Assert.assertNotNull(r3);
            Assert.assertTrue(r3.siteLocations.contains(sloc2));

            StorageLocationEvent sle;
            sle = inventoryEnvironment.storageLocationEventDAO.get(sle1.getID());
            Assert.assertNull(sle);
            sle = inventoryEnvironment.storageLocationEventDAO.get(sle2.getID());
            Assert.assertNull(sle);
            sle = inventoryEnvironment.storageLocationEventDAO.get(sle3.getID());
            Assert.assertNull(sle);
            
            HarvestState hs = inventoryEnvironment.harvestStateDAO.get(StorageLocationEvent.class.getSimpleName(), TestUtil.LUSKAN_URI);
            Assert.assertNotNull(hs);
            log.info("found: " + hs);
            Assert.assertEquals(sle3.getLastModified(), hs.curLastModified);
            
            // idempotent
            Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
                sync.doit();
                return null;
            });
            
            hs = inventoryEnvironment.harvestStateDAO.get(StorageLocationEvent.class.getSimpleName(), TestUtil.LUSKAN_URI);
            Assert.assertNotNull(hs);
            Assert.assertEquals(sle3.getLastModified(), hs.curLastModified);
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
}

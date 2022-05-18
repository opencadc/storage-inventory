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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.fenwick;

import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.tap.TapClient;
import org.opencadc.tap.TapRowMapper;

public class DeletedStorageLocationEventSyncTest {

    private static final Logger log = Logger.getLogger(DeletedStorageLocationEventSyncTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.fenwick", Level.INFO);
    }

    private final InventoryEnvironment inventoryEnvironment = new InventoryEnvironment();
    private final LuskanEnvironment luskanEnvironment = new LuskanEnvironment();
    private final Subject testUser = TestUtil.getConfiguredSubject();
    
    public DeletedStorageLocationEventSyncTest() throws Exception {
    }

    @Before
    public void beforeTest() throws Exception {
        inventoryEnvironment.cleanTestEnvironment();
        luskanEnvironment.cleanTestEnvironment();
    }

    @Test
    public void testRowMapper() {
        try {
            log.info("testRowMapper");

            UUID uuid = UUID.randomUUID();
            Date lasModified = new Date();
            URI metaChecksum = new URI(TestUtil.ZERO_BYTES_MD5);

            List<Object> row = new ArrayList<>();
            row.add(uuid);
            row.add(lasModified);
            row.add(metaChecksum);

            TapRowMapper<DeletedStorageLocationEvent> mapper =
                new DeletedStorageLocationEventSync.DeletedStorageLocationEventRowMapper();
            DeletedStorageLocationEvent event = mapper.mapRow(row);

            Assert.assertNotNull(event);
            Assert.assertEquals(uuid, event.getID());
            Assert.assertEquals(lasModified, event.getLastModified());
            Assert.assertEquals(metaChecksum, event.getMetaChecksum());
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }

    @Test
    public void testGetEventStream() {
        try {
            log.info("testGetEventStream");
            
            Subject.doAs(testUser, new PrivilegedExceptionAction<Object>() {

                public Object run() throws Exception {

                    StorageSite site1 = new StorageSite(URI.create("cadc:TEST/siteone"), "Test Site", true, false);
                    final DeletedStorageLocationEventSync sync = new DeletedStorageLocationEventSync(
                        inventoryEnvironment.artifactDAO, TestUtil.LUSKAN_URI, 6, 6, site1);
                    
                    Calendar now = Calendar.getInstance();
                    now.add(Calendar.DAY_OF_MONTH, -1);
                    Date startTime = now.getTime();
                    
                    // query with no results
                    ResourceIterator<DeletedStorageLocationEvent> emptyIterator = sync.getEventStream(startTime, null);
                    Assert.assertNotNull(emptyIterator);
                    Assert.assertFalse(emptyIterator.hasNext());

                    DeletedStorageLocationEvent expected1 = new DeletedStorageLocationEvent(UUID.randomUUID());
                    DeletedStorageLocationEvent expected2 = new DeletedStorageLocationEvent(UUID.randomUUID());

                    luskanEnvironment.deletedStorageLocationEventDAO.put(expected1);
                    luskanEnvironment.deletedStorageLocationEventDAO.put(expected2);

                    // query with multiple results
                    ResourceIterator<DeletedStorageLocationEvent> iterator = sync.getEventStream(startTime, null);
                    Assert.assertNotNull(iterator);

                    Assert.assertTrue(iterator.hasNext());
                    DeletedStorageLocationEvent actual1 = iterator.next();
                    Date lastModified1 = actual1.getLastModified();

                    Assert.assertTrue(iterator.hasNext());
                    DeletedStorageLocationEvent actual2 = iterator.next();
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
            
            // this is a site -> global sync
            
            // insert 3 artifacts in local inventory
            Artifact a1 = new Artifact(URI.create("cadc:TEST/one"), md5, new Date(), len);
            a1.siteLocations.add(new SiteLocation(site1.getID()));
            a1.siteLocations.add(new SiteLocation(site2.getID()));
            inventoryEnvironment.artifactDAO.put(a1);
            
            Thread.sleep(10L);

            Artifact a2 = new Artifact(URI.create("cadc:TEST/two"), md5, new Date(), len);
            a2.siteLocations.add(new SiteLocation(site1.getID()));
            inventoryEnvironment.artifactDAO.put(a2);
            
            Thread.sleep(10L);
            
            Artifact a3 = new Artifact(URI.create("cadc:TEST/three"), md5, new Date(), len);
            a3.siteLocations.add(new SiteLocation(site1.getID()));
            inventoryEnvironment.artifactDAO.put(a3);
            Thread.sleep(10L);
            
            // insert 2 dsle in luskan
            DeletedStorageLocationEvent dsle1 = new DeletedStorageLocationEvent(a1.getID());
            luskanEnvironment.deletedStorageLocationEventDAO.put(dsle1);
            DeletedStorageLocationEvent dsle2 = new DeletedStorageLocationEvent(a2.getID());
            luskanEnvironment.deletedStorageLocationEventDAO.put(dsle2);
            Thread.sleep(10L);
            
            // doit
            DeletedStorageLocationEventSync sync = new DeletedStorageLocationEventSync(
                    inventoryEnvironment.artifactDAO, TestUtil.LUSKAN_URI, 6, 6, site1);
            Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
                sync.doit();
                return null;
            });

            // verify: 
            // artifact a1 removed site location
            // artifact a2: removed
            // artifact a3: no change
            // DLSE not stored
            Artifact r1 = inventoryEnvironment.artifactDAO.get(a1.getID());
            Assert.assertNotNull(r1);
            Assert.assertEquals(a1.siteLocations.size() - 1, r1.siteLocations.size());

            Artifact d2 = inventoryEnvironment.artifactDAO.get(a2.getID());
            Assert.assertNull(d2);

            Artifact r3 = inventoryEnvironment.artifactDAO.get(a3.getID());
            Assert.assertNotNull(r3);
            Assert.assertEquals(a3.siteLocations.size(), r3.siteLocations.size());

            DeletedStorageLocationEvent actual = inventoryEnvironment.deletedStorageLocationEventDAO.get(dsle2.getID());
            Assert.assertNull(actual);
            
            HarvestState hs = inventoryEnvironment.harvestStateDAO.get(DeletedStorageLocationEvent.class.getName(), TestUtil.LUSKAN_URI);
            Assert.assertNotNull(hs);
            Assert.assertEquals(dsle2.getLastModified(), hs.curLastModified);
            
            // idempotent
            Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
                sync.doit();
                return null;
            });
            
            hs = inventoryEnvironment.harvestStateDAO.get(DeletedStorageLocationEvent.class.getName(), TestUtil.LUSKAN_URI);
            Assert.assertNotNull(hs);
            Assert.assertEquals(dsle2.getLastModified(), hs.curLastModified);
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
}

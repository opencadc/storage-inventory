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
 *
 ************************************************************************
 */

package org.opencadc.fenwick;

import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.Log4jInit;

import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageSite;
import org.opencadc.tap.TapClient;
import org.springframework.jdbc.core.JdbcTemplate;


public class StorageSiteSyncTest {

    private static final Logger LOGGER = Logger.getLogger(StorageSiteSyncTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.fenwick", Level.DEBUG);
    }

    private final InventoryEnvironment inventoryEnvironment = new InventoryEnvironment();
    private final LuskanEnvironment luskanEnvironment = new LuskanEnvironment();
    private final Subject testUser = TestUtil.getConfiguredSubject();

    public StorageSiteSyncTest() throws Exception {

    }

    @Before
    public void cleanTestEnvironment() throws Exception {
        inventoryEnvironment.cleanTestEnvironment();
        luskanEnvironment.cleanTestEnvironment();
    }

    @Test
    public void intTestSiteSyncOK() throws Exception {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        final TapClient<StorageSite> tapClient = new TapClient<>(TestUtil.LUSKAN_URI);

        final StorageSite storageSite = new StorageSite(URI.create("cadc:TESTSITE/one"),
                                                        StorageSiteSyncTest.class.getSimpleName(), true, false);
        luskanEnvironment.storageSiteDAO.put(storageSite);

        Assert.assertTrue("Sites should be empty.", inventoryEnvironment.storageSiteDAO.list().isEmpty());

        final StorageSiteSync storageSiteSync = new StorageSiteSync(tapClient, inventoryEnvironment.storageSiteDAO);
        Subject.doAs(testUser, (PrivilegedExceptionAction<StorageSite>) storageSiteSync::doit);

        final Set<StorageSite> storageSiteSet = inventoryEnvironment.storageSiteDAO.list();
        Assert.assertEquals("Should be a single site.", 1, storageSiteSet.size());

        final StorageSite[] storageSites = storageSiteSet.toArray(new StorageSite[0]);
        final URI expectedChecksum = storageSites[0].computeMetaChecksum(messageDigest);
        Assert.assertEquals("Wrong site name.", StorageSiteSyncTest.class.getSimpleName(), storageSites[0].getName());
        Assert.assertEquals("Wrong site resource ID.", URI.create("cadc:TESTSITE/one"),
                            storageSites[0].getResourceID());
        Assert.assertEquals("Wrong checksum.", expectedChecksum, storageSites[0].getMetaChecksum());
    }

    @Test
    public void intTestSiteSyncChecksumError() throws Exception {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        final TapClient<StorageSite> tapClient = new TapClient<>(TestUtil.LUSKAN_URI);

        Assert.assertTrue("Sites should be empty.", inventoryEnvironment.storageSiteDAO.list().isEmpty());

        final StorageSite storageSite = new StorageSite(URI.create("cadc:TESTSITE/one_1"),
                                                        StorageSiteSyncTest.class.getSimpleName(), true, false);
        luskanEnvironment.storageSiteDAO.put(storageSite);

        final JdbcTemplate jdbcTemplate = new JdbcTemplate(DBUtil.findJNDIDataSource(luskanEnvironment.jndiPath));
        Assert.assertTrue("Should have updated peacefully.",
                          jdbcTemplate.update("UPDATE " + TestUtil.LUSKAN_SCHEMA + "." + "storageSite "
                                      + "SET metaChecksum = 'md5:d41d8cd98f00b204e9800998ecf8427e' WHERE resourceID = "
                                      + "'cadc:TESTSITE/one_1'") > 0);

        final StorageSiteSync storageSiteSync = new StorageSiteSync(tapClient, inventoryEnvironment.storageSiteDAO);

        try {
            // Should throw an error.
            //
            Subject.doAs(testUser, (PrivilegedExceptionAction<StorageSite>) storageSiteSync::doit);
            Assert.fail("Should throw IllegalStateException.");
        } catch (IllegalStateException e) {
            // Good.
            final URI expectedChecksum = storageSite.computeMetaChecksum(messageDigest);
            Assert.assertEquals("Wrong error message.",
                                "Discovered Storage Site checksum (md5:d41d8cd98f00b204e9800998ecf8427e) "
                                + "does not match computed value (" + expectedChecksum + ").",
                                e.getMessage());
        }
    }

    @Test
    public void intTestSiteSyncMultiple() throws Exception {
        final TapClient<StorageSite> tapClient = new TapClient<>(TestUtil.LUSKAN_URI);
        final StorageSiteSync storageSiteSyncMultipleSitesError =
                new StorageSiteSync(tapClient, inventoryEnvironment.storageSiteDAO);

        final StorageSite storageSite = new StorageSite(URI.create("cadc:TESTSITE/one_1"),
                                                        StorageSiteSyncTest.class.getSimpleName(), true, false);
        luskanEnvironment.storageSiteDAO.put(storageSite);

        final StorageSite secondStorageSite = new StorageSite(URI.create("cadc:TESTSITE/uh-oh"),
                                                              StorageSiteSyncTest.class.getSimpleName() + "_2", true, false);
        luskanEnvironment.storageSiteDAO.put(secondStorageSite);

        try {
            // Should throw an error.
            //
            Subject.doAs(testUser, (PrivilegedExceptionAction<StorageSite>) storageSiteSyncMultipleSitesError::doit);
            Assert.fail("Should throw IllegalStateException.");
        } catch (IllegalStateException e) {
            // Good.
            Assert.assertEquals("Wrong error message.", "More than one Storage Site found.", e.getMessage());
        }
    }

    @Test
    public void intTestSiteSyncNoSitesFound() throws Exception {
        final TapClient<StorageSite> tapClient = new TapClient<>(TestUtil.LUSKAN_URI);

        Assert.assertTrue("Sites should be empty.", inventoryEnvironment.storageSiteDAO.list().isEmpty());

        final StorageSiteSync storageSiteSyncEmptySitesError =
                new StorageSiteSync(tapClient, inventoryEnvironment.storageSiteDAO);

        try {
            // Should throw an error.
            //
            Subject.doAs(testUser, (PrivilegedExceptionAction<StorageSite>) storageSiteSyncEmptySitesError::doit);
            Assert.fail("Should throw IllegalStateException.");
        } catch (IllegalStateException e) {
            // Good.
            Assert.assertEquals("Wrong error message.", "No storage sites available to sync.", e.getMessage());
        }
    }

    /**
     * Intercept the next method to modify the checksum.
     */
    private static class ResourceIteratorModifiedChecksum implements ResourceIterator<StorageSite> {
        final Iterator<StorageSite> sourceIterator;
        public ResourceIteratorModifiedChecksum(final StorageSite storageSite) {
            sourceIterator = Collections.singletonList(storageSite).iterator();
        }

        @Override
        public void close() throws IOException { }

        @Override
        public boolean hasNext() {
            return sourceIterator.hasNext();
        }

        @Override
        public StorageSite next() {
            // Altering the checksum should throw an exception
            final StorageSite storageSite = sourceIterator.next();
            InventoryUtil.assignMetaChecksum(storageSite, URI.create("md5:889900"));
            return storageSite;
        }
    }

    /**
     * Intercept the query and return multiple sites.
     */
    private static class ResourceIteratorMultipleSites implements ResourceIterator<StorageSite> {
        final Iterator<StorageSite> sourceIterator;
        public ResourceIteratorMultipleSites() {
            final List<StorageSite> storageSiteList = new ArrayList<>();

            storageSiteList.add(new StorageSite(URI.create("ivo://test/siteone"), "Test Site One.", true, false));
            storageSiteList.add(new StorageSite(URI.create("ivo://test/sitetwo"), "Test Site Two.", true, true));

            sourceIterator = storageSiteList.iterator();
        }

        @Override
        public void close() throws IOException { }

        @Override
        public boolean hasNext() {
            return sourceIterator.hasNext();
        }

        @Override
        public StorageSite next() {
            return sourceIterator.next();
        }
    }

    /**
     * Intercept the query and return no sites.
     */
    private static class ResourceIteratorEmptySites implements ResourceIterator<StorageSite> {
        final Iterator<StorageSite> sourceIterator = Collections.emptyIterator();
        public ResourceIteratorEmptySites() {
        }

        @Override
        public void close() throws IOException { }

        @Override
        public boolean hasNext() {
            return sourceIterator.hasNext();
        }

        @Override
        public StorageSite next() {
            return sourceIterator.next();
        }
    }
}

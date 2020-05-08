
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

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.AccessControlException;
import java.security.MessageDigest;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.tap.TapClient;

import javax.security.auth.Subject;


public class StorageSiteSyncTest {
    private static final File PROXY_PEM = new File(System.getProperty("user.home") + "/.ssl/cadcproxy.pem");
    private static final Logger LOGGER = Logger.getLogger(StorageSiteSyncTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.fenwick", Level.DEBUG);
    }

    private final StorageSiteDAO storageSiteDAO = new StorageSiteDAO();
    private final ArtifactDAO artifactDAO = new ArtifactDAO();

    public StorageSiteSyncTest() throws Exception {
        final DBConfig dbConfig = new DBConfig();
        final ConnectionConfig cc = dbConfig.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
        DBUtil.createJNDIDataSource("jdbc/StorageSiteSyncTest", cc);

        final Map<String,Object> config = new TreeMap<>();
        config.put(SQLGenerator.class.getName(), SQLGenerator.class);
        config.put("jndiDataSourceName", "jdbc/StorageSiteSyncTest");
        config.put("database", TestUtil.DATABASE);
        config.put("schema", TestUtil.SCHEMA);

        storageSiteDAO.setConfig(config);
        artifactDAO.setConfig(config);
    }

    private void wipe_clean(ResourceIterator<Artifact> artifactIterator) throws Exception {
        while (artifactIterator.hasNext()) {
            Artifact a = artifactIterator.next();
            LOGGER.debug("deleting test uri: " + a.getURI() + " ID: " + a.getID());
            artifactDAO.delete(a.getID());
        }
    }

    @Before
    public void cleanTestEnvironment() throws Exception {
        LOGGER.debug("cleaning stored artifacts...");
        ResourceIterator<Artifact> storedArtifacts = artifactDAO.storedIterator(null);
        wipe_clean(storedArtifacts);
        storedArtifacts.close();

        LOGGER.debug("cleaning unstored artifacts...");
        ResourceIterator<Artifact> unstoredArtifacts = artifactDAO.unstoredIterator(null);
        wipe_clean(unstoredArtifacts);
        unstoredArtifacts.close();

        LOGGER.debug("Wiping Storage Sites.");
        for (final StorageSite storageSite : storageSiteDAO.list()) {
            LOGGER.debug(String.format("Removing storage site '%s'.", storageSite.getName()));
            storageSiteDAO.delete(storageSite.getID());
        }
    }

    /**
     * Test StorageSite synchronization.
     * Test one: Populate the local inventory database with a StorageSite from the remote Luskan.
     * Test two: Ensure the same site is put again without error.
     * Test three: Ensure a failed execution if that same site has a different checksum than its computed one.
     * Test four: Ensure a failed execution if multiple sites are found.
     * Test five: Ensure a failed execution if no sites are found.
     *
     * @throws Exception
     */
    @Test
    public void doSiteSync() throws Exception {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        final Subject subject = SSLUtil.createSubject(PROXY_PEM);
        final TapClient<StorageSite> tapClient = new TapClient<>(URI.create(TestUtil.LUSKAN_URI));

        Assert.assertTrue("Sites should be empty.", storageSiteDAO.list().isEmpty());

        final StorageSiteSync storageSiteSync = new StorageSiteSync(tapClient, storageSiteDAO);
        Subject.doAs(subject, (PrivilegedExceptionAction<StorageSite>) storageSiteSync::doit);

        //
        // Test one
        //
        final Set<StorageSite> storageSiteSet = storageSiteDAO.list();
        Assert.assertEquals("Should be a single site.", 1, storageSiteSet.size());

        final StorageSite[] storageSites = storageSiteSet.toArray(new StorageSite[0]);
        final URI expectedChecksum = storageSites[0].computeMetaChecksum(messageDigest);
        Assert.assertEquals("Wrong site name.", "minoc", storageSites[0].getName());
        Assert.assertEquals("Wrong site resource ID.", URI.create("ivo://cadc.nrc.ca/minoc"),
                            storageSites[0].getResourceID());
        Assert.assertEquals("Wrong checksum.", expectedChecksum, storageSites[0].getMetaChecksum());

        //
        // Test two
        //
        // Running it again should be idempotent
        //
        Subject.doAs(subject, (PrivilegedExceptionAction<StorageSite>) storageSiteSync::doit);

        final Set<StorageSite> storageSiteSetVerify = storageSiteDAO.list();
        Assert.assertEquals("Should be a single site.", 1, storageSiteSetVerify.size());

        final StorageSite[] storageSitesVerify = storageSiteSetVerify.toArray(new StorageSite[0]);
        Assert.assertEquals("Wrong site name.", "minoc", storageSitesVerify[0].getName());
        Assert.assertEquals("Wrong site resource ID.", URI.create("ivo://cadc.nrc.ca/minoc"),
                            storageSitesVerify[0].getResourceID());
        Assert.assertEquals("Wrong checksum.", expectedChecksum, storageSitesVerify[0].getMetaChecksum());

        // ****************
        // Error conditions
        // ****************

        //
        // Test three
        //
        final StorageSiteSync storageSiteSyncChecksumError = new StorageSiteSync(tapClient, storageSiteDAO) {
            @Override
            ResourceIterator<StorageSite> queryStorageSites()
                    throws AccessControlException, ResourceNotFoundException, ByteLimitExceededException,
                           NotAuthenticatedException, IllegalArgumentException, TransientException, IOException,
                           InterruptedException {
                return new ResourceIteratorModifiedChecksum(storageSites[0]);
            }
        };

        try {
            // Should throw an error.
            //
            Subject.doAs(subject, (PrivilegedExceptionAction<StorageSite>) storageSiteSyncChecksumError::doit);
            Assert.fail("Should throw IllegalStateException.");
        } catch (IllegalStateException e) {
            // Good.
            Assert.assertEquals("Wrong error message.",
                                String.format("Discovered Storage Site checksum (md5:889900) does not "
                                              + "match computed value (%s).", expectedChecksum),
                                e.getMessage());
        }

        //
        // Test four
        //
        final StorageSiteSync storageSiteSyncMultipleSitesError = new StorageSiteSync(tapClient, storageSiteDAO) {
            @Override
            ResourceIterator<StorageSite> queryStorageSites()
                    throws AccessControlException, ResourceNotFoundException, ByteLimitExceededException,
                           NotAuthenticatedException, IllegalArgumentException, TransientException, IOException,
                           InterruptedException {
                return new ResourceIteratorMultipleSites();
            }
        };

        try {
            // Should throw an error.
            //
            Subject.doAs(subject, (PrivilegedExceptionAction<StorageSite>) storageSiteSyncMultipleSitesError::doit);
            Assert.fail("Should throw IllegalStateException.");
        } catch (IllegalStateException e) {
            // Good.
            Assert.assertEquals("Wrong error message.", "More than one Storage Site found.", e.getMessage());
        }

        //
        // Test five
        //
        final StorageSiteSync storageSiteSyncEmptySitesError = new StorageSiteSync(tapClient, storageSiteDAO) {
            @Override
            ResourceIterator<StorageSite> queryStorageSites()
                    throws AccessControlException, ResourceNotFoundException, ByteLimitExceededException,
                           NotAuthenticatedException, IllegalArgumentException, TransientException, IOException,
                           InterruptedException {
                return new ResourceIteratorEmptySites();
            }
        };

        try {
            // Should throw an error.
            //
            Subject.doAs(subject, (PrivilegedExceptionAction<StorageSite>) storageSiteSyncEmptySitesError::doit);
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

            storageSiteList.add(new StorageSite(URI.create("ivo://test/siteone"), "Test Site One."));
            storageSiteList.add(new StorageSite(URI.create("ivo://test/sitetwo"), "Test Site Two."));

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

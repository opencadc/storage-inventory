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
 *
 ************************************************************************
 */

package org.opencadc.fenwick;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.util.AllArtifacts;


/**
 * Integration test for the InventoryHarvester class.
 */
public class InventoryHarvesterTest {

    private static final Logger LOGGER = Logger.getLogger(InventoryHarvesterTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.fenwick", Level.INFO);
    }

    private final InventoryEnvironment inventoryEnvironment = new InventoryEnvironment();
    private final LuskanEnvironment luskanEnvironment = new LuskanEnvironment();
    private final Subject testUser = TestUtil.getConfiguredSubject();
    
    private final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

    public InventoryHarvesterTest() throws Exception {
    }

    @Before
    public void beforeTest() throws Exception {
        inventoryEnvironment.cleanTestEnvironment();
        luskanEnvironment.cleanTestEnvironment();
    }

    //@Test
    public void testMissingStorageSiteSync() throws Exception {
        final InventoryHarvester testSubject = new InventoryHarvester(inventoryEnvironment.daoConfig,
                inventoryEnvironment.connectionConfig, TestUtil.LUSKAN_URI, new AllArtifacts(), true, 20);
        try {
            // TODO: need to run this is separate thread so we can verify and terminate
            Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
                testSubject.run();
                return null;
            });
            
            Assert.fail("Should throw IllegalArgumentException for missing storage site.");
        } catch (IllegalStateException expected) {
            // TODO: verify that sync threads no longer running
        } finally {
            // TODO: kill threads just in case
        }
    }

    
    //@Test
    public void testStartup() throws Exception {
        // setup: create storage site
        StorageSite storageSite = new StorageSite(URI.create("cadc:TESTSITE/one"), "one", true, false);
        luskanEnvironment.storageSiteDAO.put(storageSite);
        
        final InventoryHarvester testSubject = new InventoryHarvester(inventoryEnvironment.daoConfig,
                inventoryEnvironment.connectionConfig, TestUtil.LUSKAN_URI, new AllArtifacts(), true, 20);
        try {
            // TODO: need to run this is separate thread so we can verify and terminate
            Subject.doAs(this.testUser, (PrivilegedExceptionAction<Object>) () -> {
                testSubject.run();
                return null;
            });
            
            // TODO: test to verify that InventoryHarvester.run() starts up all sync threads correctly??
            // TODO: terminate thread
        } finally {
            // TODO: kill threads just in case
        }
        
        
    }
}

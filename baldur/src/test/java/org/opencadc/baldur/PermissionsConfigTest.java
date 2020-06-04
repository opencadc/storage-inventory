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
 *  $Revision: 5 $
 *
 ************************************************************************
 */

package org.opencadc.baldur;

import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.PropertiesReader;

import java.net.URI;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import java.util.Set;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.gms.GroupURI;
import org.opencadc.inventory.permissions.ReadGrant;
import org.opencadc.inventory.permissions.WriteGrant;


public class PermissionsConfigTest {
    private static final Logger log = Logger.getLogger(PermissionsConfigTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.baldur", Level.INFO);
    }
    
    @Test
    public void testSingleEntry() {
        try {
            log.info("START - testSingleEntry");
            
            // test match
            System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "src/test/resources/testSingleEntry");
            PermissionsConfig config = new PermissionsConfig();
            URI artifactURI = URI.create("cadc:TEST/file.fits");

            Set<Principal> allowedUser = config.getAuthorizedPrincipals();
            Assert.assertFalse(allowedUser.isEmpty());
            Assert.assertEquals(2, allowedUser.size());
            Assert.assertTrue(allowedUser.contains(new X500Principal("CN=foo,OU=cadc,O=hia,C=ca")));
            Assert.assertTrue(allowedUser.contains(new X500Principal("CN=bar,OU=cadc,O=hia,C=ca")));

            Iterator<PermissionEntry> entryIterator = config.getMatchingEntries(artifactURI);
            List<PermissionEntry> entries = iteratorToList(entryIterator);
            Assert.assertNotNull(entries);
            Assert.assertEquals(1, entries.size());
            PermissionEntry entry = entries.get(0);
            Assert.assertTrue(entry.anonRead);
            Assert.assertEquals(2, entry.readOnlyGroups.size());
            Assert.assertEquals(2, entry.readWriteGroups.size());
            GroupURI readGroup1 = new GroupURI("ivo://cadc.nrc.ca/gms?group1");
            GroupURI readGroup2 = new GroupURI("ivo://cadc.nrc.ca/gms?group2");
            GroupURI readWriteGroup1 = new GroupURI("ivo://cadc.nrc.ca/gms?group3");
            GroupURI readWriteGroup2 = new GroupURI("ivo://cadc.nrc.ca/gms?group4");
            Assert.assertTrue(entry.readOnlyGroups.contains(readGroup1));
            Assert.assertTrue(entry.readOnlyGroups.contains(readGroup2));
            Assert.assertTrue(entry.readWriteGroups.contains(readWriteGroup1));
            Assert.assertTrue(entry.readWriteGroups.contains(readWriteGroup2));
            
            ReadGrant readGrant = GetAction.getReadGrant(config, artifactURI);
            Assert.assertNotNull(readGrant);
            Assert.assertNotNull(readGrant.getExpiryDate());
            Assert.assertEquals(config.getExpiryDate(), readGrant.getExpiryDate());
            Assert.assertTrue(readGrant.isAnonymousAccess());
            Assert.assertEquals(4, readGrant.getGroups().size());
            Assert.assertTrue(readGrant.getGroups().contains(readGroup1));
            Assert.assertTrue(readGrant.getGroups().contains(readGroup2));
            Assert.assertTrue(readGrant.getGroups().contains(readWriteGroup1));
            Assert.assertTrue(readGrant.getGroups().contains(readWriteGroup2));
            
            WriteGrant writeGrant = GetAction.getWriteGrant(config, artifactURI);
            Assert.assertNotNull(writeGrant);
            Assert.assertNotNull(writeGrant.getExpiryDate());
            Assert.assertEquals(config.getExpiryDate(), writeGrant.getExpiryDate());
            Assert.assertEquals(2, writeGrant.getGroups().size());
            Assert.assertTrue(writeGrant.getGroups().contains(readWriteGroup1));
            Assert.assertTrue(writeGrant.getGroups().contains(readWriteGroup2));
            
            // test no match
            config = new PermissionsConfig();
            artifactURI = URI.create("cadc:NOMATCH/file.fits");
            
            entryIterator = config.getMatchingEntries(artifactURI);
            entries = iteratorToList(entryIterator);
            Assert.assertTrue(entries.isEmpty());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY);
            log.info("END - testSingleEntry");
        }
    }
    
    @Test
    public void testMultipleMatches() {
        try {
            log.info("START - testMultipleMatches");
            System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "src/test/resources/testMultipleMatches");
            PermissionsConfig config = new PermissionsConfig();
            URI artifactURI = URI.create("TEST");

            Assert.assertNotNull(config.getExpiryDate());

            X500Principal expectedUser = new X500Principal("CN=test,OU=cadc,O=hia,C=ca");
            Set<Principal> allowedUser = config.getAuthorizedPrincipals();
            Assert.assertEquals(1, allowedUser.size());
            Assert.assertTrue(allowedUser.contains(expectedUser));
            
            Iterator<PermissionEntry> entryIterator = config.getMatchingEntries(artifactURI);
            List<PermissionEntry> entries = iteratorToList(entryIterator);
            Assert.assertNotNull(entries);
            Assert.assertEquals(2, entries.size());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY);
            log.info("END - testMultipleMatches");
        }
    }
    
    @Test
    public void testOverlappingPermissions() {
        try {
            log.info("START - testOverlappingPermissions");
            System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "src/test/resources/testOverlappingPermissions");
            PermissionsConfig config = new PermissionsConfig();
            URI artifactURI = URI.create("TEST");

            Assert.assertNotNull(config.getExpiryDate());

            X500Principal expectedUser = new X500Principal("CN=test,OU=cadc,O=hia,C=ca");
            Set<Principal> allowedUser = config.getAuthorizedPrincipals();
            Assert.assertEquals(1, allowedUser.size());
            Assert.assertTrue(allowedUser.contains(expectedUser));
            
            Iterator<PermissionEntry> entryIterator = config.getMatchingEntries(artifactURI);
            List<PermissionEntry> entries = iteratorToList(entryIterator);
            Assert.assertNotNull(entries);
            Assert.assertEquals(2, entries.size());
            
            GroupURI readGroup1 = new GroupURI("ivo://cadc.nrc.ca/gms?group1");
            GroupURI readGroup2 = new GroupURI("ivo://cadc.nrc.ca/gms?group3");
            GroupURI readWriteGroup1 = new GroupURI("ivo://cadc.nrc.ca/gms?group2");
            GroupURI readWriteGroup2 = new GroupURI("ivo://cadc.nrc.ca/gms?group4");
            
            ReadGrant readGrant = GetAction.getReadGrant(config, artifactURI);
            Assert.assertNotNull(readGrant);
            Assert.assertNotNull(readGrant.getExpiryDate());
            Assert.assertEquals(config.getExpiryDate(), readGrant.getExpiryDate());
            Assert.assertTrue(readGrant.isAnonymousAccess());
            Assert.assertEquals(4, readGrant.getGroups().size());
            Assert.assertTrue(readGrant.getGroups().contains(readGroup1));
            Assert.assertTrue(readGrant.getGroups().contains(readGroup2));
            Assert.assertTrue(readGrant.getGroups().contains(readWriteGroup1));
            Assert.assertTrue(readGrant.getGroups().contains(readWriteGroup2));
            
            WriteGrant writeGrant = GetAction.getWriteGrant(config, artifactURI);
            Assert.assertNotNull(writeGrant);
            Assert.assertNotNull(writeGrant.getExpiryDate());
            Assert.assertEquals(config.getExpiryDate(), writeGrant.getExpiryDate());
            Assert.assertEquals(2, writeGrant.getGroups().size());
            Assert.assertTrue(writeGrant.getGroups().contains(readWriteGroup1));
            Assert.assertTrue(writeGrant.getGroups().contains(readWriteGroup2));
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY);
            log.info("END - testOverlappingPermissions");
        }
    }
    
    @Test
    public void testMissingConfig() {
        try {
            log.info("START - testMissingConfig");
            System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "src/test/resources/testMissingConfig");
            try {
                new PermissionsConfig();
                Assert.fail("should have received IllegalStateException");
            } catch (IllegalStateException e) {
                // expected
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY);
            log.info("END - testMissingConfig");
        }
    }
    
    @Test
    public void testEmptyConfig() {
        try {
            log.info("START - testEmptyConfig");
            System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "src/test/resources/testEmptyConfig");
            try {
                new PermissionsConfig();
                Assert.fail("should have received IllegalStateException");
            } catch (IllegalStateException e) {
                // expected
                Assert.assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("baldur.properties"));
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY);
            log.info("END - testEmptyConfig");
        }
    }
    
    @Test
    public void testDuplicateEntries() {
        try {
            log.info("START - testDuplicateEntries");
            System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "src/test/resources/testDuplicateEntries");
            try {
                new PermissionsConfig();
                Assert.fail("should have received IllegalStateException");
            } catch (IllegalStateException e) {
                // expected
                Assert.assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("duplicate"));
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY);
            log.info("END - testDuplicateEntries");
        }
    }
    
    @Test
    public void testDuplicatePermissions() {
        try {
            log.info("START - testDuplicatePermissions");
            System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "src/test/resources/testDuplicatePermissions");
            try {
                new PermissionsConfig();
                Assert.fail("should have received IllegalStateException");
            } catch (IllegalStateException e) {
                // expected
                Assert.assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("too many"));
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.clearProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY);
            log.info("END - testDuplicatePermissions");
        }
    }

    private List<PermissionEntry> iteratorToList(Iterator<PermissionEntry> it) {
        List<PermissionEntry> list = new ArrayList<PermissionEntry>();
        it.forEachRemaining(list::add);
        return list;
    }

}

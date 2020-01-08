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

import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.gms.GroupURI;


public class PermissionsConfigTest {
    private static final Logger log = Logger.getLogger(PermissionsConfig.class);

    static {
        Log4jInit.setLevel("org.opencadc.baldur", Level.INFO);
    }

    @Test
    public void testLoadGroupURIS() throws Exception {
        try {
            PermissionsConfig testSubject = getTestSubject(null);

            // Should never be an empty group string..
            List<GroupURI> groups = new ArrayList<GroupURI>();
            StringBuilder sb = new StringBuilder();
            String groupUris = "";
            String accessType = "read-only";

            testSubject.loadGroupURIS(groups, sb, groupUris, accessType);
            Assert.assertTrue(sb.length() > 0);

            // Invalid group uri
            sb.setLength(0);
            groupUris = "foo";

            testSubject.loadGroupURIS(groups, sb, groupUris, accessType);
            Assert.assertTrue(sb.length() > 0);

            // Valid group uri
            sb.setLength(0);
            groupUris = "ivo://cadc.nrc.ca/gms?CFHT";

            testSubject.loadGroupURIS(groups, sb, groupUris, accessType);
            Assert.assertEquals(0, sb.length());

            // Two Valid group uri's
            sb.setLength(0);
            groupUris = "ivo://cadc.nrc.ca/gms?CFHT,ivo://cadc.nrc.ca/gms?JCMT";

            testSubject.loadGroupURIS(groups, sb, groupUris, accessType);
            Assert.assertEquals(0, sb.length());

            // One Valid group uri, one invalid
            sb.setLength(0);
            groupUris = "ivo://cadc.nrc.ca/gms?CFHT,ivo:cadc.nrc.ca/gms";

            testSubject.loadGroupURIS(groups, sb, groupUris, accessType);
            Assert.assertTrue(sb.length() > 0);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private void testParsePermissions(PermissionsConfig testSubject, String value) {
        Permissions actual = testSubject.parsePermissions(value);
        Assert.assertNotNull(actual);
        Assert.assertFalse(actual.getIsAnonymous());
        Assert.assertEquals(0, actual.getReadOnlyGroups().size());
        Assert.assertEquals(0, actual.getReadWriteGroups().size());
    }
    @Test
    public void testParsePermissions() throws Exception {
        try {
            String fooUri = "ivo://cadc.nrc.ca/gms?FOO";
            String barUri = "ivo://cadc.nrc.ca/gms?BAR";
            String bazUri = "ivo://cadc.nrc.ca/gms?BAZ";
            String hamUri = "ivo://cadc.nrc.ca/gms?HAM";
            String spamUri = "ivo://cadc.nrc.ca/gms?SPAM";
            GroupURI fooURI = new GroupURI(fooUri);
            GroupURI barURI = new GroupURI(barUri);
            GroupURI bazURI = new GroupURI(bazUri);
            GroupURI hamURI = new GroupURI(hamUri);
            GroupURI spamURI = new GroupURI(spamUri);
            String value;
            Permissions actual;

            PermissionsConfig testSubject = getTestSubject(null);

            value = "";
            testParsePermissions(testSubject, value);

            value = "T foo ";
            testParsePermissions(testSubject, value);

            value = "Z foo://bar far://foo";
            testParsePermissions(testSubject, value);

            value = "T foo foo://bar";
            testParsePermissions(testSubject, value);

            value = "F foo://bar bar";
            testParsePermissions(testSubject, value);

            value = "T " + fooUri + " " + barUri;
            actual = testSubject.parsePermissions(value);
            Assert.assertNotNull(actual);
            Assert.assertTrue(actual.getIsAnonymous());
            Assert.assertEquals(fooURI, actual.getReadOnlyGroups().get(0));
            Assert.assertEquals(barURI, actual.getReadWriteGroups().get(0));

            value = "F " + fooUri + "," + barUri + "," + bazUri + " " + hamUri + "," + spamUri;
            actual = testSubject.parsePermissions(value);
            Assert.assertNotNull(actual);
            Assert.assertFalse(actual.getIsAnonymous());
            Assert.assertEquals(fooURI, actual.getReadOnlyGroups().get(0));
            Assert.assertEquals(barURI, actual.getReadOnlyGroups().get(1));
            Assert.assertEquals(bazURI, actual.getReadOnlyGroups().get(2));
            Assert.assertEquals(hamURI, actual.getReadWriteGroups().get(0));
            Assert.assertEquals(spamURI, actual.getReadWriteGroups().get(1));

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testLoadEmptyConfig() throws Exception {
        try {
            // Empty file path
            try {
                PermissionsConfig actual = new PermissionsConfig(new File(""));
                Assert.fail("Should throw FileNotFoundException");
            } catch (Exception expected) {
                Assert.assertTrue(expected instanceof  FileNotFoundException);
            }

            // Empty properties file
            File file = FileUtil.getFileFromResource("empty.properties", PermissionsConfigTest.class);
            try {
                PermissionsConfig testSubject = new PermissionsConfig(file);
                Permissions actual = testSubject.getPermissions(new URI("ad:CFHT/1234.fits"));
                Assert.assertFalse(actual.getIsAnonymous());
                Assert.assertTrue(actual.getReadOnlyGroups().isEmpty());
                Assert.assertTrue(actual.getReadWriteGroups().isEmpty());
            } catch (Exception e) {
                Assert.fail("Should not throw Exception: " + e);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetPattern() throws Exception {
        try {
            PermissionsConfig testSubject = getTestSubject(null);

            // matches that fail
            String configKey = "ivo:foo0/*";
            String artifactUri = "ad:foo0/1234.fits";

            Pattern pattern = testSubject.getPattern(configKey);
            boolean matches = pattern.matcher(artifactUri).matches();
            Assert.assertFalse(matches);

            configKey = "ad:foo0/*";
            artifactUri = "ad:foo1/1234.fits";

            pattern = testSubject.getPattern(configKey);
            matches = pattern.matcher(artifactUri).matches();
            Assert.assertFalse(matches);

            // matches that succeed
            configKey = "ad:foo0/*";
            artifactUri = "ad:foo0/a";

            pattern = testSubject.getPattern(configKey);
            matches = pattern.matcher(artifactUri).matches();
            Assert.assertTrue(matches);

            configKey = "ad:foo0/*";
            artifactUri = "ad:foo0/1.fz";

            pattern = testSubject.getPattern(configKey);
            matches = pattern.matcher(artifactUri).matches();
            Assert.assertTrue(matches);

            configKey = "ad:foo0/*";
            artifactUri = "ad:foo0/1234.fits";

            pattern = testSubject.getPattern(configKey);
            matches = pattern.matcher(artifactUri).matches();
            Assert.assertTrue(matches);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testLoadInvalidConfig() throws Exception {
        try {
            // Invalid properties file
            File file = FileUtil.getFileFromResource("invalid.properties", PermissionsConfigTest.class);
            PermissionsConfig testSubject = new PermissionsConfig(file);
            Permissions actual;

            // missing flag and groups
            testInvalidConfig(testSubject, new URI("foo:bar0/1234.fits"));

            // missing groups
            testInvalidConfig(testSubject, new URI("foo:bar1/1234.fits"));

            // missing read-write group
            // foo:bar2/*=F ivo://cadc.nrc.ca/gms?BAR-RO-1
            testInvalidConfig(testSubject, new URI("foo:bar2/1234.fits"));

            // invalid read-write group
            // foo:bar3/* = F ivo://cadc.nrc.ca/gms?BAR-RO-1 bar
            testInvalidConfig(testSubject, new URI("foo:bar3/1234.fits"));

            // invalid read-only group
            // foo:bar4/*=T foo ivo://cadc.nrc.ca/gms?BAR-RW-1
            testInvalidConfig(testSubject, new URI("foo:bar4/1234.fits"));

            // invalid anonymous flag
            // foo:bar5/* = Z ivo://cadc.nrc.ca/gms?BAR-RO-1 ivo://cadc.nrc.ca/gms?BAR-RW-1
            testInvalidConfig(testSubject, new URI("foo:bar5/1234.fits"));

            // invalid read-only group
            // foo:bar6/*=F ivo://cadc.nrc.ca/gms?BAR-RO-1,foo ivo://cadc.nrc.ca/gms?BAR-RW-1
            testInvalidConfig(testSubject, new URI("foo:bar6/1234.fits"));

            // invalid read-write group
            // foo:bar7/* = T ivo://cadc.nrc.ca/gms?BAR-RO-1 ivo://cadc.nrc.ca/gms?BAR-RW-1,bar
            testInvalidConfig(testSubject, new URI("foo:bar7/1234.fits"));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private void testInvalidConfig(PermissionsConfig testSubject, URI testUri) {
        Permissions actual = testSubject.getPermissions(testUri);
        Assert.assertNotNull(actual);
        Assert.assertFalse(actual.getIsAnonymous());
        Assert.assertEquals(0, actual.getReadOnlyGroups().size());
        Assert.assertEquals(0, actual.getReadWriteGroups().size());
    }

    @Test
    public void testLoadValidConfig() throws Exception {
        try {
            File file = FileUtil.getFileFromResource("valid.properties", PermissionsConfigTest.class);
            PermissionsConfig testSubject = new PermissionsConfig(file);
            Permissions actual;

            // One group each
            // foo:bar0/* = T ivo://cadc.nrc.ca/gms?BAR-RO-1     ivo://cadc.nrc.ca/gms?BAR-RW-1
            actual = testSubject.getPermissions(new URI("foo:bar0/1234.fits"));
            Assert.assertNotNull(actual);
            Assert.assertTrue(actual.getIsAnonymous());
            Assert.assertEquals(1, actual.getReadOnlyGroups().size());
            Assert.assertEquals(1, actual.getReadWriteGroups().size());
            Assert.assertTrue(actual.getReadOnlyGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RO-1")));
            Assert.assertTrue(actual.getReadWriteGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RW-1")));

            // Two read-only groups, one read-write group
            // foo:bar1/* =F ivo://cadc.nrc.ca/gms?BAR-RO-1,ivo://cadc.nrc.ca/gms?BAR-RO-2 ivo://cadc.nrc.ca/gms?BAR-RW-1
            actual = testSubject.getPermissions(new URI("foo:bar1/1234.fits"));
            Assert.assertNotNull(actual);
            Assert.assertFalse(actual.getIsAnonymous());
            Assert.assertEquals(2, actual.getReadOnlyGroups().size());
            Assert.assertEquals(1, actual.getReadWriteGroups().size());
            Assert.assertTrue(actual.getReadOnlyGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RO-1")));
            Assert.assertTrue(actual.getReadOnlyGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RO-2")));
            Assert.assertTrue(actual.getReadWriteGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RW-1")));

            // One read-only group, two read-write groups
            // foo:bar2/* = T ivo://cadc.nrc.ca/gms?BAR-RO-1     ivo://cadc.nrc.ca/gms?BAR-RW-1,ivo://cadc.nrc.ca/gms?BAR-RW-2
            actual = testSubject.getPermissions(new URI("foo:bar2/1234.fits"));
            Assert.assertNotNull(actual);
            Assert.assertTrue(actual.getIsAnonymous());
            Assert.assertEquals(1, actual.getReadOnlyGroups().size());
            Assert.assertEquals(2, actual.getReadWriteGroups().size());
            Assert.assertTrue(actual.getReadOnlyGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RO-1")));
            Assert.assertTrue(actual.getReadWriteGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RW-1")));
            Assert.assertTrue(actual.getReadWriteGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RW-2")));

            // Two read-only groups, two read-write groups
            // foo:bar3/* =  F ivo://cadc.nrc.ca/gms?BAR-RO-1,ivo://cadc.nrc.ca/gms?BAR-RO-2  ivo://cadc.nrc.ca/gms?BAR-RW-1,ivo://cadc.nrc.ca/gms?BAR-RW-2
            actual = testSubject.getPermissions(new URI("foo:bar3/1234.fits"));
            Assert.assertNotNull(actual);
            Assert.assertFalse(actual.getIsAnonymous());
            Assert.assertEquals(2, actual.getReadOnlyGroups().size());
            Assert.assertEquals(2, actual.getReadWriteGroups().size());
            Assert.assertTrue(actual.getReadOnlyGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RO-1")));
            Assert.assertTrue(actual.getReadOnlyGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RO-2")));
            Assert.assertTrue(actual.getReadWriteGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RW-1")));
            Assert.assertTrue(actual.getReadWriteGroups().contains(new GroupURI("ivo://cadc.nrc.ca/gms?BAR-RW-2")));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private PermissionsConfig getTestSubject(File file) throws Exception{
        return new PermissionsConfig(new File("/")) {
            @Override
            List<String> loadConfig(File propertiesfile) {
                if (file == null) {
                    return null;
                } else {
                    return this.loadConfig(file);
                }
            }
        };
    }

}

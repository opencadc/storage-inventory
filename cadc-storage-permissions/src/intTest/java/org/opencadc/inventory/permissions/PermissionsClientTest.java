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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.inventory.permissions;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencadc.gms.GroupURI;

public class PermissionsClientTest {

    private static final Logger log = Logger.getLogger(PermissionsClientTest.class);

    private static Subject cadcAnonTest1Subject;
    private static Subject cadcRegTest1Subject;
    private static URI serviceID;

    private static URI testArtifact;
    private static GroupURI readGroup1;
    private static GroupURI writeGroup1;
    private static GroupURI writeGroup2;
    private static GroupURI writeGroup3;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        Log4jInit.setLevel("org.opencadc.inventory.permissions", Level.INFO);

        cadcAnonTest1Subject = SSLUtil.createSubject(
            FileUtil.getFileFromResource("x509_CADCAnontest1.pem", PermissionsClientTest.class));
        cadcRegTest1Subject = SSLUtil.createSubject(
            FileUtil.getFileFromResource("x509_CADCRegtest1.pem", PermissionsClientTest.class));

        serviceID = URI.create("ivo://cadc.nrc.ca/baldur");

        testArtifact = URI.create("cadc:TEST-GROUPS/foo");
        readGroup1 = new GroupURI("ivo://cadc.nrc.ca/gms?TestReadGroup-1");
        writeGroup1 = new GroupURI("ivo://cadc.nrc.ca/gms?TestWriteGroup-1");
        writeGroup2 = new GroupURI("ivo://cadc.nrc.ca/gms?TestWriteGroup-2");
        writeGroup3 = new GroupURI("ivo://cadc.nrc.ca/gms?TestWriteGroup-3");
    }

    @Test
    public void testAnonAccess() {
        try {
            Subject.doAs(cadcAnonTest1Subject, new PrivilegedExceptionAction<Object>()
            {
                @Override
                public Object run() throws Exception
                {
                    PermissionsClient testSubject = new PermissionsClient(serviceID);
                    try {
                        ReadGrant readGrant = testSubject.getReadGrant(testArtifact);
                        Assert.fail("Anonymous user access should " + "throw exception");
                    } catch (Exception expected) { }
                    return null;
                }
            });
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetReadGrants() {
        try {
            Subject.doAs(cadcRegTest1Subject, new PrivilegedExceptionAction<Object>()
            {
                @Override
                public Object run() throws Exception
                {
                    PermissionsClient testSubject = new PermissionsClient(serviceID);
                    ReadGrant readGrant = testSubject.getReadGrant(testArtifact);
                    Assert.assertNotNull(readGrant);
                    Assert.assertTrue(readGrant.isAnonymousAccess());

                    List<GroupURI> groups = readGrant.groups;
                    Assert.assertEquals(groups.size(), 4);
                    Assert.assertTrue(groups.contains(readGroup1));
                    Assert.assertTrue(groups.contains(writeGroup1));
                    Assert.assertTrue(groups.contains(writeGroup2));
                    Assert.assertTrue(groups.contains(writeGroup3));
                    return null;
                }
            });
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetWriteGrants() {
        try {
            Subject.doAs(cadcRegTest1Subject, new PrivilegedExceptionAction<Object>()
            {
                @Override
                public Object run() throws Exception
                {
                    PermissionsClient testSubject = new PermissionsClient(serviceID);
                    WriteGrant writeGrant = testSubject.getWriteGrant(testArtifact);
                    Assert.assertNotNull(writeGrant);

                    List<GroupURI> groups = writeGrant.groups;
                    Assert.assertEquals(groups.size(), 3);
                    Assert.assertTrue(groups.contains(writeGroup1));
                    Assert.assertTrue(groups.contains(writeGroup2));
                    Assert.assertTrue(groups.contains(writeGroup3));

                    return null;
                }
            });
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}

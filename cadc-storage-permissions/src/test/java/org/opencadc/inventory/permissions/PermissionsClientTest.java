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

import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.Date;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opencadc.inventory.permissions.xml.GrantWriter;

public class PermissionsClientTest {

    private static final Logger log =
        Logger.getLogger(PermissionsClientTest.class);

    static {
        Log4jInit.setLevel(
            "package org.opencadc.inventory.permissions", Level.INFO);
    }

    public PermissionsClientTest() {}

    @Ignore
    @Test
    public void testGetGrant() {
        try {
            ReadGrant expected = new ReadGrant(
                URI.create("ivo:foo/bar"), new Date(), true);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GrantWriter writer = new GrantWriter();
            writer.write(expected, out);
            String grantXml = out.toString();

            final URL serviceURL = new URL("http://service.com/url");
            final URL grantURL = new URL("http://grant.com/url");
            URI serviceID = new URI("foo:service/id");
            URI artifactURI = new URI("foo:artifact/uri");
            PermissionsClient.Operation op = PermissionsClient.Operation.read;

            HttpGet mockHttpGet = EasyMock.createMock(HttpGet.class);
            EasyMock.expect(mockHttpGet.getThrowable()).andReturn(null);
            EasyMock.expect(mockHttpGet.getResponseCode()).andReturn(200);

            ByteArrayOutputStream mockOut = EasyMock.createMock(ByteArrayOutputStream.class);
            EasyMock.expect(mockOut.toString()).andReturn(grantXml);

            EasyMock.replay(mockHttpGet);
            EasyMock.replay(mockOut);

            PermissionsClient testSubject = new PermissionsClient(serviceID) {

                URL getServiceURL(URI serviceID) {
                    return serviceURL;
                }

                URL getGrantURL(URL serviceURL, Operation op, URI artifactURI) {
                    return grantURL;
                }

                HttpGet getHttpGet(URL grantURL, OutputStream out) {
                    return mockHttpGet;
                }

                OutputStream getOutputStream() {
                    return mockOut;
                }
            };
            Grant actual = testSubject.getGrant(artifactURI, op);

            Assert.assertNotNull(actual);
            Assert.assertTrue(actual instanceof ReadGrant);
            Assert.assertEquals(expected, actual);

            EasyMock.verify(mockHttpGet, mockOut);
        } catch (Exception unexpected) {
            unexpected.printStackTrace();
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}

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
************************************************************************
*/

package org.opencadc.minoc;

import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class ArtifactActionTest {

    private static final Logger log = Logger.getLogger(ArtifactActionTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.minoc", Level.INFO);
    }
    
    private void assertCorrectPath(String path, String expURI, String expToken) {
        assertCorrectPath(path, expURI, expToken, null);
    }
    
    private void assertCorrectPath(String path, String expURI, String expToken, String expFilenameOverride) {
        boolean extractFileNameOverride = (expFilenameOverride != null);
        ArtifactAction.ParsedPath pp = ArtifactAction.parsePath(path, extractFileNameOverride);
        log.info(path + " -> " + pp.artifactURI + " - " + pp.authToken + " - " + pp.filenameOverride);
        Assert.assertEquals("artifactURI", URI.create(expURI), pp.artifactURI);
        Assert.assertEquals("authToken", expToken, pp.authToken);
        Assert.assertEquals("filenameOverride", expFilenameOverride, pp.filenameOverride);
        if (pp.artifactURI == null) {
            Assert.fail("Failed to parse legal path: " + path);
        }
    }
    
    private void assertIllegalPath(String path, boolean expectException) {
        log.info("assertIllegalPath: " + path);
        if (expectException) {
            try {
                ArtifactAction.ParsedPath pp = ArtifactAction.parsePath(path, false);
                Assert.fail("expected IllegalArgumentException, got: " + pp);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
        } else {
            ArtifactAction.ParsedPath action = ArtifactAction.parsePath(path, false);
            Assert.assertNotNull(action);
            Assert.assertNull(action.artifactURI);
        }
    }
    
    @Test
    public void testParsePath() {
        try {
            
            assertCorrectPath("cadc:TEST/myartifact", "cadc:TEST/myartifact", null);
            assertCorrectPath("token/cadc:TEST/myartifact", "cadc:TEST/myartifact", "token");
            assertCorrectPath("cadc:TEST/myartifact", "cadc:TEST/myartifact", null);
            assertCorrectPath("token/cadc:TEST/myartifact", "cadc:TEST/myartifact", "token");
            assertCorrectPath("mast:long/uri/with/segments/something.fits", "mast:long/uri/with/segments/something.fits", null);
            assertCorrectPath("token/mast:long/uri/with/segments/fits.fits", "mast:long/uri/with/segments/fits.fits", "token");
            assertCorrectPath("token-with-dashes/cadc:TEST/myartifact", "cadc:TEST/myartifact", "token-with-dashes");
            
            assertCorrectPath("cadc:vault/uuid:fo/something.fits", "cadc:vault/uuid", null, "something.fits");
            assertCorrectPath("token/cadc:vault/uuid:fo/something.fits", "cadc:vault/uuid", "token", "something.fits");
            
            assertIllegalPath(null, false);
            assertIllegalPath("", false);
            assertIllegalPath("noschemeinuri", false);
            assertIllegalPath("token/noschemeinuri", false);
            assertIllegalPath("cadc:path#fragment", true);
            assertIllegalPath("cadc:path?query", true);
            assertIllegalPath("cadc:path#fragment?query", true);
            assertIllegalPath("cadc://host/path", true);
            assertIllegalPath("cadc://host:port/path", true);
            assertIllegalPath("artifacts/token1/token2/cadc:FOO/bar", true); // sketchy scheme
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
}
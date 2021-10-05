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

import ca.nrc.cadc.net.RangeNotSatisfiableException;
import ca.nrc.cadc.rest.SyncInput;
import ca.nrc.cadc.util.Log4jInit;
import java.io.IOException;
import java.util.SortedSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.storage.ByteRange;

public class GetActionTest {

    private static final Logger log = Logger.getLogger(GetActionTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.minoc", Level.INFO);
    }
    
    class TestSyncInput extends SyncInput {

        private String path;
        
        public TestSyncInput(String path) throws IOException {
            super(null, null);
            this.path = path;
        }
        
        public String getPath() {
            return path;
        }

        public String getComponentPath() {
            return "";
        }
    }

    private void assertIgnoredRange(String range, long contentLength) throws RangeNotSatisfiableException {
        GetAction action = new GetAction(false);
        Assert.assertNull(action.parseRange(range, contentLength));
    }
    
    private void assertCorrectRange(String range, long contentLength, long expectedOffset, long expectedLength)
            throws RangeNotSatisfiableException {
        GetAction action = new GetAction(false);
        SortedSet<ByteRange> byteRangeSet = action.parseRange(range, contentLength);
        Assert.assertEquals(1, byteRangeSet.size());
        ByteRange br = byteRangeSet.first();
        Assert.assertEquals(expectedOffset, br.getOffset());
        Assert.assertEquals(expectedLength, br.getLength());
    }
    
    @Test
    public void testParseRange() throws Exception {
        assertCorrectRange("bytes=3-99", 200, 3, 97);
        assertCorrectRange(" bytes \t= 3 -   99 ", 200, 3, 97);
        assertCorrectRange("bytes=3-99", 50, 3, 47);
        assertCorrectRange("bytes=-99", 50, 0, 50);
        assertCorrectRange("bytes=20-", 50, 20, 30);
        assertCorrectRange("bytes=20-50", 50, 20, 30);

        assertIgnoredRange("nobyteunit=2-4", 10);
        assertIgnoredRange("bytes 2-4", 10);
        assertIgnoredRange("2 - 4", 10);
        assertIgnoredRange("bytes=2-4,6-8", 10);
        assertIgnoredRange("bytes=9-7", 10);
        assertIgnoredRange("bytes=2:4", 10);

        GetAction action = new GetAction(false);
        Assert.assertThrows(Exception.class, () -> {
            action.parseRange("bytes=30-40", 20); });
        Assert.assertThrows(RangeNotSatisfiableException.class, () -> {
            action.parseRange("bytes=30-", 20); });
    }
    
}
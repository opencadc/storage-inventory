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

package org.opencadc.minoc;

import java.util.Arrays;

import ca.nrc.cadc.util.Log4jInit;
import nom.tam.fits.BasicHDU;
import nom.tam.fits.Fits;
import nom.tam.fits.Header;
import nom.tam.fits.HeaderCard;
import nom.tam.fits.header.IFitsHeader;
import nom.tam.fits.header.Standard;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;


public class FitsTest {
    private static final Logger LOGGER = Logger.getLogger(FitsTest.class);

    static {
        Log4jInit.setLevel("org.opencadc", Level.INFO);
    }

    private static final IFitsHeader[] HEADER_CARD_KEYS_TO_CHECK = new IFitsHeader[]{
            Standard.BITPIX, Standard.NAXIS, Standard.EXTNAME, Standard.XTENSION, Standard.SIMPLE, Standard.EXTVER,
            Standard.BSCALE, Standard.BUNIT
    };

    public static void assertFitsEqual(final Fits expected, final Fits result) throws Exception {
        final BasicHDU<?>[] expectedHDUList = expected.read();
        final BasicHDU<?>[] resultHDUList = result.read();

        Assert.assertEquals("Wrong number of HDUs.", expectedHDUList.length, resultHDUList.length);

        for (int expectedIndex = 0; expectedIndex < expectedHDUList.length; expectedIndex++) {
            final BasicHDU<?> nextExpectedHDU = expectedHDUList[expectedIndex];
            final BasicHDU<?> nextResultHDU = resultHDUList[expectedIndex];

            try {
                FitsTest.assertHDUEqual(nextExpectedHDU, nextResultHDU);
            } catch (AssertionError assertionError) {
                LOGGER.error("On Extension at index " + expectedIndex);
                throw assertionError;
            }
        }
    }

    public static void assertHDUEqual(final BasicHDU<?> expectedHDU, final BasicHDU<?> resultHDU) throws Exception {
        final Header expectedHeader = expectedHDU.getHeader();
        final Header resultHeader = resultHDU.getHeader();

        FitsTest.assertHeadersEqual(expectedHeader, resultHeader);
    }

    public static void assertHeadersEqual(final Header expectedHeader, final Header resultHeader) throws Exception {
        Arrays.stream(HEADER_CARD_KEYS_TO_CHECK).forEach(headerCardKey -> {
            final HeaderCard expectedCard = expectedHeader.findCard(headerCardKey);
            final HeaderCard resultCard = resultHeader.findCard(headerCardKey);

            if (expectedCard == null) {
                Assert.assertNull("Card " + headerCardKey.key() + " should be null.", resultCard);
            } else {
                Assert.assertNotNull("Header " + headerCardKey.key() + " should not be null.", resultCard);
                Assert.assertEquals("Header " + headerCardKey.key() + " has the wrong value.",
                                    expectedCard.getValue(), resultCard.getValue());
            }
        });

        final int axes = expectedHeader.getIntValue(Standard.NAXIS);
        Assert.assertEquals("Wrong NAXIS value.", axes, resultHeader.getIntValue(Standard.NAXIS));
        for (int i = 1; i <= axes; i++) {
            final int expectedAxes = expectedHeader.getIntValue(Standard.NAXISn.n(i));
            final int resultAxes = resultHeader.getIntValue(Standard.NAXISn.n(i));

            Assert.assertEquals("Wrong NAXIS" + i + " value.", expectedAxes, resultAxes, 0.1);

            final double expectedCRPix = expectedHeader.getDoubleValue(Standard.CRPIXn.n(i));
            final double resultCRPix = resultHeader.getDoubleValue(Standard.CRPIXn.n(i));

            Assert.assertEquals("Wrong CRPIX" + i + " value.", expectedCRPix, resultCRPix, 0.1);

            final double expectedCRVal = expectedHeader.getDoubleValue(Standard.CRVALn.n(i));
            final double resultCRVal = resultHeader.getDoubleValue(Standard.CRVALn.n(i));

            Assert.assertEquals("Wrong CRVAL" + i + " value.", expectedCRVal, resultCRVal, 0.1);
        }
    }
}

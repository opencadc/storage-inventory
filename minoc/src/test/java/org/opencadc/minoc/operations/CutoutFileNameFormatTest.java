/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2021.                            (c) 2021.
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

package org.opencadc.minoc.operations;

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.Range;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.soda.ExtensionSlice;
import org.opencadc.soda.ExtensionSliceFormat;
import org.opencadc.soda.PixelRange;
import org.opencadc.soda.server.Cutout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CutoutFileNameFormatTest {
    /**
     * Simple two extension MEF cutout.
     * [2]
     * [SCI,3][400:500]
     */
    @Test
    public void testFileNameMangleSimple() {
        final CutoutFileNameFormat testSubject = new CutoutFileNameFormat("my_file.fits");
        final Cutout cutout = new Cutout();
        final List<ExtensionSlice> testSlices = new ArrayList<>();
        testSlices.add(new ExtensionSlice(2));

        cutout.pixelCutouts = testSlices;

        final ExtensionSlice slice2 = new ExtensionSlice("SCI", 3);
        slice2.getPixelRanges().add(new PixelRange(400, 500));
        testSlices.add(slice2);

        final String result = testSubject.format(cutout);
        final String expected = "my_file.2____SCI_3__400_500.fits";

        Assert.assertEquals("Wrong output.", expected, result);
    }

    /**
     * Larger MEF cutout with full length pixel ranges (<code>*</code>).
     * [116][100:250,*]
     * [SCI,14][100:125,100:175]
     * [91][*,90:255]
     */
    @Test
    public void testFileNameMangleComplex() {
        final ExtensionSliceFormat format = new ExtensionSliceFormat();
        final Cutout cutout = new Cutout();
        final String[] cutouts = new String[] {
                "[116][100:250,*]",
                "[SCI,14][100:125,100:175]",
                "[91][*,90:255]",
                "[101]"
        };
        cutout.pixelCutouts = Arrays.stream(cutouts).map(format::parse).collect(Collectors.toList());
        final CutoutFileNameFormat testSubject = new CutoutFileNameFormat("myfile_raw.fits");

        final String result = testSubject.format(cutout);
        final String expected = "myfile_raw.116__100_250_____SCI_14__100_125_100_175___91____90_255___101.fits";

        Assert.assertEquals("Wrong output.", expected, result);
    }

    @Test
    public void testFileNameMangleCircle() {
        final Cutout cutout = new Cutout();
        cutout.pos = new Circle(new Point(45.6D, 77.3D), 0.5D);

        final CutoutFileNameFormat testSubject = new CutoutFileNameFormat("fitsfile.fits");

        Assert.assertEquals("Wrong filename.", "fitsfile.circle_45_6_77_3_0_5.fits",
                            testSubject.format(cutout));
    }

    @Test
    public void testFileNameManglePolygon() {
        final Cutout cutout = new Cutout();
        final Polygon polygon = new Polygon();

        polygon.getVertices().add(new Point(12.4D, 38.4D));
        polygon.getVertices().add(new Point(12.4D, 58.4D));
        polygon.getVertices().add(new Point(0.4D, 58.4D));
        polygon.getVertices().add(new Point(0.4D, 38.4D));

        cutout.pos = polygon;

        final CutoutFileNameFormat testSubject = new CutoutFileNameFormat("fitsfile.fits");

        Assert.assertEquals("Wrong filename.",
                            "fitsfile.polygon_12_4_38_4_12_4_58_4_0_4_58_4_0_4_38_4.fits",
                            testSubject.format(cutout));
    }

    @Test
    public void testFileNameMangleRange() {
        final Cutout cutout = new Cutout();
        cutout.pos = new Range(new DoubleInterval(0.12D, 3.45D),
                               new DoubleInterval(6.78D, 9.10D));

        final CutoutFileNameFormat testSubject = new CutoutFileNameFormat("fitsfile.fits");

        Assert.assertEquals("Wrong filename.", "fitsfile.range_0_12_3_45_6_78_9_1.fits",
                            testSubject.format(cutout));
    }
}

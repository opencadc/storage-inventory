/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2011.                            (c) 2011.
 * National Research Council            Conseil national de recherches
 * Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 * All rights reserved                  Tous droits reserves
 *
 * NRC disclaims any warranties         Le CNRC denie toute garantie
 * expressed, implied, or statu-        enoncee, implicite ou legale,
 * tory, of any kind with respect       de quelque nature que se soit,
 * to the software, including           concernant le logiciel, y com-
 * without limitation any war-          pris sans restriction toute
 * ranty of merchantability or          garantie de valeur marchande
 * fitness for a particular pur-        ou de pertinence pour un usage
 * pose.  NRC shall not be liable       particulier.  Le CNRC ne
 * in any event for any damages,        pourra en aucun cas etre tenu
 * whether direct or indirect,          responsable de tout dommage,
 * special or general, consequen-       direct ou indirect, particul-
 * tial or incidental, arising          ier ou general, accessoire ou
 * from the use of the software.        fortuit, resultant de l'utili-
 *                                      sation du logiciel.
 *
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 ************************************************************************
 */

package org.opencadc.inventory.storage;

import java.util.Arrays;


/**
 * Class for holding cutout information.
 * 
 * @author majorb
 *
 */
public class Cutout
{
    
    private HDULocation hduLocation;
    private String section;
    private int[] offsets;

    /**
     * Constructor.
     * 
     * @param hduLocation       The HDU Location
     * @param section
     * @param offsets
     */
    public Cutout(HDULocation hduLocation, String section, int[] offsets)
    {
        this.hduLocation = hduLocation;
        this.section = section;
        this.offsets = offsets;
    }

    public HDULocation getHDULocation()
    {
        return hduLocation;
    }

    public String getSection()
    {
        return section;
    }

    public int[] getCorners(final int[] dimensions) {
        final String[] ranges = section.split(",");
        final int cutoutDimensionCount = ranges.length;
        final int[] corners = Arrays.copyOf(dimensions, dimensions.length);

        for (int i = (cutoutDimensionCount - 1), j = 0; (i >= 0) && (j < cutoutDimensionCount); i--, j++) {
            final String range = ranges[i];
            final String[] parsedRange = range.split(":");
            final int bottomRange = Integer.parseInt(parsedRange[0]);
            //final int upperRange = Integer.parseInt(parsedRange[1]);
            corners[j] = bottomRange - 1;
        }

        return corners;
    }

    public int[] getLengths(final int[] dimensions) {
        final String[] ranges = section.split(",");
        final int cutoutDimensionCount = ranges.length;
        final int[] lengths = Arrays.copyOf(dimensions, dimensions.length);

        for (int i = (cutoutDimensionCount - 1), j = 0; (i >= 0) && (j < cutoutDimensionCount); i--, j++) {
            final String range = ranges[i];
            final String[] parsedRange = range.split(":");
            final int bottomRange = Integer.parseInt(parsedRange[0]);
            final int upperRange = Integer.parseInt(parsedRange[1]);
            lengths[j] = (upperRange - bottomRange) + 1;
        }

        return lengths;
    }

    public int[] getOffsets()
    {
        return offsets;
    }

}

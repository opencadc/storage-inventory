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
************************************************************************
*/

package org.opencadc.critwall;

import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.StringUtil;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;

/**
 *
 * @author pdowler
 */
public class BucketSelector {
    private static final Logger log = Logger.getLogger(BucketSelector.class);
    private final int min;
    private final int max;
    private final String hexMin;
    private final String hexMax;
    private final String hexBuffer = "000";

    public BucketSelector(String selectors) {
        InventoryUtil.assertNotNull(BucketSelector.class, "selectors", selectors);

        // For first iteration, selectors can only be a range
        // Check that selector range only uses 0..15
        String[] minMax = selectors.split(",");

        try {
            hexMin = StringUtil.trimLeadingWhitespace(minMax[0]);
            hexMax = StringUtil.trimLeadingWhitespace(minMax[1]);
            min = HexUtil.toShort(hexBuffer + hexMin);
            max = HexUtil.toShort(hexBuffer +hexMax);

            log.debug("values: " + hexMax + " " + hexMin
                + " " + min + " "  + max);
        } catch (Exception e ){
            log.debug("min or max toShort failed" + e);
            throw new IllegalArgumentException("invalid number format: " + selectors, e);
        }


        // If properties file will be integer??
        //        try {
        //            min = Integer.parseInt(StringUtil.trimLeadingWhitespace(minMax[0]));
        //            max = Integer.parseInt(StringUtil.trimLeadingWhitespace(minMax[1]));
        //        } catch (NumberFormatException nfe) {
        //            throw new IllegalArgumentException("invalid number format: " + selectors, nfe);
        //        }

        //
        //        try {
        //            // HexUtil returns '00000000' need to get just the last character
        //            // or should these be byte?
        //            hexMin = HexUtil.toHex(min).substring(7,8);
        //            hexMax = HexUtil.toHex(max).substring(7,8);
        //        } catch (Exception e) {
        //            throw new IllegalArgumentException("min/max must be in range 0,15", e);
        //        }

        if (max < min || max > 15 || min < 0) {
            throw new IllegalArgumentException("invalid bucket selector range (min,max): " +
                min + "," + max);
        }


    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public String getHexMin() {
        return hexMin;
    }

    public String getHexMax() {
        return hexMax;
    }

}


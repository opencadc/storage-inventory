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
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;


public class BucketSelector {
    private static final Logger log = Logger.getLogger(BucketSelector.class);
    // values as entered in .properties file
    private final String bucketSelectors;
    private final String rangeMin;
    private final String rangeMax;
    // generated list of bucket selectors
    private TreeSet<String> bucketList = new TreeSet<String>();

    public Iterator<String> getBucketIterator() {
        return bucketList.iterator();
    }

    public BucketSelector(String selectors) {
        InventoryUtil.assertNotNull(BucketSelector.class, "selectors", selectors);
        this.bucketSelectors = selectors;

        String[] minMax = this.bucketSelectors.split("-");
        int min;
        int max;

        if (minMax.length > 2) {
            throw new IllegalArgumentException("invalid bucket selector: single value or range only: "
                + bucketSelectors);
        } else {
            try {
                // trim and convert to lower case for consistent processing
                rangeMin = StringUtil.trimTrailingWhitespace(StringUtil.trimLeadingWhitespace(minMax[0])).toLowerCase();

                if (StringUtil.hasLength(rangeMin) && rangeMin.length() < 6) {
                    String padded = "0000".substring(rangeMin.length()) + rangeMin;
                    // check quality of entry, convert to int for generating full range
                    min = HexUtil.toShort(HexUtil.toBytes(padded));
                    log.debug("here" + min);
                } else {
                    throw new IllegalArgumentException("invalid value in range: " + rangeMin);
                }

                if (minMax.length == 1) {
                    rangeMax = rangeMin;
                    max = min;
                } else {
                    rangeMax = StringUtil.trimTrailingWhitespace(StringUtil.trimLeadingWhitespace(minMax[1])).toLowerCase();
                    String padded = "0000".substring(rangeMax.length()) + rangeMax;
                    // check quality of entry, convert to int for generating full range
                    max = HexUtil.toShort(HexUtil.toBytes(padded));
                }
                log.debug("range values as ints: " + min + "-" + max);

                // 0-f is acceptable range
                if (min < 0 || max < min || max > 15) {
                    throw new IllegalArgumentException("invalid bucket selector (min,max): " + min + "," + max);
                }

                for (int i = min; i <= max; i++) {
                    // add values to the TreeSet, as hex strings
                    // TreeSet should ensure no duplicates
                    bucketList.add(HexUtil.toHex(i).replaceFirst("^0+(?!$)", ""));
                    log.debug("added " + HexUtil.toHex(i).replaceFirst("^0+(?!$)", ""));
                }

            } catch (Exception e) {
                log.debug("error processing range: " + this.bucketSelectors);
                throw new IllegalArgumentException("invalid format: " + this.bucketSelectors + "\n");
            }
        }
    }
}


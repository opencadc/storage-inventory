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

import ca.nrc.cadc.util.StringUtil;
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;


public class BucketSelector {
    private static final Logger log = Logger.getLogger(BucketSelector.class);

    private static final int MAX_PREFIX_LENGTH = 1;
    private final String rangeMin;
    private final String rangeMax;
    private final TreeSet<String> bucketList = new TreeSet<String>();

    public Iterator<String> getBucketIterator() {
        return bucketList.iterator();
    }

    public BucketSelector(String selector) {
        InventoryUtil.assertNotNull(BucketSelector.class, "selectors", selector);


        String[] minMax = selector.split("-");
        StringBuilder errMsg = new StringBuilder();
        int min;
        int max;

        if (minMax.length > 2) {
            throw new IllegalArgumentException("invalid bucket selector: single value or range only: "
                + selector);
        }

        // trim and convert to lower case for consistent processing
        rangeMin = minMax[0].trim().toLowerCase();

        if (minMax.length == 1) {
            rangeMax = rangeMin;
        } else {
            rangeMax = minMax[1].trim().toLowerCase();
        }

        if (!StringUtil.hasLength(rangeMin)) {
            errMsg.append("empty min range value: " + rangeMin + "\n");
        }
        if (!StringUtil.hasLength(rangeMax)) {
            errMsg.append("empty max range value: " + rangeMax + "\n");
        }

        if (rangeMin.length() > MAX_PREFIX_LENGTH) {
            errMsg.append("min range value greater than maxlen (" + MAX_PREFIX_LENGTH +"): " + rangeMin + "\n");
        }
        if (rangeMax.length() > MAX_PREFIX_LENGTH) {
            errMsg.append("max range value greater than maxlen (" + MAX_PREFIX_LENGTH +"): " + rangeMax + "\n");
        }

        // 0-f is acceptable range
        min = Artifact.HEXVALUES.indexOf(rangeMin);
        if (min == -1) {
            errMsg.append("invalid hex value: " + rangeMin + "\n");
        }
        max = Artifact.HEXVALUES.indexOf(rangeMax);
        if (max == -1) {
            errMsg.append("invalid hex value: " + rangeMax + "\n");
        }

        // order of range must be sane
        if ( max != -1 && max < min ) {
            errMsg.append("invalid range (min,max): " + rangeMin + ", " + rangeMax + "\n");
        }

        log.debug("range values as ints: " + min + ", " + max);

        if (errMsg.length() != 0) {
            throw new IllegalArgumentException("error creating BucketSelector: " + errMsg);
        }

        // Populate the bucketList that the iterator will be based on
        for (int i = min; i <= max; i++) {
            bucketList.add(Character.toString(Artifact.HEXVALUES.charAt(i)));
            log.debug("added " + Character.toString(Artifact.HEXVALUES.charAt(i)));
        }
    }
}


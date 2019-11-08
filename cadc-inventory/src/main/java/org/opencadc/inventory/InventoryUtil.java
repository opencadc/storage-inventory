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

package org.opencadc.inventory;

import ca.nrc.cadc.util.HexUtil;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Date;
import java.util.UUID;
import org.apache.log4j.Logger;

/**
 * Static utility methods.
 * 
 * @author pdowler
 */
public abstract class InventoryUtil {
    private static final Logger log = Logger.getLogger(InventoryUtil.class);

    private InventoryUtil() { 
    }

    /**
     * Assign last modified timestamp to an entity. This method is to support
     * persisting/serialising and reconstructing/deserialising an entity.
     * 
     * @param ce the entity
     * @param d the timestamp
     */
    public static void assignLastModified(Entity ce, Date d) {
        try {
            Field f = Entity.class.getDeclaredField("lastModified");
            f.setAccessible(true);
            f.set(ce, d);
        } catch (NoSuchFieldException fex) {
            throw new RuntimeException("BUG", fex);
        } catch (IllegalAccessException bug) {
            throw new RuntimeException("BUG", bug);
        }
    }

    /**
     * Assign metaChecksum URI to an entity. This method is to support
     * persisting/serialising and reconstructing/deserialising an entity.
     * 
     * @param ce the entity
     * @param u the URI
     */
    public static void assignMetaChecksum(Entity ce, URI u) {
        try {
            Field f = Entity.class.getDeclaredField("metaChecksum");
            f.setAccessible(true);
            f.set(ce, u);
        } catch (NoSuchFieldException fex) {
            throw new RuntimeException("BUG", fex);
        } catch (IllegalAccessException bug) {
            throw new RuntimeException("BUG", bug);
        }
    }
    
    /**
     * Utility method so constructors can validate arguments.
     * 
     * @param caller class doing test
     * @param name field name being checked
     * @param test object to test
     * @throws IllegalArgumentException if the value is invalid
     */
    public static void assertNotNull(Class caller, String name, Object test)
            throws IllegalArgumentException {
        if (test == null) {
            throw new IllegalArgumentException("invalid " + caller.getSimpleName() + "." + name + ": null");
        }
    }
    
    /**
     * A valid path component has no space ( ), slash (/), escape (\), or
     * percent (%) characters.
     * 
     * @param caller class doing test
     * @param name field name being checked
     * @param test object to test
     * @throws IllegalArgumentException if the value is invalid 
     */
    public static void assertValidPathComponent(Class caller, String name, String test) {
        assertNotNull(caller, name, test);
        boolean space = (test.indexOf(' ') >= 0);
        boolean slash = (test.indexOf('/') >= 0);
        boolean escape = (test.indexOf('\\') >= 0);
        boolean percent = (test.indexOf('%') >= 0);

        if (space || slash || escape || percent) {
            throw new IllegalArgumentException("invalid " + caller.getSimpleName() + "." + name + ": " 
                    + test + "reason: value may not contain space ( ), slash (/), escape (\\), or percent (%)");
        }
    }
    
    /**
     * Checksum URI validation.
     * 
     * @param caller class doing test
     * @param name field name being checked
     * @param uri URI to test
     * @throws IllegalArgumentException if the value is invalid 
     */
    public static void assertValidChecksumURI(Class caller, String name, URI uri) {
        String scheme = uri.getScheme();
        String sval = uri.getSchemeSpecificPart();
        if (scheme == null || sval == null) {
            throw new IllegalArgumentException("invalid " + caller.getSimpleName() + "." + name + ": " 
                + uri + "reason: expected <algorithm>:<hex value>");
        }
        try {
            byte[] b = HexUtil.toBytes(sval); // TODO: could check algorithm vs length here
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("invalid " + caller.getSimpleName() + "." + name + ": " 
                + uri + " contains invalid hex chars -- expected <algorithm>:<hex value>");
        }
    }
}

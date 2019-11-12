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

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.HexUtil;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.log4j.Logger;

/**
 * Base class for storage inventory entities.
 * 
 * @author pdowler
 */
public abstract class Entity {
    private static final Logger log = Logger.getLogger(Entity.class);

    private static final boolean MCS_DEBUG = false;
    
    private UUID id;
    private Date lastModified;
    private URI metaChecksum;
    
    protected Entity() {
        this.id = UUID.randomUUID();
    }
    
    protected Entity(UUID id) {
        InventoryUtil.assertNotNull(Entity.class, "id", id);
        this.id = id;
    }

    public UUID getID() {
        return id;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public URI getMetaChecksum() {
        return metaChecksum;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName());
        sb.append("[");
        sb.append(id).append(",").append(metaChecksum).append(",");
        if (lastModified != null) {
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            sb.append(df.format(lastModified));
        } else {
            sb.append(lastModified); // null
        }
        sb.append("]");
        return sb.toString();
    }
    
    /**
     * Compute the current checksum of this entity.
     * 
     * @param digest checksum/digest implementation to use
     * @return checksum of the form {algorithm}:{hexadecimal value}
     */
    public URI computeMetaChecksum(MessageDigest digest) {
        try {
            calcMetaChecksum(this.getClass(), this, digest);
            byte[] metaChecksumBytes = digest.digest();
            String hexMetaChecksum = HexUtil.toHex(metaChecksumBytes);
            String alg = digest.getAlgorithm().toLowerCase();
            return new URI(alg, hexMetaChecksum, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to create metadata checksum URI for "
                + this.getClass().getName(), e);
        }
    }
    
    private void calcMetaChecksum(Class c, Object o, MessageDigest digest) {
        // calculation order:
        // 1. Entity.id for entities
        // 2. Entity.lastModified?? TBD
        // 3. state fields in alphabetic order; depth-first recursion
        // value handling:
        // Date: truncate time to whole number of seconds and treat as a long
        // String: UTF-8 encoded bytes
        // URI: UTF-8 encoded bytes of string representation
        // float: IEEE754 single (4 bytes)
        // double: IEEE754 double (8 bytes)
        // boolean: convert to single byte, false=0, true=1 (1 bytes)
        // byte: as-is (1 byte)
        // short: (2 bytes, network byte order == big endian))
        // integer: (4 bytes, network byte order == big endian)
        // long: (8 bytes, network byte order == big endian)
        try {
            if (o instanceof Entity) {
                Entity ce = (Entity) o;
                digest.update(primitiveValueToBytes(ce.id, "Entity.id", digest.getAlgorithm()));
                if (MCS_DEBUG) {
                    log.debug("metaChecksum: " + ce.getClass().getSimpleName() + ".id " + ce.id);
                }
                // TBD: include lastModified in metaChecksum: CAOM does not but it only supports one-direction
                // harvesting and always copies lastModified from origin
            }

            SortedSet<Field> fields = getStateFields(c);
            for (Field f : fields) {
                String cf = c.getSimpleName() + "." + f.getName();
                f.setAccessible(true);
                Object fo = f.get(o);
                if (fo != null) {
                    digest.update(primitiveValueToBytes(fo, cf, digest.getAlgorithm()));
                } else if (MCS_DEBUG) {
                    log.debug("skip null: " + cf);
                }
            }

        } catch (IllegalAccessException bug) {
            throw new RuntimeException("Unable to calculate metaChecksum for class " + c.getName(), bug);
        }
    }
    
    private SortedSet<Field> getStateFields(Class c)
            throws IllegalAccessException {
        SortedSet<Field> ret = new TreeSet<>(new FieldComparator());
        Field[] fields = c.getDeclaredFields();
        for (Field f : fields) {
            int m = f.getModifiers();
            boolean inc = true;
            inc = inc && !Modifier.isTransient(m);
            inc = inc && !Modifier.isStatic(m);
            // no child entities in storage inventory model
            //inc = inc && !isChildCollection(f); // 0..* relations to other Entity
            //inc = inc && !isChildEntity(f); // 0..1 relation to other Entity
            if (inc) {
                ret.add(f);
            }
        }
        Class sc = c.getSuperclass();
        while (sc != null && !Entity.class.equals(sc)) {
            ret.addAll(getStateFields(sc));
            sc = sc.getSuperclass();
        }
        return ret;
    }

    // used by File
    protected byte[] primitiveValueToBytes(Object o, String name, String digestAlg) {
        byte[] ret = null;
        if (o instanceof Byte) {
            ret = HexUtil.toBytes((Byte) o); // auto-unbox
        } else if (o instanceof Short) {
            ret = HexUtil.toBytes((Short) o); // auto-unbox
        } else if (o instanceof Integer) {
            ret = HexUtil.toBytes((Integer) o); // auto-unbox
        } else if (o instanceof Long) {
            ret = HexUtil.toBytes((Long) o); // auto-unbox
        } else if (o instanceof Boolean) {
            Boolean b = (Boolean) o;
            if (b) {
                ret = HexUtil.toBytes((byte) 1);
            } else {
                ret = HexUtil.toBytes((byte) 0);
            }
        } else if (o instanceof Date) {
            Date date = (Date) o;
            //long sec = (date.getTime() / 1000L); // seconds: CAOM did this because some DBs cannot round trip milliseconds
            long sec = date.getTime(); // milliseconds
            ret = HexUtil.toBytes(sec);
        } else if (o instanceof Float) {
            ret = HexUtil.toBytes(Float.floatToIntBits((Float) o)); /* auto-unbox, IEEE754 float */
        } else if (o instanceof Double) {
            ret = HexUtil.toBytes(Double.doubleToLongBits((Double) o)); /* auto-unbox, IEEE754 double */
        } else if (o instanceof String) {
            try {
                ret = ((String) o).trim().getBytes("UTF-8");
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException("BUG: failed to encode String in UTF-8", ex);
            }
        } else if (o instanceof URI) {
            try {
                ret = ((URI) o).toASCIIString().trim().getBytes("UTF-8");
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException("BUG: failed to encode String in UTF-8", ex);
            }
        } else if (o instanceof UUID) {
            UUID uuid = (UUID) o;
            byte[] msb = HexUtil.toBytes(uuid.getMostSignificantBits());
            byte[] lsb = HexUtil.toBytes(uuid.getLeastSignificantBits());
            ret = new byte[16];
            System.arraycopy(msb, 0, ret, 0, 8);
            System.arraycopy(lsb, 0, ret, 8, 8);
        }

        if (ret != null) {
            if (MCS_DEBUG) {
                try {
                    MessageDigest md  = MessageDigest.getInstance(digestAlg);
                    byte[] dig = md.digest(ret);
                    log.debug(o.getClass().getSimpleName() + " " + name + " = " + o.toString()
                        + " -- " + HexUtil.toHex(dig));
                } catch (Exception ignore) {
                    log.debug("OOPS", ignore);
                }
            }
            return ret;
        }

        throw new UnsupportedOperationException(
                "unexpected primitive/value type: " + o.getClass().getName());
    }

    private class FieldComparator implements Comparator<Field> {
        @Override
        public int compare(Field o1, Field o2) {
            return o1.getName().compareTo(o2.getName());
        }
    }
}

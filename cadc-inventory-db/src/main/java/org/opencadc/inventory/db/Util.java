/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2017.                            (c) 2017.
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
 *  $Revision: 5 $
 *
 ************************************************************************
 */

package org.opencadc.inventory.db;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.HexUtil;
import java.net.URI;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.vospace.NodeProperty;

/**
 *
 * @author pdowler
 */
public class Util {
    private static Logger log = Logger.getLogger(Util.class);

    public static String safeGetClassName(Object o) {
        if (o == null) {
            return null;
        }
        return o.getClass().getName();
    }

    public static String formatSQL(String[] sql) {
        StringBuilder sb = new StringBuilder();
        for (String s : sql) {
            sb.append("\n");
            sb.append(formatSQL(s));
        }
        return sb.toString();
    }

    public static String formatSQL(String sql) {
        sql = sql.replaceAll("SELECT ", "\nSELECT ");
        sql = sql.replaceAll("FROM ", "\nFROM ");
        sql = sql.replaceAll("LEFT ", "\n  LEFT ");
        sql = sql.replaceAll("RIGHT ", "\n  RIGHT ");
        sql = sql.replaceAll("WHERE ", "\nWHERE ");
        sql = sql.replaceAll("AND ", "\n  AND ");
        sql = sql.replaceAll("OR ", "\n  OR ");
        sql = sql.replaceAll("ORDER", "\nORDER");
        sql = sql.replaceAll("GROUP ", "\nGROUP ");
        sql = sql.replaceAll("HAVING ", "\nHAVING ");
        sql = sql.replaceAll("UNION ", "\nUNION ");

        // note: \\s* matches one or more whitespace chars
        //sql = sql.replaceAll("OUTER JOIN", "\n  OUTER JOIN");
        return sql;
    }
    
    public static String escapeChar(String s, char p) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == p) {
                sb.append(c); // an extra one
            }
            sb.append(c);
        }
        return sb.toString();
    }

    public static String replaceAll(String s, char p, char r) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == p) {
                sb.append(r);
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public static URI getURI(ResultSet rs, int col)
            throws SQLException {
        String o = rs.getString(col);
        if (o == null) {
            return null;
        }
        try {
            return new URI((String) o);
        } catch (Throwable t) {
            throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to URI", t);
        }
        //throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to URI");
    }

    public static Object getObject(ResultSet rs, int col)
            throws SQLException {
        Object obj = rs.getObject(col);
        if (obj == null) {
            return null;
        }
        if (obj instanceof Date) {
            return getDate(rs, col, null);
        }
        return obj;
    }

    public static Boolean getBoolean(ResultSet rs, int col)
            throws SQLException {
        Object o = rs.getObject(col);
        if (o == null) {
            return null;
        }

        // try boolean
        if (o instanceof Boolean) {
            return (Boolean) o;
        }

        // try integer
        Integer i = null;
        if (o instanceof Integer) {
            i = (Integer) o;

        } else if (o instanceof Number) {
            i = ((Number) o).intValue();
        }
        if (i != null) {
            if (i == 0) {
                return Boolean.FALSE;
            }
            if (i == 1) {
                return Boolean.TRUE;
            }
        }

        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Boolean");
    }

    public static Integer getInteger(ResultSet rs, int col)
            throws SQLException {
        Object o = rs.getObject(col);
        if (o == null) {
            return null;
        }
        if (o instanceof Integer) {
            return (Integer) o;
        }
        if (o instanceof Number) {
            return ((Number) o).intValue();
        }
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Integer");
    }

    public static Long getLong(ResultSet rs, int col)
            throws SQLException {
        Object o = rs.getObject(col);
        if (o == null) {
            return null;
        }
        if (o instanceof Long) {
            return (Long) o;
        }
        if (o instanceof Number) {
            return ((Number) o).longValue();
        }
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Long");
    }

    private static UUID objectToUUID(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof UUID) {
            return (UUID) o;
        }
        if (o instanceof Long) {
            return new UUID(0L, (Long) o);
        }
        if (o instanceof Number) {
            return new UUID(0L, ((Number) o).longValue());
        }
        if (o instanceof byte[]) {
            byte[] b = (byte[]) o;
            if (b.length < 16) {
                // sybase truncates trailing 0s
                byte[] bb = new byte[16];
                System.arraycopy(b, 0, bb, 0, b.length);
                b = bb;
            }
            long msb = HexUtil.toLong(b, 0);
            long lsb = HexUtil.toLong(b, 8);

            return new UUID(msb, lsb);
        }
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to UUID");
    }
  
    public static UUID getUUID(ResultSet rs, int col)
            throws SQLException {
        Object o = rs.getObject(col);
        return objectToUUID(o);
    }
    
    public static UUID[] getUUIDArray(ResultSet rs, int col)
            throws SQLException {
        Object o = rs.getObject(col);
        if (o == null) {
            return null;
        }
        if (o instanceof Array) {
            Array a = (Array) o;
            Object[] ao = (Object[]) a.getArray();
            UUID[] ret = new UUID[ao.length];
            for (int i = 0; i < ao.length; i++) {
                ret[i] = objectToUUID(ao[i]);
            }
            return ret;
        }
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to UUID");
    }

    public static Float getFloat(ResultSet rs, int col)
            throws SQLException {
        Object o = rs.getObject(col);
        if (o == null) {
            return null;
        }
        if (o instanceof Float) {
            return (Float) o;
        }
        if (o instanceof Number) {
            return ((Number) o).floatValue();
        }
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Float");
    }

    public static Double getDouble(ResultSet rs, int col)
            throws SQLException {
        Object o = rs.getObject(col);
        return objectToDouble(o);
    }

    private static Double objectToDouble(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Double) {
            return (Double) o;
        }
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        }
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Double");
    }

    public static double[] getDoubleArray(ResultSet rs, int col)
            throws SQLException {
        Object o = rs.getObject(col);
        if (o == null) {
            return null;
        }
        if (o instanceof Array) {
            Array a = (Array) o;
            Object[] ao = (Object[]) a.getArray();
            double[] ret = new double[ao.length];
            for (int i = 0; i < ao.length; i++) {
                ret[i] = objectToDouble(ao[i]);
            }
            return ret;
        }
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to double[]");
    }

    // truncate to even number of seconds
    public static Date truncate(Date d) {
        if (d == null) {
            return null;
        }
        return new Date(1000L * (d.getTime() / 1000L));
    }

    // round to nearest second
    public static Date getRoundedDate(ResultSet rs, int col, Calendar cal)
            throws SQLException {
        Date ret = getDate(rs, col, cal);
        if (ret == null) {
            return null;
        }
        double t = (double) ret.getTime();
        t /= 1000.0;
        ret = new Date(1000L * Math.round(t));
        return ret;
    }

    public static Date getDate(ResultSet rs, int col, Calendar cal)
            throws SQLException {
        Object o = rs.getTimestamp(col, cal);
        return DateUtil.toDate(o);
    }

    public static byte[] getByteArray(ResultSet rs, int col)
            throws SQLException {
        Object o = rs.getObject(col);
        if (o == null) {
            return null;
        }
        if (o instanceof byte[]) {
            return (byte[]) o;
        }
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to byte[]");
    }

    public static void parseArrayURI(String val, Set<URI> dest) {
        // postgresql 1D array: {a,"b,c"}
        if (val == null || val.isEmpty()) {
            return;
        }
        // GroupURI names can contain alphanumeric,comma,dash,dot,underscore,~
        // PG quotes them if comma is present (eg in the group name)
        char delim = '"';
        int i = 0;
        int j = val.indexOf(delim);
        while (j != -1) {
            String token = val.substring(i, j);
            //log.warn("token: " + i + "," + j + " " + token);
            i = j + 1;
            j = val.indexOf(delim, i);
            
            handleToken(token, dest);
        }
        String token = val.substring(i);
        //log.warn("token: " + i + " " + token);
        handleToken(token, dest);
    }
    
    private static void handleToken(String token, Set<URI> dest) {
        if (token.startsWith("ivo://")) {
            dest.add(URI.create(token));
        } else {
            StringTokenizer st = new StringTokenizer(token, "{,}");
            while (st.hasMoreTokens()) {
                String s = st.nextToken();
                dest.add(URI.create(s));
            }
        }
    }
        
    public static void parseArrayProps(String val, Set<NodeProperty> dest) {
        // postgresql 2D array: {{a,b},{c,d}}
        if (val == null || val.isEmpty()) {
            return;
        }
        char open = '{';
        char close = '}';
        char quote = '"';
        int i = val.indexOf(open);
        int j = val.lastIndexOf(close);
        if (j > i) {
            val = val.substring(i + 1, j);
        }
        i = val.indexOf(open);
        j = val.indexOf(close, i + 1);
        int k = 0;
        while (i != -1 && j != -1 && k++ < 20) {
            String t1 = val.substring(i + 1, j);
            //log.warn("\tt1: " + i + "," + j + " " + t1);
            handleProp(t1, dest);
            
            if (i != -1 && j > 0) {
                i = val.indexOf(open, j);
                j = val.indexOf(close, i + 1);
                // look ahead for quotes
                int q = val.indexOf(quote, i);
                //log.warn("i=" + i + " j=" + j + " q=" + q);
                if (q != -1 && q < j) {
                    int cq = val.indexOf(quote, q + 1);
                    j = val.indexOf(close, cq);
                    //log.warn("\tcq=" + cq + " j=" + j);
                }
            }
        }
    }
    
    private static void handleProp(String token, Set<NodeProperty> dest) {
        int q = token.indexOf('"');
        int cq = -1;
        if (q == -1) {
            q = Integer.MAX_VALUE;
        } else {
            cq = token.indexOf('"', q + 1);
        }
        int c = token.indexOf(',');

        String key;
        int split = c;
        if (c < q) {
            // key
            key = token.substring(0, c);
        } else {
            // "key"
            key = token.substring(q + 1, cq);
            split = cq + 1;
        }
        //log.warn("\tkey: " + key);
        
        q = token.indexOf('"', split + 1);
        cq = -1;
        if (q == -1) {
            q = Integer.MAX_VALUE;
        } else {
            cq = token.indexOf('"', q + 1);
        }
        String val;
        if (token.length() < q) {
            val = token.substring(split + 1);
        } else {
            val = token.substring(q + 1, cq);
        }
        //log.warn("\tval: " + val);
        
        dest.add(new NodeProperty(URI.create(key), val));
    }
}

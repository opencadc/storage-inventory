/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2026.                            (c) 2026.
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

package org.opencadc.inventory.storage;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.mappers.JdbcMapUtil;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;
import org.apache.log4j.Logger;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public class StorageMetadataMapper {
    private static final Logger log = Logger.getLogger(StorageMetadataMapper.class);

    private static String[] COLUMNS = new String[] {
        "artifactURI", "contentChecksum", "contentLastModified", "contentLength",
        "storageLocation_storageBucket", "storageLocation_storageID" // 2 PKs last
    };

    private final Calendar utcCalendar = Calendar.getInstance(DateUtil.UTC);
    private final JdbcTemplate jdbc;
    private final String tableName;
    
    public StorageMetadataMapper(JdbcTemplate jdbc, String tableName) {
        this.jdbc = jdbc;
        this.tableName = tableName;
    }

    public StorageMetadata doGet(StorageLocation key) {
        GetEntity get = new GetEntity(key);
        return jdbc.query(get, new StorageMetadataExtractor());
    }
    
    public void doPut(StorageMetadata value) {
        PutEntity put = new PutEntity(value);
        jdbc.update(put);
    }

    public void doDelete(StorageLocation key) {
        DeleteEntity del = new DeleteEntity(key);
        jdbc.update(del);
    }
    
    // usable by extractor and iterator
    private class StorageMetadataRowMapper implements RowMapper<StorageMetadata> {
        @Override
        public StorageMetadata mapRow(ResultSet rs, int i) throws SQLException {
            int col = 1;
            URI auri = JdbcMapUtil.getURI(rs, col++);
            URI cs = JdbcMapUtil.getURI(rs, col++);
            Date clm = JdbcMapUtil.getDate(rs, col++, utcCalendar);
            Long clen = JdbcMapUtil.getLong(rs, col++);
            String bucket = rs.getString(col++);
            URI sid = JdbcMapUtil.getURI(rs, col++);
            StorageLocation loc = new StorageLocation(sid);
            loc.storageBucket = bucket;
            return new StorageMetadata(loc, auri, cs, clen, clm);
        }
    }
    
    private class StorageMetadataExtractor implements ResultSetExtractor<StorageMetadata> {
        private StorageMetadataRowMapper map = new StorageMetadataRowMapper();
        
        @Override
        public StorageMetadata extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (rs.next()) {
                return map.mapRow(rs, 1);
            }
            return null;
        }
    }

    private class GetEntity implements PreparedStatementCreator {
        private StorageLocation key;
        
        GetEntity(StorageLocation key) {
            this.key = key;
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(" ");
            for (int i = 0; i < COLUMNS.length; i++) {
                sb.append(COLUMNS[i]);
                if (i + 1 < COLUMNS.length) {
                    sb.append(",");
                }
            };
            sb.append(" FROM ").append(tableName);
            sb.append(" WHERE ").append(COLUMNS[4]).append(" = ?");
            sb.append(" AND ").append(COLUMNS[5]).append(" = ?");
            String sql = sb.toString();
            log.debug("SQL: " + sql);
            
            PreparedStatement ret = conn.prepareStatement(sql);
            int col = 1;
            
            safeSetString(ret, col++, key.storageBucket);
            safeSetString(ret, col++, key.getStorageID());
            return ret;
        }
    }

    private class PutEntity implements PreparedStatementCreator {
        private StorageMetadata value;
        
        PutEntity(StorageMetadata value) {
            this.value = value;
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ").append(tableName).append(" (");
            for (int i = 0; i < COLUMNS.length; i++) {
                sb.append(COLUMNS[i]);
                if (i + 1 < COLUMNS.length) {
                    sb.append(",");
                }
            }
            sb.append(") VALUES (");
            for (int i = 0; i < COLUMNS.length; i++) {
                sb.append("?");
                if (i + 1 < COLUMNS.length) {
                    sb.append(",");
                }
            };
            sb.append(")");
            String sql = sb.toString();
            log.debug("SQL: " + sql);
            
            PreparedStatement ret = conn.prepareStatement(sql);
            int col = 1;
            safeSetString(ret, col++, value.getArtifactURI());
            safeSetString(ret, col++, value.getContentChecksum());
            safeSetTimestamp(ret, col++, new Timestamp(value.getContentLastModified().getTime()), utcCalendar);
            safeSetLong(ret, col++, value.getContentLength());
            safeSetString(ret, col++, value.getStorageLocation().storageBucket);
            safeSetString(ret, col++, value.getStorageLocation().getStorageID());
            return ret;
        }
    }
    
    private class DeleteEntity implements PreparedStatementCreator {
        private StorageLocation key;
        
        DeleteEntity(StorageLocation key) {
            this.key = key;
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("DELETE FROM ").append(tableName);
            sb.append(" WHERE ").append(COLUMNS[4]).append(" = ?");
            sb.append(" AND ").append(COLUMNS[5]).append(" = ?");
            String sql = sb.toString();
            log.debug("SQL: " + sql);
            
            PreparedStatement ret = conn.prepareStatement(sql);
            int col = 1;
            
            safeSetString(ret, col++, key.storageBucket);
            safeSetString(ret, col++, key.getStorageID());
            return ret;
        }
    }

    // TODO: collect these util methods together someplace re-usable
    private void safeSetString(PreparedStatement prep, int col, URI value) throws SQLException {
        String v = null;
        if (value != null) {
            v = value.toASCIIString();
        }
        safeSetString(prep, col, v);
    }

    private void safeSetString(PreparedStatement prep, int col, String value) throws SQLException {
        log.debug("safeSetString: " + col + " " + value);
        if (value != null) {
            prep.setString(col, value);
        } else {
            prep.setNull(col, Types.VARCHAR);
        }
    }
    
    private void safeSetLong(PreparedStatement prep, int col, Long value) throws SQLException {
        log.debug("safeSetLong: " + col + " " + value);
        if (value != null) {
            prep.setLong(col, value);
        } else {
            prep.setNull(col, Types.BIGINT);
        }
    }
    
    private void safeSetTimestamp(PreparedStatement prep, int col, Timestamp value, Calendar cal) throws SQLException {
        log.debug("safeSetTimestamp: " + col + " " + value);
        if (value != null) {
            prep.setTimestamp(col, value, cal);
        } else {
            prep.setNull(col, Types.TIMESTAMP);
        }
    }

}

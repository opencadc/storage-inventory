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

package org.opencadc.inventory.db;

import ca.nrc.cadc.date.DateUtil;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.Entity;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.StorageSite;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public class SQLGenerator {
    private static final Logger log = Logger.getLogger(SQLGenerator.class);

    private final Map<Class,String> tableMap = new TreeMap<Class,String>(new ClassComp());
    private final Map<Class,String[]> columnMap = new TreeMap<Class,String[]>(new ClassComp());
    
    protected final String database; // currently not used in SQL
    protected final String schema; // may be null
    
    /**
     * Constructor. The database name is currently not used in any generated SQL; code assumes
     * that the DataSource is connected to the right database already and cross-database statements
     * are not supported. The schema name is used to qualify table names (if set). The optional table
     * name prefix is pre-pended to the table name (after the optional {schema}.) and is provided as
     * a work around in shared (usually test) databases where schema name is a username and table 
     * names may collide with other content. Normal "production" use would specify the schema only.
     * 
     * @param database database name (may be null)
     * @param schema schema name (may be null)
     */
    public SQLGenerator(String database, String schema) { 
        this.database = database;
        this.schema = schema;
        init();
    }
    
    protected void init() {
        String pref = "";
        if (schema != null) {
            pref = schema + ".";
        }
        this.tableMap.put(Artifact.class, pref + Artifact.class.getSimpleName());
        this.tableMap.put(StorageSite.class, pref + StorageSite.class.getSimpleName());
        this.tableMap.put(DeletedArtifactEvent.class, pref + DeletedArtifactEvent.class.getSimpleName());
        this.tableMap.put(DeletedStorageLocationEvent.class, pref + DeletedStorageLocationEvent.class.getSimpleName());
        
        String[] cols = new String[] {
            "uri", // first column is logical key
            "uriBucket",
            "contentChecksum", "contentLastModified", "contentLength", "contentType", "contentEncoding",
            "siteLocations",
            "storageLocation_storageID",
            "storageLocation_storageBucket",
            "lastModified",
            "metaChecksum",
            "id" // last column is always PK
        };
        this.columnMap.put(Artifact.class, cols);
        
        cols = new String[] {
            "resourceID", // first column is logical key
            "name",
            "lastModified",
            "metaChecksum",
            "id" // last column is always PK
        };
        this.columnMap.put(StorageSite.class, cols);
        
        cols = new String[] {
            "lastModified",
            "metaChecksum",
            "id" // last column is always PK
        };
        this.columnMap.put(DeletedArtifactEvent.class, cols);
        this.columnMap.put(DeletedStorageLocationEvent.class, cols);
        this.columnMap.put(Entity.class, cols); // skeleton
    }
    
    private static class ClassComp implements Comparator<Class> {
        @Override
        public int compare(Class o1, Class o2) {
            Class c1 = (Class) o1;
            Class c2 = (Class) o2;
            return c1.getName().compareTo(c2.getName());
        }
    }
    
    public String getCurrentTimeSQL() {
        return "SELECT now()";
    }
    
    // test usage
    String getTable(Class c) {
        return tableMap.get(c);
    }
    
    public EntityGet getEntityGet(Class c) {
        if (Artifact.class.equals(c)) {
            return new ArtifactGet();
        }
        if (StorageSite.class.equals(c)) {
            return new StorageSiteGet();
        }
        if (DeletedArtifactEvent.class.equals(c)) {
            return new DeletedArtifactEventGet();
        }
        if (DeletedStorageLocationEvent.class.equals(c)) {
            return new DeletedStorageLocationEventGet();
        }
        throw new UnsupportedOperationException("entity-get: " + c.getName());
    }
    
    public EntityList getEntityList(Class c) {
        if (StorageSite.class.equals(c)) {
            return new StorageSiteList();
        }
        throw new UnsupportedOperationException("entity-list: " + c.getName());
    }
    
    public EntityLock getEntityLock(Class c) {
        if (Artifact.class.equals(c)) {
            return new EntityLockImpl(c);
        }
        throw new UnsupportedOperationException("entity-list: " + c.getName());
    }
    
    public EntityGet getSkeletonEntityGet(Class c) {
        EntityGet ret = new SkeletonGet(c);
        return ret;
    }

    public EntityPut getEntityPut(Class c, boolean update) {
        if (Artifact.class.equals(c)) {
            return new ArtifactPut(update);
        }
        if (StorageSite.class.equals(c)) {
            return new StorageSitePut(update);
        }
        if (DeletedArtifactEvent.class.equals(c)) {
            return new DeletedEventPut(update);
        }
        if (DeletedStorageLocationEvent.class.equals(c)) {
            return new DeletedEventPut(update);
        }
        throw new UnsupportedOperationException("entity-put: " + c.getName());
    }
    
    public EntityDelete getEntityDelete(Class c) {
        return new EntityDeleteImpl(c);
    }
    
    private class EntityLockImpl implements EntityLock<Entity> {
        private final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final Class entityClass;
        private UUID id;
        
        EntityLockImpl(Class entityClass) {
            this.entityClass = entityClass;
        }

        @Override
        public void setID(UUID id) {
            this.id = id;
        }
        
        @Override
        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = getLockSQL(entityClass);
            log.debug("EntityLockImpl: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            int col = 1;
            prep.setObject(col++, id);
            prep.setObject(col++, id);
            
            return prep;
        }
    }

    private class SkeletonGet implements EntityGet<Entity> {
        private UUID id;
        private final Class entityClass;
        
        SkeletonGet(Class entityClass) {
            this.entityClass = entityClass;
        }
        
        @Override
        public void setID(UUID id) {
            this.id = id;
        }

        @Override
        public void setURI(URI uri) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Entity execute(JdbcTemplate jdbc) {
            return (Entity) jdbc.query(this, new SkeletonEntityExtractor());
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = getSelectFromSQL(entityClass, true);
            sb.append(" WHERE ");
            String col = getKeyColumn(entityClass, true);
            sb.append(col).append(" = ?");
            String sql = sb.toString();
            log.debug("SkeletonGet: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            prep.setObject(1, id);
            return prep;
        }
    }
    
    private class DeletedArtifactEventGet extends SkeletonGet {
        DeletedArtifactEventGet() {
            super(DeletedArtifactEvent.class);
        }

        @Override
        public Entity execute(JdbcTemplate jdbc) {
            return (Entity) jdbc.query(this, new DeletedArtifactEventExtractor());
        }
    }
    
    private class DeletedStorageLocationEventGet extends SkeletonGet {
        DeletedStorageLocationEventGet() {
            super(DeletedStorageLocationEvent.class);
        }

        @Override
        public Entity execute(JdbcTemplate jdbc) {
            return (Entity) jdbc.query(this, new DeletedStorageLocationEventExtractor());
        }
    }
    
    private class ArtifactGet implements EntityGet<Artifact> {
        private UUID id;
        private URI uri;

        @Override
        public void setID(UUID id) {
            this.id = id;
        }

        @Override
        public void setURI(URI uri) {
            this.uri = uri;
        }

        @Override
        public Artifact execute(JdbcTemplate jdbc) {
            return (Artifact) jdbc.query(this, new ArtifactExtractor());
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = getSelectFromSQL(Artifact.class, false);
            sb.append(" WHERE ");
            if (id != null) {
                String col = getKeyColumn(Artifact.class, true);
                sb.append(col).append(" = ?");
            } else {
                String col = getKeyColumn(Artifact.class, false);
                sb.append(col).append(" = ?");
            }
            String sql = sb.toString();
            log.debug("ArtifactGet: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            if (id != null) {
                prep.setObject(1, id);
            } else {
                prep.setString(1, uri.toASCIIString());
            }
            return prep;
        }
    }
    
    private class StorageSiteGet implements EntityGet<StorageSite> {
        private UUID id;
        private URI uri;

        @Override
        public void setID(UUID id) {
            this.id = id;
        }

        @Override
        public void setURI(URI uri) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageSite execute(JdbcTemplate jdbc) {
            return (StorageSite) jdbc.query(this, new StorageSiteExtractor());
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = getSelectFromSQL(StorageSite.class, false);
            sb.append(" WHERE ");
            if (id != null) {
                String col = getKeyColumn(StorageSite.class, true);
                sb.append(col).append(" = ?");
            } else {
                String col = getKeyColumn(StorageSite.class, false);
                sb.append(col).append(" = ?");
            }
            String sql = sb.toString();
            log.debug("StorageSiteGet: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            if (id != null) {
                prep.setObject(1, id);
            } else {
                prep.setString(1, uri.toASCIIString());
            }
            return prep;
        }
    }

    private class StorageSiteList implements EntityList<StorageSite> {

        @Override
        public List<StorageSite> query(JdbcTemplate jdbc) {
            return (List<StorageSite>) jdbc.query(this, new StorageSiteRowMapper());
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = getSelectFromSQL(StorageSite.class, false);
            String sql = sb.toString();
            log.debug("StorageSiteList: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            return prep;
        }
    }
    
    private void safeSetString(PreparedStatement prep, int col, String value) throws SQLException {
        log.debug("safeSetString: " + col + " " + value);
        if (value != null) {
            prep.setString(col, value);
        } else {
            prep.setNull(col, Types.VARCHAR);
        }
    }
    
    private class ArtifactPut implements EntityPut<Artifact> {
        private final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final boolean update;
        private Artifact value;
        
        ArtifactPut(boolean update) {
            this.update = update;
        }

        @Override
        public void setValue(Artifact value) {
            this.value = value;
        }
        
        @Override
        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = null;
            if (update) {
                sql = getUpdateSQL(Artifact.class);
                       
            } else {
                sql = getInsertSQL(Artifact.class);
            }
            log.debug("ArtifactPut: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            int col = 1;
            prep.setString(col++, value.getURI().toASCIIString());
            prep.setString(col++, value.getBucket());
            prep.setString(col++, value.getContentChecksum().toASCIIString());
            prep.setTimestamp(col++, new Timestamp(value.getContentLastModified().getTime()), utc);
            prep.setLong(col++, value.getContentLength());
            safeSetString(prep, col++, value.contentType);
            safeSetString(prep, col++, value.contentEncoding);
            
            if (!value.siteLocations.isEmpty()) {
                UUID[] ua = new UUID[value.siteLocations.size()];
                for (int i = 0; i < value.siteLocations.size(); i++) {
                    ua[i] = value.siteLocations.get(i).getSiteID();
                }
                java.sql.Array arr = prep.getConnection().createArrayOf("uuid", ua);
                prep.setObject(col++, arr);
            } else {
                prep.setObject(col++, null); // siteLocations uuid[]
            }
            if (value.storageLocation != null) {
                prep.setString(col++, value.storageLocation.getStorageID().toASCIIString());
                safeSetString(prep, col++, value.storageLocation.storageBucket);
            } else {
                prep.setNull(col++, Types.VARCHAR); // storageLocation.storageID
                prep.setNull(col++, Types.VARCHAR); // storageLocation.storageBucket
            }
            
            prep.setTimestamp(col++, new Timestamp(value.getLastModified().getTime()), utc);
            prep.setString(col++, value.getMetaChecksum().toASCIIString());
            prep.setObject(col++, value.getID());
            
            return prep;
        }
        
    }

    private class StorageSitePut implements EntityPut<StorageSite> {
        private final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final boolean update;
        private StorageSite value;
        
        StorageSitePut(boolean update) {
            this.update = update;
        }

        @Override
        public void setValue(StorageSite value) {
            this.value = value;
        }
        
        @Override
        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = null;
            if (update) {
                sql = getUpdateSQL(StorageSite.class);
                       
            } else {
                sql = getInsertSQL(StorageSite.class);
            }
            log.debug("StorageSitePut: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            int col = 1;
            prep.setString(col++, value.getResourceID().toASCIIString());
            prep.setString(col++, value.getName());
            
            prep.setTimestamp(col++, new Timestamp(value.getLastModified().getTime()), utc);
            prep.setString(col++, value.getMetaChecksum().toASCIIString());
            prep.setObject(col++, value.getID());
            
            return prep;
        }
        
    }
    
    private class DeletedEventPut implements EntityPut<Entity> {
        private final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final boolean update;
        private Entity value;
        
        DeletedEventPut(boolean update) {
            this.update = update;
        }

        @Override
        public void setValue(Entity value) {
            this.value = value;
        }
        
        @Override
        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = null;
            if (update) {
                sql = getUpdateSQL(value.getClass());
                       
            } else {
                sql = getInsertSQL(value.getClass());
            }
            log.debug("DeletedEventPut: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            int col = 1;
            prep.setTimestamp(col++, new Timestamp(value.getLastModified().getTime()), utc);
            prep.setString(col++, value.getMetaChecksum().toASCIIString());
            prep.setObject(col++, value.getID());
            
            return prep;
        }
        
    }

    private class EntityDeleteImpl implements EntityDelete {
        private final Class entityClass;
        private UUID id;

        public EntityDeleteImpl(Class entityClass) {
            this.entityClass = entityClass;
        }

        @Override
        public void setID(UUID id) {
            this.id = id;
        }
        
        @Override
        public void execute(JdbcTemplate jdbc) {
            jdbc.update(this);
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = getDeleteSQL(entityClass);
            log.debug("EntityDeleteImpl: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            prep.setObject(1, id);
            return prep;
        }
    }
    
    private StringBuilder getSelectFromSQL(Class c, boolean entityCols) {
        String tab = tableMap.get(c);
        String[] cols = columnMap.get(c);
        if (entityCols) {
            cols = columnMap.get(Entity.class);
        }
        if (tab == null || cols == null) {
            throw new IllegalArgumentException("BUG: no table/columns for class " + c.getName());
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (int i = 0; i < cols.length; i++) {
            sb.append(cols[i]);
            if (i < cols.length - 1) {
                sb.append(",");
            }
        }
        sb.append(" FROM ");
        sb.append(tab);
        
        return sb;
    }
    
    private String getLockSQL(Class c) {
        StringBuilder sb = new StringBuilder();
        String pk = getKeyColumn(c, true);
        sb.append("UPDATE ");
        sb.append(tableMap.get(c));
        sb.append(" SET ").append(pk).append(" = ? WHERE ").append(pk).append(" = ?");
        return sb.toString();
    }
    
    private String getUpdateSQL(Class c) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(tableMap.get(c));
        sb.append(" SET ");
        String[] cols = columnMap.get(c);
        for (int i = 0; i < cols.length - 1; i++) { // PK is last
            if (i > 0) {
                sb.append(",");
            }
            sb.append(cols[i]);
            sb.append(" = ?");
        }
        sb.append(" WHERE ");
        sb.append(getKeyColumn(c, true));
        sb.append(" = ?");

        return sb.toString();
    }
    
    private String getInsertSQL(Class c) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(tableMap.get(c));
        sb.append(" (");
        String[] cols = columnMap.get(c);
        for (int i = 0; i < cols.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(cols[i]);
        }
        sb.append(" ) VALUES (");
        for (int i = 0; i < cols.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("?");
        }
        sb.append(")");

        return sb.toString();
    }
    
    private String getDeleteSQL(Class c) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        sb.append(tableMap.get(c));
        sb.append(" WHERE id = ?");
        return sb.toString();
    }
    
    private String getKeyColumn(Class c, boolean pk) {
        String[] cols = columnMap.get(c);
        if (cols == null) {
            throw new IllegalArgumentException("BUG: no table/columns for class " + c.getName());
        }
        if (pk) {
            return cols[cols.length - 1]; // last column is always PK
        }
        if (!(Artifact.class.equals(c) || StorageSite.class.equals(c))) {
            throw new IllegalArgumentException("BUG: no logical key column" + c.getClass().getSimpleName());
        }
        return cols[0]; // first column is logical key
    }
    
    public ResultSetExtractor getArtifactExtractor() {
        return new ArtifactExtractor();
    }
    
    private class SkeletonEntityExtractor implements ResultSetExtractor {

        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            int col = 1;
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            Entity ret = new SkeletonEntity(id);
            InventoryUtil.assignLastModified(ret, lastModified);
            InventoryUtil.assignMetaChecksum(ret, metaChecksum);
            return ret;
        }
    }
    
    private class ArtifactExtractor implements ResultSetExtractor {

        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            
            int col = 1;
            final URI uri = Util.getURI(rs, col++);
            col++; // uriBucket
            final URI contentChecksum = Util.getURI(rs, col++);
            final Date contentLastModified = Util.getDate(rs, col++, utc);
            final Long contentLength = Util.getLong(rs, col++);
            final String contentType = rs.getString(col++);
            final String contentEncoding = rs.getString(col++);
            final UUID[] siteLocs = Util.getUUIDArray(rs, col++);
            final URI storLoc = Util.getURI(rs, col++);
            final String storBucket = rs.getString(col++);
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            Artifact a = new Artifact(id, uri, contentChecksum, contentLastModified, contentLength);
            a.contentType = contentType;
            a.contentEncoding = contentEncoding;
            if (siteLocs != null && siteLocs.length > 0) {
                for (UUID s: siteLocs) {
                    a.siteLocations.add(new SiteLocation(s));
                }
            }
            log.debug("ArtifactExtractor: " + storLoc + " " + storBucket);
            if (storLoc != null) {
                a.storageLocation = new StorageLocation(storLoc);
                a.storageLocation.storageBucket = storBucket;
            }
            InventoryUtil.assignLastModified(a, lastModified);
            InventoryUtil.assignMetaChecksum(a, metaChecksum);
            return a;
        }
    }
    
    private class StorageSiteRowMapper implements RowMapper {
        Calendar utc = Calendar.getInstance(DateUtil.UTC);

        @Override
        public Object mapRow(ResultSet rs, int i) throws SQLException {
            int col = 1;
            final URI resourceID = Util.getURI(rs, col++);
            final String name = rs.getString(col++);
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            StorageSite s = new StorageSite(id, resourceID, name);
            InventoryUtil.assignLastModified(s, lastModified);
            InventoryUtil.assignMetaChecksum(s, metaChecksum);
            return s;
        }
        
    }
    
    private class StorageSiteExtractor implements ResultSetExtractor {
        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            StorageSiteRowMapper m = new StorageSiteRowMapper();
            return m.mapRow(rs, 1);
        }
    }
    
    private class DeletedArtifactEventExtractor implements ResultSetExtractor {

        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            int col = 1;
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            Entity ret = new DeletedArtifactEvent(id);
            InventoryUtil.assignLastModified(ret, lastModified);
            InventoryUtil.assignMetaChecksum(ret, metaChecksum);
            return ret;
        }
    }
    
    private class DeletedStorageLocationEventExtractor implements ResultSetExtractor {

        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            int col = 1;
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            Entity ret = new DeletedStorageLocationEvent(id);
            InventoryUtil.assignLastModified(ret, lastModified);
            InventoryUtil.assignMetaChecksum(ret, metaChecksum);
            return ret;
        }
    }
}

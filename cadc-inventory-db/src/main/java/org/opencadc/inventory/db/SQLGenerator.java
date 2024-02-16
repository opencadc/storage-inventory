/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.ObsoleteStorageLocation;
import org.opencadc.inventory.PreauthKeyPair;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.StorageLocationEvent;
import org.opencadc.inventory.StorageSite;
import org.opencadc.persist.Entity;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.DeletedNodeEvent;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeProperty;
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

    private final Map<Class,String> tableMap = new TreeMap<>(new ClassComp());
    private final Map<Class,String[]> columnMap = new TreeMap<>(new ClassComp());
    
    protected final String database; // currently not used in SQL
    protected final String invSchema;
    protected final String genSchema;
    protected final String vosSchema;
    
    /**
     * Constructor. The database name is currently not used in any generated SQL; code assumes
     * that the DataSource is connected to the right database already and cross-database statements
     * are not supported.
     * 
     * @param database database name: not used; future-proof
     * @param invSchema inventory schema name (required, implies genSchema = invSchema)
     * @param genSchema generic schema name for internal tables (PreauthKeys, HarvestState) - optional
     */
    public SQLGenerator(String database, String invSchema, String genSchema) { 
        this(database, invSchema, genSchema, null);
    }
    
    /**
     * Constructor. The database name is currently not used in any generated SQL; code assumes
     * that the DataSource is connected to the right database already and cross-database statements
     * are not supported. The genSchema is optional (required for HarvestState and PreauthKeyPair).
     * The vosSchema is optional (used for Node and DeletedNodeEvent).
     * 
     * @param database database name: not used; future-proof
     * @param invSchema inventory schema name - required
     * @param genSchema generic schema name - optional
     * @param vosSchema vospace schema name - optional
     */
    public SQLGenerator(String database, String invSchema, String genSchema, String vosSchema) { 
        this.database = database;
        InventoryUtil.assertNotNull(SQLGenerator.class, "invSchema", invSchema); // required for all uses
        this.invSchema = invSchema;
        InventoryUtil.assertNotNull(SQLGenerator.class, "genSchema", genSchema); // required for correct init
        this.genSchema = genSchema;
        this.vosSchema = vosSchema; // only required for vospace
        init();
    }
    
    protected void init() {
        // inventory model
        this.tableMap.put(Artifact.class, invSchema + "." + Artifact.class.getSimpleName());
        this.tableMap.put(StorageSite.class, invSchema + "." + StorageSite.class.getSimpleName());
        this.tableMap.put(DeletedArtifactEvent.class, invSchema + "." + DeletedArtifactEvent.class.getSimpleName());
        this.tableMap.put(DeletedStorageLocationEvent.class, invSchema + "." + DeletedStorageLocationEvent.class.getSimpleName());
        this.tableMap.put(StorageLocationEvent.class, invSchema + "." + StorageLocationEvent.class.getSimpleName());
        // internal
        this.tableMap.put(ObsoleteStorageLocation.class, invSchema + "." + ObsoleteStorageLocation.class.getSimpleName());
        
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
            "allowRead", 
            "allowWrite",
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
        this.columnMap.put(StorageLocationEvent.class, cols);
        
        this.columnMap.put(Entity.class, cols); // skeleton
        
        cols = new String[] {
            "location_storageID",
            "location_storageBucket",
            "lastModified",
            "metaChecksum",
            "id" // last column is always PK
        };
        this.columnMap.put(ObsoleteStorageLocation.class, cols);
        
        log.debug("genSchema: " + genSchema);
        if (genSchema != null) {
            // generic support
            this.tableMap.put(HarvestState.class, genSchema + "." + HarvestState.class.getSimpleName());
            this.tableMap.put(PreauthKeyPair.class, genSchema + "." + PreauthKeyPair.class.getSimpleName());
            cols = new String[] {
                "name",
                "resourceID",
                "curLastModified",
                "curID",
                "instanceID",
                "lastModified",
                "metaChecksum",
                "id" // last column is always PK
            };
            this.columnMap.put(HarvestState.class, cols);
        
            cols = new String[] {
                "name",
                "publicKey",
                "privateKey",
                "lastModified",
                "metaChecksum",
                "id" // last column is always PK
            };
            this.columnMap.put(PreauthKeyPair.class, cols);
        }
        
        // optional vospace
        log.debug("vosSchema: " + vosSchema);
        if (vosSchema != null) {
            tableMap.put(Node.class, vosSchema + "." + Node.class.getSimpleName());
            tableMap.put(DeletedNodeEvent.class, vosSchema + "." + DeletedNodeEvent.class.getSimpleName());
            
            cols = new String[] {
                "parentID",
                "name",
                "nodeType",
                "ownerID",
                "isPublic",
                "isLocked",
                "readOnlyGroups",
                "readWriteGroups",
                "properties",
                "inheritPermissions",
                "busy",
                "storageID",
                "target",
                "lastModified",
                "metaChecksum",
                "id" // last column is always PK
            };
            this.columnMap.put(Node.class, cols);
            
            cols = new String[] {
                "nodeType",
                "storageID",
                "lastModified",
                "metaChecksum",
                "id" // last column is always PK
            };
            this.columnMap.put(DeletedNodeEvent.class, cols);
        }
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
    
    public String getTable(Class c) {
        Class targetClass = c;
        String ret = tableMap.get(targetClass);
        if (ret == null) {
            // enable finding a common table that stores subclass instances
            targetClass = targetClass.getSuperclass();
            ret = tableMap.get(targetClass);
        }
        log.debug("table: " + c.getSimpleName() + " -> " + targetClass.getSimpleName() + " -> " + ret);
        return ret;
    }
    
    private String[] getColumns(Class c) {
        Class targetClass = c;
        String[] ret = columnMap.get(targetClass);
        if (ret == null) {
            // enable finding a common table that stores subclass instances
            targetClass = targetClass.getSuperclass();
            ret = columnMap.get(targetClass);
        }
        log.debug("columns: " + c.getSimpleName() + " -> " + targetClass.getSimpleName() + " -> " + (ret == null ? null : ret.length));
        return ret;
    }
    
    public EntityGet<? extends Entity> getEntityGet(Class c) {
        return getEntityGet(c, false);
    }
    
    public EntityGet<? extends Entity> getEntityGet(Class c, boolean forUpdate) {
        if (Artifact.class.equals(c)) {
            return new ArtifactGet(forUpdate);
        }
        if (StorageSite.class.equals(c)) {
            return new StorageSiteGet(forUpdate);
        }
        if (PreauthKeyPair.class.equals(c)) {
            return new KeyPairGet(forUpdate);
        }
        if (Node.class.equals(c)) {
            return new NodeGet(forUpdate);
        }
        
        if (forUpdate) {
            throw new UnsupportedOperationException("entity-get + forUpdate: " + c.getSimpleName());
        }
        
        // raw events are never locked for update
        if (DeletedArtifactEvent.class.equals(c)) {
            return new DeletedArtifactEventGet();
        }
        if (DeletedStorageLocationEvent.class.equals(c)) {
            return new DeletedStorageLocationEventGet();
        }
        if (StorageLocationEvent.class.equals(c)) {
            return new StorageLocationEventGet();
        }
        if (ObsoleteStorageLocation.class.equals(c)) {
            return new ObsoleteStorageLocationGet();
        }
        
        if (DeletedNodeEvent.class.equals(c)) {
            //return new DeletedNodeGet();
        }
        
        if (HarvestState.class.equals(c)) {
            return new HarvestStateGet();
        }
        
        
        
        throw new UnsupportedOperationException("entity-get: " + c.getName());
    }
    
    public NodeCount getNodeCount() {
        return new NodeCount();
    }
    
    public class NodeCount {
        private UUID id;
        
        public void setID(UUID id) {
            this.id = id;
        }
        
        public int execute(JdbcTemplate jdbc) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT count(*) FROM ").append(getTable(Node.class));
            sb.append(" WHERE parentID = '").append(id.toString()).append("'");
            String sql = sb.toString();
            log.debug("NodeCount: " + sql);
            int ret = jdbc.queryForObject(sql, Integer.class);
            return ret;
        }
    }
    
    public EntityIteratorQuery getEntityIteratorQuery(Class c) {
        if (Artifact.class.equals(c)) {
            return new ArtifactIteratorQuery();
        }
        if (Node.class.equals(c)) {
            return new NodeIteratorQuery();
        }
        throw new UnsupportedOperationException("entity-iterator: " + c.getName());
    }
    
    public EntityList getEntityList(Class c) {
        if (StorageSite.class.equals(c)) {
            return new StorageSiteList();
        }
        if (PreauthKeyPair.class.equals(c)) {
            return new KeyPairList();
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
            return new EntityEventPut(update);
        }
        if (DeletedStorageLocationEvent.class.equals(c)) {
            return new EntityEventPut(update);
        }
        if (StorageLocationEvent.class.equals(c)) {
            return new EntityEventPut(update);
        }
        if (ObsoleteStorageLocation.class.equals(c)) {
            return new ObsoleteStorageLocationPut(update);
        }
        if (HarvestState.class.equals(c)) {
            return new HarvestStatePut(update);
        }
        if (PreauthKeyPair.class.equals(c)) {
            return new KeyPairPut(update);
        }
        if (Node.class.isAssignableFrom(c)) {
            return new NodePut(update);
        }
        if (DeletedNodeEvent.class.equals(c)) {
            //return new DeletedNodePut(update);
        }
        throw new UnsupportedOperationException("entity-put: " + c.getName());
    }
    
    public EntityDelete getEntityDelete(Class c) {
        return new EntityDeleteImpl(c);
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
    
    private class StorageLocationEventGet extends SkeletonGet {
        StorageLocationEventGet() {
            super(StorageLocationEvent.class);
        }

        @Override
        public Entity execute(JdbcTemplate jdbc) {
            return (Entity) jdbc.query(this, new StorageLocationEventExtractor());
        }
    }
    
    // used directly in ObsoleteStorageLocationDAO
    class ObsoleteStorageLocationGet implements EntityGet<ObsoleteStorageLocation> {
        private UUID id;
        private StorageLocation loc;
       
        @Override
        public void setID(UUID id) {
            this.id = id;
        }

        public void setLocation(StorageLocation loc) {
            this.loc = loc;
        }
        
        @Override
        public ObsoleteStorageLocation execute(JdbcTemplate jdbc) {
            return jdbc.query(this, new ObsoleteStorageLocationExtractor());
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            if (id == null && loc == null) {
                throw new IllegalStateException("BUG: execute called with no value for ID or StorageLocation"); 
            }
            
            StringBuilder sb = getSelectFromSQL(ObsoleteStorageLocation.class, false);
            sb.append(" WHERE ");
            if (id != null) {
                String col = getKeyColumn(ObsoleteStorageLocation.class, true);
                sb.append(col).append(" = ?");
            } else {
                String[] cols = getColumns(ObsoleteStorageLocation.class);
                sb.append(cols[0]).append(" = ?");
                sb.append(" AND ");
                if (loc.storageBucket != null) {
                    sb.append(cols[1]).append(" = ?");
                } else {
                    sb.append(cols[1]).append(" IS NULL");
                }
            }
            String sql = sb.toString();
            log.debug("DeletedStorageLocationGet: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            if (id != null) {
                prep.setObject(1, id);
            } else {
                prep.setString(1, loc.getStorageID().toASCIIString());
                if (loc.storageBucket != null) {
                    prep.setString(2, loc.storageBucket);
                }
            }
            return prep;
        }
    }
    
    // used directly in HarvestStateDAO
    class HarvestStateGet implements EntityGet<HarvestState> {
        private UUID id;
        private String name;
        private URI resourceID;
       
        @Override
        public void setID(UUID id) {
            this.id = id;
        }

        public void setSource(String name, URI resourceID) {
            this.name = name;
            this.resourceID = resourceID;
        }
        
        @Override
        public HarvestState execute(JdbcTemplate jdbc) {
            return (HarvestState) jdbc.query(this, new HarvestStateExtractor());
        }
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            if (id == null && (name == null || resourceID == null)) {
                throw new IllegalStateException("BUG: execute called with no value for ID or name/resourceID"); 
            }
            
            StringBuilder sb = getSelectFromSQL(HarvestState.class, false);
            sb.append(" WHERE ");
            if (id != null) {
                String col = getKeyColumn(HarvestState.class, true);
                sb.append(col).append(" = ?");
            } else {
                String[] cols = getColumns(HarvestState.class);
                sb.append(cols[0]).append(" = ?");
                sb.append(" AND ");
                sb.append(cols[1]).append(" = ?");
            }
            String sql = sb.toString();
            log.debug("HarvestStateGet: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            if (id != null) {
                prep.setObject(1, id);
            } else {
                prep.setString(1, name);
                prep.setString(2, resourceID.toASCIIString());
            }
            return prep;
        }
    }
    
    // used directly in ArtifactDAO
    class ArtifactGet implements EntityGet<Artifact> {
        private UUID id;
        private URI uri;
        private final boolean forUpdate;
        
        ArtifactGet(boolean forUpdate) {
            this.forUpdate = forUpdate;
        }
        
        @Override
        public void setID(UUID id) {
            this.id = id;
        }

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
            if (forUpdate) {
                sb.append(" FOR UPDATE");
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
    
    class ArtifactIteratorQuery implements EntityIteratorQuery<Artifact> {

        private Boolean storageLocationRequired;
        private String prefix;
        private UUID siteID;
        private String whereClause;
        private boolean ordered;

        public ArtifactIteratorQuery() {
        }

        /**
         * true: Artifact.storageLocation not null
         * null: all artifacts
         * false: Artifact.storageLocation is null
         * 
         * @param slr whether Artifact.storageLocation is required or not
         */
        public void setStorageLocationRequired(Boolean slr) {
            this.storageLocationRequired = slr;
        }
        
        public void setPrefix(String prefix) {
            if (StringUtil.hasText(prefix)) {
                this.prefix = prefix.trim();
            } else {
                this.prefix = null;
            }
        }

        public void setCriteria(String whereClause) {
            if (StringUtil.hasText(whereClause)) {
                this.whereClause = whereClause.trim();
            } else {
                this.whereClause = null;
            }
        }

        public void setOrderedOutput(boolean ordered) {
            this.ordered = ordered;
        }

        public void setSiteID(UUID siteID) {
            this.siteID = siteID;
        }
        
        @Override
        public ResourceIterator<Artifact> query(DataSource ds) {
            
            StringBuilder sb = getSelectFromSQL(Artifact.class, false);
            sb.append(" WHERE");
            
            if (storageLocationRequired != null && storageLocationRequired) {
                // ArtifactDAO.storedIterator
                if (StringUtil.hasText(prefix)) {
                    sb.append(" storageLocation_storageBucket LIKE ? AND");
                }
                sb.append(" storageLocation_storageID IS NOT NULL");
            
                // NOTE: StorageLocation.compare() specifies the correct order
                // null storageBucket to come after non-null
                // postgresql: the default order is equivalent to explicitly specifying ASC NULLS LAST
                // default behaviour may not be db-agnostic
                if (ordered) {
                    sb.append(" ORDER BY storageLocation_storageBucket, storageLocation_storageID");
                }
            } else if (storageLocationRequired != null && !storageLocationRequired) {
                // ArtifactDAO.unstoredIterator
                if (StringUtil.hasText(prefix)) {
                    sb.append(" uriBucket LIKE ? AND");
                }
                sb.append(" storageLocation_storageID IS NULL");
                if (ordered) {
                    sb.append(" ORDER BY uri");
                }
            } else if (siteID != null) {
                if (prefix != null && siteID != null) {
                    sb.append(" uriBucket LIKE ? AND ").append("siteLocations @> ARRAY[?]");
                } else {
                    sb.append(" siteLocations @> ARRAY[?]");
                }
                if (ordered) {
                    sb.append(" ORDER BY uri");
                }
            } else if (whereClause != null) {
                if (prefix != null && whereClause != null) {
                    sb.append(" uriBucket LIKE ? AND ( ").append(whereClause).append(" )");
                } else {
                    sb.append(" (").append(whereClause).append(" )");
                }
                if (ordered) {
                    sb.append(" ORDER BY uri");
                }
            } else if (prefix != null) {
                sb.append(" uriBucket LIKE ?");
                if (ordered) {
                    sb.append(" ORDER BY uri");
                }
            } else {
                // trim off " WHERE"
                sb.delete(sb.length() - 6, sb.length());
            }
            
            String sql = sb.toString();
            log.debug("sql: " + sql);
            
            try {
                Connection con = ds.getConnection();
                log.debug("ArtifactIterator: setAutoCommit(false)");
                con.setAutoCommit(false);
                // defaults for options: ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
                PreparedStatement ps = con.prepareStatement(sql);
                ps.setFetchSize(1000);
                ps.setFetchDirection(ResultSet.FETCH_FORWARD);
                int col = 1;
                if (prefix != null) {
                    String val = prefix + "%";
                    log.debug("bucket prefix: " + val);
                    ps.setString(col++, val);
                }
                if (siteID != null) {
                    log.debug("siteID: " + siteID);
                    ps.setObject(col++, siteID);
                }
                ResultSet rs = ps.executeQuery();
                
                return new ArtifactResultSetIterator(con, rs);
            } catch (SQLException ex) {
                throw new RuntimeException("BUG: artifact iterator query failed", ex);
            }
        }
        
    }
    
    private class StorageSiteGet implements EntityGet<StorageSite> {
        private UUID id;
        private final boolean forUpdate;

        public StorageSiteGet(boolean forUpdate) {
            this.forUpdate = forUpdate;
        }
        
        @Override
        public void setID(UUID id) {
            this.id = id;
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
                throw new IllegalStateException("primary key is null");
            }
            if (forUpdate) {
                sb.append(" FOR UPDATE");
            }
            String sql = sb.toString();
            log.debug("StorageSiteGet: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            prep.setObject(1, id);
            return prep;
        }
    }

    private class StorageSiteList implements EntityList<StorageSite> {

        @Override
        public Set<StorageSite> query(JdbcTemplate jdbc) {
            List<StorageSite> sites = (List<StorageSite>) jdbc.query(this, new StorageSiteRowMapper());
            Set<StorageSite> ret = new TreeSet<>();
            ret.addAll(sites);
            return ret;
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
    
    class KeyPairGet implements EntityGet<PreauthKeyPair> {
        private UUID id;
        private String name;
        private final boolean forUpdate;

        public KeyPairGet(boolean forUpdate) {
            this.forUpdate = forUpdate;
        }
        
        @Override
        public void setID(UUID id) {
            this.id = id;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public PreauthKeyPair execute(JdbcTemplate jdbc) {
            return (PreauthKeyPair) jdbc.query(this, new KeyPairExtractor());
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = getSelectFromSQL(PreauthKeyPair.class, false);
            sb.append(" WHERE ");
            if (id != null) {
                String col = getKeyColumn(PreauthKeyPair.class, true);
                sb.append(col).append(" = ?");
            } else if (name != null) {
                sb.append("name = ?");
            } else {
                throw new IllegalStateException("primary key and name are both null");
            }
            if (forUpdate) {
                sb.append(" FOR UPDATE");
            }
            String sql = sb.toString();
            log.debug("KeyPairGet: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            if (id != null) {
                prep.setObject(1, id);
            } else {
                prep.setString(1, name);
            }
            return prep;
        }
    }

    private class KeyPairList implements EntityList<PreauthKeyPair> {

        @Override
        public Set<PreauthKeyPair> query(JdbcTemplate jdbc) {
            List<PreauthKeyPair> keys = (List<PreauthKeyPair>) jdbc.query(this, new KeyPairRowMapper());
            Set<PreauthKeyPair> ret = new TreeSet<>();
            ret.addAll(keys);
            return ret;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = getSelectFromSQL(PreauthKeyPair.class, false);
            String sql = sb.toString();
            log.debug("KeyPairList: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            return prep;
        }
    }

    public class NodeGet implements EntityGet<Node> {
        private UUID id;
        private ContainerNode parent;
        private String name;
        private final boolean forUpdate;

        public NodeGet(boolean forUpdate) {
            this.forUpdate = forUpdate;
        }
        
        @Override
        public void setID(UUID id) {
            this.id = id;
        }

        public void setPath(ContainerNode parent, String name) {
            this.parent = parent;
            this.name = name;
        }
        
        @Override
        public Node execute(JdbcTemplate jdbc) {
            return (Node) jdbc.query(this, new NodeExtractor(parent));
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = getSelectFromSQL(Node.class, false);
            sb.append(" WHERE ");
            if (id != null) {
                String col = getKeyColumn(Node.class, true);
                sb.append(col).append(" = ?");
            } else if (parent != null && name != null) {
                String pidCol = "parentID";
                String nameCol = "name";
                // TODO: better way to get column names?
                sb.append(pidCol).append(" = ? and ").append(nameCol).append(" = ?");
            } else {
                throw new IllegalStateException("primary key is null");
            }
            if (forUpdate) {
                sb.append(" FOR UPDATE");
            }
            String sql = sb.toString();
            log.debug("Node: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            if (id != null) {
                prep.setObject(1, id);
            } else {
                prep.setObject(1, parent.getID());
                prep.setObject(2, name);
            }
            
            return prep;
        }
    }
    
    public class NodeIteratorQuery implements EntityIteratorQuery<Node> {
        private ContainerNode parent;
        private String start;
        private Integer limit;
        
        public NodeIteratorQuery() {
        }

        public void setParent(ContainerNode parent) {
            this.parent = parent;
        }

        public void setStart(String start) {
            this.start = start;
        }

        public void setLimit(Integer limit) {
            this.limit = limit;
        }
        
        @Override
        public ResourceIterator<Node> query(DataSource ds) {
            if (parent == null) {
                throw new RuntimeException("BUG: cannot query for children with parent==null");
            }
            
            StringBuilder sb = getSelectFromSQL(Node.class, false);
            sb.append(" WHERE parentID = ?");
            if (start != null) {
                sb.append(" AND ? <= name");
            }
            sb.append(" ORDER BY name ASC");
            if (limit != null) {
                sb.append(" LIMIT ?");
            }
            
            String sql = sb.toString();
            log.debug("sql: " + sql);
            
            try {
                Connection con = ds.getConnection();
                log.debug("NodeIteratorQuery: setAutoCommit(false)");
                con.setAutoCommit(false);
                // defaults for options: ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
                PreparedStatement ps = con.prepareStatement(sql);
                ps.setFetchSize(1000);
                ps.setFetchDirection(ResultSet.FETCH_FORWARD);
                int col = 1;
                
                ps.setObject(col++, parent.getID());
                log.debug("parentID = " + parent.getID());
                if (start != null) {
                    ps.setString(col++, start);
                    log.debug("start = " + start);
                }
                if (limit != null) {
                    ps.setInt(col++, limit);
                    log.debug("limit = " + limit);
                }
                ResultSet rs = ps.executeQuery();
                
                return new NodeResultSetIterator(con, rs, parent);
            } catch (SQLException ex) {
                throw new RuntimeException("BUG: artifact iterator query failed", ex);
            }
            
        }
    }
    
    private void safeSetBoolean(PreparedStatement prep, int col, Boolean value) throws SQLException {
        log.debug("safeSetBoolean: " + col + " " + value);
        if (value != null) {
            prep.setBoolean(col, value);
        } else {
            prep.setNull(col, Types.BOOLEAN);
        }
    }
    
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
    
    private void safeSetArray(PreparedStatement prep, int col, Set<GroupURI> values) throws SQLException {
        
        if (values != null && !values.isEmpty()) {
            log.debug("safeSetArray: " + col + " " + values.size());
            String[] array1d = new String[values.size()];
            int i = 0;
            for (GroupURI u : values) {
                array1d[i] = u.getURI().toASCIIString();
                i++;
            }
            java.sql.Array arr = prep.getConnection().createArrayOf("text", array1d);
            prep.setObject(col, arr);
        } else {
            log.debug("safeSetArray: " + col + " null");
            prep.setNull(col, Types.ARRAY);
        }
    }
    
    private void safeSetArray(PreparedStatement prep, int col, UUID[] value) throws SQLException {
        
        if (value != null) {
            log.debug("safeSetArray: " + col + " UUID[" + value.length + "]");
            java.sql.Array arr = prep.getConnection().createArrayOf("uuid", value);
            prep.setObject(col, arr);
        } else {
            log.debug("safeSetArray: " + col + " " + value);
            prep.setNull(col, Types.ARRAY);
        }
    }
    
    private void safeSetProps(PreparedStatement prep, int col, Set<NodeProperty> values) throws SQLException {
        
        if (values != null && !values.isEmpty()) {
            log.debug("safeSetProps: " + col + " " + values.size());
            String[][] array2d = new String[values.size()][2]; // TODO: w-h or h-w??
            int i = 0;
            for (NodeProperty np : values) {
                array2d[i][0] = np.getKey().toASCIIString();
                array2d[i][1] = np.getValue();
                i++;
            }
            java.sql.Array arr = prep.getConnection().createArrayOf("text", array2d);
            prep.setObject(col, arr);
        } else {
            log.debug("safeSetProps: " + col + " = null");
            prep.setNull(col, Types.ARRAY);
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
            safeSetString(prep, col++, value.getURI().toASCIIString());
            safeSetString(prep, col++, value.getBucket());
            safeSetString(prep, col++, value.getContentChecksum().toASCIIString());
            safeSetTimestamp(prep, col++, new Timestamp(value.getContentLastModified().getTime()), utc);
            safeSetLong(prep, col++, value.getContentLength());
            safeSetString(prep, col++, value.contentType);
            safeSetString(prep, col++, value.contentEncoding);
            
            if (!value.siteLocations.isEmpty()) {
                UUID[] ua = new UUID[value.siteLocations.size()];
                int i = 0;
                for (SiteLocation si : value.siteLocations) {
                    ua[i++] = si.getSiteID();
                }
                safeSetArray(prep, col++, ua);
            } else {
                safeSetArray(prep, col++, (UUID[]) null);
            }
            
            if (value.storageLocation != null) {
                safeSetString(prep, col++, value.storageLocation.getStorageID().toASCIIString());
                safeSetString(prep, col++, value.storageLocation.storageBucket);
            } else {
                safeSetString(prep, col++, (URI) null); // storageLocation.storageID
                safeSetString(prep, col++, (URI) null); // storageLocation.storageBucket
            }
            
            safeSetTimestamp(prep, col++, new Timestamp(value.getLastModified().getTime()), utc);
            safeSetString(prep, col++, value.getMetaChecksum().toASCIIString());
            
            log.debug("id " + value.getID());
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
            prep.setBoolean(col++, value.getAllowRead());
            prep.setBoolean(col++, value.getAllowWrite());
            
            prep.setTimestamp(col++, new Timestamp(value.getLastModified().getTime()), utc);
            prep.setString(col++, value.getMetaChecksum().toASCIIString());
            prep.setObject(col++, value.getID());
            
            return prep;
        }
        
    }
    
    private class ObsoleteStorageLocationPut implements EntityPut<ObsoleteStorageLocation> {
        private final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final boolean update;
        private ObsoleteStorageLocation value;
        
        ObsoleteStorageLocationPut(boolean update) {
            this.update = update;
        }

        @Override
        public void setValue(ObsoleteStorageLocation value) {
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
                sql = getUpdateSQL(ObsoleteStorageLocation.class);
                       
            } else {
                sql = getInsertSQL(ObsoleteStorageLocation.class);
            }
            log.debug("ObsoleteStorageLocationPut: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            int col = 1;
            
            prep.setString(col++, value.getLocation().getStorageID().toASCIIString());
            if (value.getLocation().storageBucket != null) {
                safeSetString(prep, col++, value.getLocation().storageBucket);
            } else {
                safeSetString(prep, col++, (String) null);
            }
            
            prep.setTimestamp(col++, new Timestamp(value.getLastModified().getTime()), utc);
            prep.setString(col++, value.getMetaChecksum().toASCIIString());
            prep.setObject(col++, value.getID());
            
            return prep;
        }
        
    }
    
    private class HarvestStatePut implements EntityPut<HarvestState> {
        private final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final boolean update;
        private HarvestState value;
        
        HarvestStatePut(boolean update) {
            this.update = update;
        }

        @Override
        public void setValue(HarvestState value) {
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
                sql = getUpdateSQL(HarvestState.class);
                       
            } else {
                sql = getInsertSQL(HarvestState.class);
            }
            log.debug("HarvestStatePut: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            int col = 1;
            
            prep.setString(col++, value.getName());
            prep.setString(col++, value.getResourceID().toASCIIString());
            if (value.curLastModified != null) {
                prep.setTimestamp(col++, new Timestamp(value.curLastModified.getTime()), utc);
            } else {
                prep.setNull(col++, Types.TIMESTAMP);
            }
            if (value.curID != null) {
                prep.setObject(col++, value.curID);
            } else {
                prep.setNull(col++, Types.OTHER);
            }
            if (value.instanceID != null) {
                prep.setObject(col++, value.instanceID);
            } else {
                prep.setNull(col++, Types.OTHER);
            }
            
            prep.setTimestamp(col++, new Timestamp(value.getLastModified().getTime()), utc);
            prep.setString(col++, value.getMetaChecksum().toASCIIString());
            prep.setObject(col++, value.getID());
            
            return prep;
        }
        
    }
    
    private class KeyPairPut implements EntityPut<PreauthKeyPair> {
        private final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final boolean update;
        private PreauthKeyPair value;
        
        KeyPairPut(boolean update) {
            this.update = update;
        }

        @Override
        public void setValue(PreauthKeyPair value) {
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
                sql = getUpdateSQL(PreauthKeyPair.class);
                       
            } else {
                sql = getInsertSQL(PreauthKeyPair.class);
            }
            log.debug("KeyPairPut: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            int col = 1;
            
            prep.setString(col++, value.getName());
            prep.setBytes(col++, value.getPublicKey());
            prep.setBytes(col++, value.getPrivateKey());

            prep.setTimestamp(col++, new Timestamp(value.getLastModified().getTime()), utc);
            prep.setString(col++, value.getMetaChecksum().toASCIIString());
            prep.setObject(col++, value.getID());
            
            return prep;
        }
    }
    
    private class NodePut implements EntityPut<Node> {
        private final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final boolean update;
        private Node value;
        
        NodePut(boolean update) {
            this.update = update;
        }

        @Override
        public void setValue(Node value) {
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
                sql = getUpdateSQL(Node.class);
                       
            } else {
                sql = getInsertSQL(Node.class);
            }
            log.debug("NodePut: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            int col = 1;
            
            if (value.parentID == null) {
                throw new RuntimeException("BUG: cannot put Node without a parentID: " + value);
            }
            prep.setObject(col++, value.parentID);
            prep.setString(col++, value.getName());
            prep.setString(col++, value.getClass().getSimpleName().substring(0, 1)); // HACK
            if (value.ownerID == null) {
                throw new RuntimeException("BUG: cannot put Node without an ownerID: " + value);
            }
            prep.setString(col++, value.ownerID.toString());
            safeSetBoolean(prep, col++, value.isPublic);
            safeSetBoolean(prep, col++, value.isLocked);
            safeSetArray(prep, col++, value.getReadOnlyGroup());
            safeSetArray(prep, col++, value.getReadWriteGroup());
            safeSetProps(prep, col++, value.getProperties());
            if (value instanceof ContainerNode) {
                ContainerNode cn = (ContainerNode) value;
                safeSetBoolean(prep, col++, cn.inheritPermissions);
            } else {
                safeSetBoolean(prep, col++, null);
            }
            if (value instanceof DataNode) {
                DataNode dn = (DataNode) value;
                if (dn.storageID == null) {
                    throw new RuntimeException("BUG: cannot put DataNode without a storageID: " + value);
                }
                safeSetBoolean(prep, col++, dn.busy);
                safeSetString(prep, col++, dn.storageID);
            } else {
                safeSetBoolean(prep, col++, null);
                safeSetString(prep, col++, (URI) null);
            }
            if (value instanceof LinkNode) {
                LinkNode ln = (LinkNode) value;
                prep.setString(col++, ln.getTarget().toASCIIString());
            } else {
                safeSetString(prep, col++, (URI) null);
            }
            
            prep.setTimestamp(col++, new Timestamp(value.getLastModified().getTime()), utc);
            prep.setString(col++, value.getMetaChecksum().toASCIIString());
            prep.setObject(col++, value.getID());
            
            return prep;
        }
    }
    
    private class EntityEventPut implements EntityPut<Entity> {
        private final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final boolean update;
        private Entity value;
        
        EntityEventPut(boolean update) {
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
        String tab = getTable(c);
        Class targetClass = c;
        if (entityCols) {
            targetClass = Entity.class;
        }
        String[] cols = getColumns(targetClass);
        
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
    
    private String getUpdateSQL(Class c) {
        StringBuilder sb = new StringBuilder();
        String tab = getTable(c);
        sb.append("UPDATE ");
        sb.append(tab);
        sb.append(" SET ");
        String[] cols = getColumns(c);
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
        String tab = getTable(c);
        sb.append("INSERT INTO ");
        sb.append(tab);
        sb.append(" (");
        String[] cols = getColumns(c);
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
        String tab = getTable(c);
        sb.append("DELETE FROM ");
        sb.append(tab);
        sb.append(" WHERE id = ?");
        return sb.toString();
    }
    
    private String getKeyColumn(Class c, boolean pk) {
        String[] cols = getColumns(c);
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
    
    private class SkeletonEntityExtractor implements ResultSetExtractor<SkeletonEntity> {

        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public SkeletonEntity extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            int col = 1;
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            SkeletonEntity ret = new SkeletonEntity(id);
            InventoryUtil.assignLastModified(ret, lastModified);
            InventoryUtil.assignMetaChecksum(ret, metaChecksum);
            return ret;
        }
    }
    
    // convenience to extract one artifact from single row result set
    private class ArtifactExtractor implements ResultSetExtractor<Artifact> {

        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public Artifact extractData(ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return null;
            }
            return mapRowToArtifact(rs, utc);
            
        }
    }
    
    private class ArtifactResultSetIterator implements ResourceIterator<Artifact> {
        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final Connection con;
        private final ResultSet rs;
        boolean hasRow;
        boolean closeWhenDone = false; // not a pooled connection
        
        ArtifactResultSetIterator(Connection con, ResultSet rs) throws SQLException {
            this.con = con;
            this.rs = rs;
            hasRow = rs.next();
            log.debug("ArtifactResultSetIterator: " + super.toString() + " ctor " + hasRow);
            if (!hasRow) {
                log.debug("ArtifactResultSetIterator:  " + super.toString() + " ctor - setAutoCommit(true)");
                try {
                    con.setAutoCommit(true); // commit txn
                    if (closeWhenDone) {
                        con.close(); // return to pool
                    }
                } catch (SQLException unexpected) {
                    log.error("Connection.setAutoCommit(true) & close() failed", unexpected);
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (hasRow) {
                log.debug("ArtifactResultSetIterator:  " + super.toString() + " ctor - setAutoCommit(true)");
                try {
                    con.setAutoCommit(true); // commit txn
                    if (closeWhenDone) {
                        con.close(); // return to pool
                    }
                    hasRow = false; 
                } catch (SQLException unexpected) {
                    log.error("Connection.setAutoCommit(true) & close() failed", unexpected);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return hasRow;
        }

        @Override
        public Artifact next() {
            try {
                Artifact ret = mapRowToArtifact(rs, utc);
                hasRow = rs.next();
                if (!hasRow) {
                    log.debug("ArtifactResultSetIterator:  " + super.toString() + " DONE - setAutoCommit(true)");
                    try {
                        con.setAutoCommit(true); // commit txn
                        if (closeWhenDone) {
                            con.close(); // return to pool
                        }
                    } catch (SQLException unexpected) {
                        log.error("Connection.setAutoCommit(true) & close() failed", unexpected);
                    }
                }
                return ret;
            } catch (Exception ex) {
                if (hasRow) {
                    log.debug("ArtifactResultSetIterator:  " + super.toString() + " ResultSet.next() FAILED - setAutoCommit(true)");
                    try {
                        con.setAutoCommit(true); // commit txn
                        if (closeWhenDone) {
                            con.close(); // return to pool
                        }
                        hasRow = false;
                    } catch (SQLException unexpected) {
                        log.error("Connection.setAutoCommit(true) & close() failed", unexpected);
                    }
                }
                throw new RuntimeException("BUG: artifact list query failed while iterating", ex);
            }
        }
    }
    
    private class NodeResultSetIterator implements ResourceIterator<Node> {
        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final Connection con;
        private final ResultSet rs;
        boolean hasRow;
        
        ContainerNode parent;

        public NodeResultSetIterator(Connection con, ResultSet rs, ContainerNode parent) throws SQLException {
            this.con = con;
            this.rs = rs;
            this.parent = parent;
            hasRow = rs.next();
            log.debug("NodeResultSetIterator: " + super.toString() + " ctor " + hasRow);
            if (!hasRow) {
                log.debug("NodeResultSetIterator:  " + super.toString() + " ctor - setAutoCommit(true)");
                
                try {
                    con.setAutoCommit(true); // commit txn
                    con.close(); // return to pool
                } catch (SQLException ignore) {
                    log.error("Connection.setAutoCommit(true) & close() failed", ignore);
                }
            }
        }
        
        @Override
        public void close() throws IOException {
            if (hasRow) {
                log.debug("NodeResultSetIterator:  " + super.toString() + " close - setAutoCommit(true)");
                try {
                    con.setAutoCommit(true); // commit txn
                    con.close(); // return to pool
                    hasRow = false;
                } catch (SQLException ignore) {
                    log.error("Connection.setAutoCommit(true) & close() failed", ignore);
                }
            }
        }
        
        @Override
        public boolean hasNext() {
            return hasRow;
        }
        
        @Override
        public Node next() {
            try {
                Node ret = mapRowToNode(rs, utc, parent);
                hasRow = rs.next();
                if (!hasRow) {
                    log.debug("NodeResultSetIterator:  " + super.toString() + " DONE - setAutoCommit(true)");
                    try {
                        con.setAutoCommit(true); // commit txn
                        con.close(); // return to pool
                        hasRow = false;
                    } catch (SQLException ignore) {
                        log.error("Connection.setAutoCommit(true) & close() failed", ignore);
                    }
                }
                return ret;
            } catch (Exception ex) {
                if (hasRow) {
                    log.debug("NodeResultSetIterator:  " + super.toString() + " ResultSet.next() FAILED - setAutoCommit(true)");
                    try {
                        con.setAutoCommit(true); // commit txn
                        con.close(); // return to pool
                        hasRow = false;
                    } catch (SQLException ignore) {
                        log.error("Connection.setAutoCommit(true) & close() failed", ignore);
                    }
                }
                throw new RuntimeException("BUG: node list query failed while iterating", ex);
            }
        }
    }
    
    private Artifact mapRowToArtifact(ResultSet rs, Calendar utc) throws SQLException {
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
        if (storLoc != null) {
            a.storageLocation = new StorageLocation(storLoc);
            a.storageLocation.storageBucket = storBucket;
        }
        InventoryUtil.assignLastModified(a, lastModified);
        InventoryUtil.assignMetaChecksum(a, metaChecksum);
        return a;
    }
    
    private Node mapRowToNode(ResultSet rs, Calendar utc, ContainerNode parent) throws SQLException {
        int col = 1;
        final UUID parentID = Util.getUUID(rs, col++);
        final String name = rs.getString(col++);
        final String nodeType = rs.getString(col++);
        final String ownerID = rs.getString(col++);
        final Boolean isPublic = Util.getBoolean(rs, col++);
        final Boolean isLocked = Util.getBoolean(rs, col++);
        final String rawROG = rs.getString(col++);
        final String rawRWG = rs.getString(col++);
        final String rawProps = rs.getString(col++);
        final Boolean inheritPermissions = Util.getBoolean(rs, col++);
        final Boolean busy = Util.getBoolean(rs, col++);
        final URI storageID = Util.getURI(rs, col++);
        final URI linkTarget = Util.getURI(rs, col++);
        final Date lastModified = Util.getDate(rs, col++, utc);
        final URI metaChecksum = Util.getURI(rs, col++);
        final UUID id = Util.getUUID(rs, col++);

        Node ret;
        if (nodeType.equals("C")) {
            ContainerNode cn = new ContainerNode(id, name);
            cn.inheritPermissions = inheritPermissions;
            ret = cn;
        } else if (nodeType.equals("D")) {
            ret = new DataNode(id, name, storageID);
        } else if (nodeType.equals("L")) {
            ret = new LinkNode(id, name, linkTarget);
        } else {
            throw new RuntimeException("BUG: unexpected node type code: " + nodeType);
        }
        ret.parentID = parentID;
        ret.ownerID = ownerID;
        ret.isPublic = isPublic;
        ret.isLocked = isLocked;

        if (rawROG != null) {
            Util.parseArrayGroupURI(rawROG, ret.getReadOnlyGroup());
        }
        if (rawRWG != null) {
            Util.parseArrayGroupURI(rawRWG, ret.getReadWriteGroup());
        }
        if (rawProps != null) {
            Util.parseArrayProps(rawProps, ret.getProperties());
        }

        InventoryUtil.assignLastModified(ret, lastModified);
        InventoryUtil.assignMetaChecksum(ret, metaChecksum);

        return ret;
    }
    
    private class ObsoleteStorageLocationExtractor implements ResultSetExtractor<ObsoleteStorageLocation> {

        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public ObsoleteStorageLocation extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            int col = 1;
            final URI storLoc = Util.getURI(rs, col++);
            final String storBucket = rs.getString(col++);
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            StorageLocation s = new StorageLocation(storLoc);
            s.storageBucket = storBucket;
            ObsoleteStorageLocation ret = new ObsoleteStorageLocation(id, s);
            InventoryUtil.assignLastModified(ret, lastModified);
            InventoryUtil.assignMetaChecksum(ret, metaChecksum);
            return ret;
        }
    }
    
    private class HarvestStateExtractor implements ResultSetExtractor<HarvestState> {
        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public HarvestState extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            int col = 1;
            final String name = rs.getString(col++);
            final URI resourecID = Util.getURI(rs, col++);
            final Date curLastModified = Util.getDate(rs, col++, utc);
            final UUID curID = Util.getUUID(rs, col++);
            final UUID instanceID = Util.getUUID(rs, col++);
            
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            HarvestState ret = new HarvestState(id, name, resourecID);
            ret.curLastModified = curLastModified;
            ret.curID = curID;
            ret.instanceID = instanceID;
            InventoryUtil.assignLastModified(ret, lastModified);
            InventoryUtil.assignMetaChecksum(ret, metaChecksum);
            return ret;
        }
    }
    
    private class StorageSiteRowMapper implements RowMapper<StorageSite> {
        Calendar utc = Calendar.getInstance(DateUtil.UTC);

        @Override
        public StorageSite mapRow(ResultSet rs, int i) throws SQLException {
            int col = 1;
            final URI resourceID = Util.getURI(rs, col++);
            final String name = rs.getString(col++);
            final boolean allowRead = rs.getBoolean(col++);
            final boolean allowWrite = rs.getBoolean(col++);
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            StorageSite s = new StorageSite(id, resourceID, name, allowRead, allowWrite);
            InventoryUtil.assignLastModified(s, lastModified);
            InventoryUtil.assignMetaChecksum(s, metaChecksum);
            return s;
        }
        
    }
    
    private class StorageSiteExtractor implements ResultSetExtractor<StorageSite> {
        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public StorageSite extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            StorageSiteRowMapper m = new StorageSiteRowMapper();
            return m.mapRow(rs, 1);
        }
    }
    
    private class KeyPairRowMapper implements RowMapper<PreauthKeyPair> {
        Calendar utc = Calendar.getInstance(DateUtil.UTC);

        @Override
        public PreauthKeyPair mapRow(ResultSet rs, int i) throws SQLException {
            int col = 1;
            final String name = rs.getString(col++);
            final byte[] pub = rs.getBytes(col++);
            final byte[] priv = rs.getBytes(col++);

            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            PreauthKeyPair s = new PreauthKeyPair(id, name, pub, priv);
            InventoryUtil.assignLastModified(s, lastModified);
            InventoryUtil.assignMetaChecksum(s, metaChecksum);
            return s;
        }
    }
    
    private class KeyPairExtractor implements ResultSetExtractor<PreauthKeyPair> {
        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public PreauthKeyPair extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            KeyPairRowMapper m = new KeyPairRowMapper();
            return m.mapRow(rs, 1);
        }
    }
    
    private class DeletedArtifactEventExtractor implements ResultSetExtractor<DeletedArtifactEvent> {

        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public DeletedArtifactEvent extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            int col = 1;
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            DeletedArtifactEvent ret = new DeletedArtifactEvent(id);
            InventoryUtil.assignLastModified(ret, lastModified);
            InventoryUtil.assignMetaChecksum(ret, metaChecksum);
            return ret;
        }
    }
    
    private class DeletedStorageLocationEventExtractor implements ResultSetExtractor<DeletedStorageLocationEvent> {

        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public DeletedStorageLocationEvent extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            int col = 1;
            
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            DeletedStorageLocationEvent ret = new DeletedStorageLocationEvent(id);
            InventoryUtil.assignLastModified(ret, lastModified);
            InventoryUtil.assignMetaChecksum(ret, metaChecksum);
            return ret;
        }
    }
    
    private class StorageLocationEventExtractor implements ResultSetExtractor<StorageLocationEvent> {

        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        
        @Override
        public StorageLocationEvent extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            int col = 1;
            
            final Date lastModified = Util.getDate(rs, col++, utc);
            final URI metaChecksum = Util.getURI(rs, col++);
            final UUID id = Util.getUUID(rs, col++);
            
            StorageLocationEvent ret = new StorageLocationEvent(id);
            InventoryUtil.assignLastModified(ret, lastModified);
            InventoryUtil.assignMetaChecksum(ret, metaChecksum);
            return ret;
        }
    }
    
    private class NodeExtractor implements ResultSetExtractor<Node> {
        private ContainerNode parent;
        final Calendar utc = Calendar.getInstance(DateUtil.UTC);

        public NodeExtractor(ContainerNode parent) {
            this.parent = parent; // optional
        }
        
        @Override
        public Node extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (!rs.next()) {
                return null;
            }
            
            return mapRowToNode(rs, utc, parent);
        }
        
        
    }
}

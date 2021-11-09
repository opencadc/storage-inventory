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
 *  $Revision: 5 $
 *
 ************************************************************************
 */

package org.opencadc.inventory.db;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.db.TransactionManager;
import java.lang.reflect.Constructor;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Entity;
import org.opencadc.inventory.InventoryUtil;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 * @param <T> type of object to persist
 */
public abstract class AbstractDAO<T extends Entity> {

    private static final Logger log = Logger.getLogger(AbstractDAO.class);

    protected SQLGenerator gen;
    protected DataSource dataSource;
    
    protected final boolean origin;

    protected AbstractDAO(boolean origin) {
        this.origin = origin;
    }
    
    /**
     * Copy configuration from argument DAO. This uses the same DataSource and TransactionManager
     * so calls to this and another DAO participate in the same transaction.
     * 
     * @param dao another DAO to copy/share configuration
     */
    protected AbstractDAO(AbstractDAO dao) {
        this(dao.origin);
        this.gen = dao.getSQLGenerator();
        this.dataSource = dao.getDataSource();
    }

    /**
     * Copy configuration but override origin setting. This is for HarvestStateDAO which is
     * always origin=true.
     * 
     * @param dao
     * @param origin 
     */
    protected AbstractDAO(AbstractDAO dao, boolean origin) {
        this(origin);
        this.gen = dao.getSQLGenerator();
        this.dataSource = dao.getDataSource();
    }
    
    protected MessageDigest getDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("FATAL: no MD5 digest algorithm available", ex);
        }
    }
    
    // try to unwrap spring jdbc exception consistently and throw a RuntimeException with
    // a decent message
    protected void handleInternalFail(BadSqlGrammarException ex) throws RuntimeException {
        Throwable cause = ex.getCause();
        if (cause != null) {
            if (cause.getMessage().contains("permission")) {
                throw new RuntimeException("CONFIG: " + cause.getMessage(), cause);
            }
            throw new RuntimeException("BUG: " + cause.getMessage(), cause);
        }
        throw new RuntimeException("BUG: " + ex.getMessage(), ex);
    }
    
    /**
     * Get the DataSource in use by the DAO. This is intended so that
     * applications can include other SQL statements along with DAO operations
     * in a single transaction.
     *
     * @return the DataSource
     */
    public DataSource getDataSource() {
        checkInit();
        return dataSource;
    }

    SQLGenerator getSQLGenerator() {
        checkInit();
        return gen;
    }

    /**
     * Get a new TransactionManager that controls transactions using this DAOs
     * DataSource. For thread safe use of a DAO class, this method creates a new 
     * TransactionManager each time it is called. The caller must maintain a reference 
     * to the instance for the life of a transaction.
     *
     * @return the TransactionManager
     */
    public TransactionManager getTransactionManager() {
        checkInit();
        return new DatabaseTransactionManager(dataSource);
    }

    public Map<String, Class> getParams() {
        Map<String, Class> ret = new TreeMap<String, Class>();
        ret.put("jndiDataSourceName", String.class);
        ret.put("database", String.class);
        ret.put("schema", String.class);
        ret.put(SQLGenerator.class.getName(), Class.class);
        return ret;
    }
    
    /**
     * Configuration parameters:
     * <ul>
     * <li>String jndiDataSourceName
     * <li>String database (currently unused)
     * <li>String schema
     * <li>Class  org.opencadc.inventory.db.SQLGenerator
     * </ul>
     * 
     * @param config map with the required configuration
     */
    public void setConfig(Map<String, Object> config) {
        Class<?> genClass = (Class<?>) config.get(SQLGenerator.class.getName());
        if (genClass == null) {
            throw new IllegalArgumentException("missing required config: " + SQLGenerator.class.getName());
        }
        
        String jndiDataSourceName = (String) config.get("jndiDataSourceName");
        if (jndiDataSourceName == null) {
            throw new IllegalArgumentException("missing required config: jndiDataSourceName");
        }
        try {
            this.dataSource = DBUtil.findJNDIDataSource(jndiDataSourceName);
        } catch (NamingException ex) {
            throw new IllegalArgumentException("cannot find JNDI DataSource: " + jndiDataSourceName);
        }

        String database = (String) config.get("database");
        String schema = (String) config.get("schema");
        try {
            Constructor<?> ctor = genClass.getConstructor(String.class, String.class);
            this.gen = (SQLGenerator) ctor.newInstance(database, schema);
        } catch (Exception ex) {
            throw new RuntimeException("failed to instantiate SQLGenerator: " + genClass.getName(), ex);
        }
        
        try {
            Date now = getCurrentTime();
            log.debug("connection test: " + now);
        } catch (Exception ex) {
            throw new RuntimeException("failed to verify DataSource", ex);
        }
    }

    protected void checkInit() {
        if (gen == null) {
            throw new IllegalStateException("setConfig never called or failed");
        }
    }

    protected Date getCurrentTime() {
        checkInit();
        String tsSQL = gen.getCurrentTimeSQL();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        Date now = jdbc.queryForObject(tsSQL, new RowMapper<Date>() {
            @Override
            public Date mapRow(ResultSet rs, int i) throws SQLException {
                return Util.getDate(rs, 1, Calendar.getInstance(DateUtil.LOCAL));
            }
        });
        return now;
        
    }
    
    protected T get(Class entityClass, UUID id) {
        checkInit();
        log.debug("GET: " + id);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            EntityGet<T> get = (EntityGet<T>) gen.getEntityGet(entityClass);
            get.setID(id);
            T e = (T) get.execute(jdbc);
            return e;
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + id + " " + dt + "ms");
        }
        throw new RuntimeException("BUG: should be unreachable");
    }
    
    /**
     * Acquire a write lock on the existing entity. This is used as the first action
     * in a transaction in order to avoid race conditions and deadlocks.
     * @param val entity value to lock
     * @throws EntityNotFoundException if the specified entity does not exist
     */
    public void lock(T val) throws EntityNotFoundException {
        if (val == null) {
            throw new IllegalArgumentException("entity cannot be null");
        }
        checkInit();
        log.debug("LOCK: " + val.getID());
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            EntityLock lock = gen.getEntityLock(val.getClass());
            lock.setID(val.getID());
            lock.execute(jdbc);
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("PUT: " + val.getID() + " " + dt + "ms");
        }
    }
    
    public void put(T val) {
        put(val, false, false);
    }
    
    protected void put(T val, boolean extendedUpdate, boolean timestampUpdate) {
        if (val == null) {
            throw new IllegalArgumentException("entity cannot be null");
        }
        checkInit();
        log.debug("PUT: " + val.getID() + " force=" + extendedUpdate);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            EntityGet get = gen.getSkeletonEntityGet(val.getClass());
            get.setID(val.getID());
            Entity cur = get.execute(jdbc);
            Date now = getCurrentTime();
            boolean update = cur != null;
            boolean delta = updateEntity(val, cur, now, timestampUpdate);
            if (delta || extendedUpdate) {
                EntityPut put = gen.getEntityPut(val.getClass(), update);
                put.setValue(val);
                put.execute(jdbc);
            } else {
                log.debug("no change: " + cur);
            }
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("PUT: " + val.getID() + " " + dt + "ms");
        }
    }
    
    protected void delete(Class entityClass, UUID id) {
        if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        checkInit();
        log.debug("DELETE: " + id);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            EntityDelete del = gen.getEntityDelete(entityClass);
            del.setID(id);
            del.execute(jdbc);
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("DELETE: " + id + " " + dt + "ms");
        }
    }
    
    // assign metaChecksum and update lastModified
    private boolean updateEntity(T entity, Entity cur, Date now, boolean timestampUpdate) {
        log.debug("updateEntity: " + entity);
        MessageDigest digest = getDigest();
        
        InventoryUtil.assignMetaChecksum(entity, entity.computeMetaChecksum(digest));
        
        boolean delta = false;
        if (cur == null) {
            delta = true;
        } else {
            // metadata change
            delta = !entity.getMetaChecksum().equals(cur.getMetaChecksum());
        }
        
        if ((origin && delta) || (origin && timestampUpdate) || entity.getLastModified() == null) {
            InventoryUtil.assignLastModified(entity, now);
        }
        
        if (cur != null && !cur.getLastModified().equals(entity.getLastModified())) {
            // timestamp update
            delta = true;
        }

        return delta;
    }
}

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

package org.opencadc.inventory.util;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfigException;
import ca.nrc.cadc.db.StandaloneContextFactory;
import javax.management.ObjectName;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.log4j.Logger;

/**
 * Utility for applications to create and manage JDBC connections. This class extends
 * <code>ca.nrc.cadc.db.DBUtil</code> to add some extra static methods for creating a
 * connection pool. This code may be merged upstream into the parent class at some point.
 * 
 * @author pdowler
 */
public abstract class DBUtil extends ca.nrc.cadc.db.DBUtil {
    private static final Logger log = Logger.getLogger(DBUtil.class);

    private DBUtil() { 
    }
    
    public static class PoolConfig {
        private ConnectionConfig cc;

        private int poolSize;
        
        private long maxWait;
        
        private String validationQuery;

        public PoolConfig(ConnectionConfig cc, int poolSize, long maxWait, String validationQuery) {
            this.cc = cc;
            this.poolSize = poolSize;
            this.maxWait = maxWait;
            this.validationQuery = validationQuery;
        }
    }
    
    public static void createJNDIDataSource(String dataSourceName, PoolConfig config) throws NamingException {
        log.debug("createJNDIDataSource: " + dataSourceName + " POOL START");
        
            
        StandaloneContextFactory.initJNDI();

        Context initContext = new InitialContext();
        Context envContext = (Context) initContext.lookup(DEFAULT_JNDI_ENV_CONTEXT);
        if (envContext == null) {
            envContext = initContext.createSubcontext(DEFAULT_JNDI_ENV_CONTEXT);
        }
        log.debug("env: " + envContext);

        DataSource dataSource = createPool(config);
 
        envContext.bind(dataSourceName, dataSource);
        log.debug("createJNDIDataSource: " + dataSourceName + " POOL DONE");
    }
    
    private static DataSource createPool(PoolConfig config) {
        try {
            // load JDBC driver
            Class.forName(config.cc.getDriver());
        
            ConnectionFactory connectionFactory = 
                new DriverManagerConnectionFactory(config.cc.getURL(), config.cc.getUsername(), config.cc.getPassword());
            
            ObjectName jmxObjectName = null; // TODO? 
            PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, jmxObjectName);
            poolableConnectionFactory.setValidationQuery(config.validationQuery);
            
            GenericObjectPool objectPool = new GenericObjectPool(poolableConnectionFactory);
            objectPool.setMinIdle(config.poolSize);
            objectPool.setMaxIdle(config.poolSize);
            objectPool.setMaxTotal(config.poolSize);
            objectPool.setTestOnBorrow(true);
            objectPool.setTestOnCreate(true);
            objectPool.setMaxWaitMillis(config.maxWait);
            poolableConnectionFactory.setPool(objectPool);
            
            PoolingDataSource dataSource = new PoolingDataSource(objectPool);
            
            return dataSource;
        } catch (ClassNotFoundException ex) {
            throw new DBConfigException("failed to load JDBC driver: " + config.cc.getDriver(), ex);
        } catch (Exception ex) {
            throw new DBConfigException("failed to init connection pool: " + config.cc.getURL(), ex);
        }
    }
}

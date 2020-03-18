
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
 *
 ************************************************************************
 */

package org.opencadc.inventory.db;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.StringUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.naming.NamingException;

import org.opencadc.inventory.Entity;

/**
 * Class to manage and create DAO instances.  This will create new DAOs, or build one from an existing DAO to have
 * true transaction management.
 *
 * <p>Expected Properties are:
 * org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
 * org.opencadc.inventory.db.schema={schema}
 * org.opencadc.inventory.db.username={dbuser}
 * org.opencadc.inventory.db.password={dbpassword}
 * org.opencadc.inventory.db.url=jdbc:postgresql://{server}/{database}
 */
public class DAOConfigurationManager {

    private static final String JNDI_ARTIFACT_DATASOURCE_NAME = "jdbc/inventory";
    private static final String SQL_GEN_KEY = SQLGenerator.class.getName();

    private static final String JDBC_CONFIG_KEY_PREFIX = "org.opencadc.tantar.db";
    private static final String JDBC_SCHEMA_KEY = String.format("%s.schema", JDBC_CONFIG_KEY_PREFIX);
    private static final String JDBC_USERNAME_KEY = String.format("%s.username", JDBC_CONFIG_KEY_PREFIX);
    private static final String JDBC_PASSWORD_KEY = String.format("%s.password", JDBC_CONFIG_KEY_PREFIX);
    private static final String JDBC_URL_KEY = String.format("%s.url", JDBC_CONFIG_KEY_PREFIX);
    private static final String JDBC_DRIVER_CLASSNAME = "org.postgresql.Driver";

    private final Properties properties;


    public DAOConfigurationManager(final Properties properties) {
        this.properties = properties;
    }


    /**
     * Ensure the DataSource is registered in the JNDI, and return the name under which it was registered.
     *
     * @return The JNDI name.
     */
    private String registerDataSource() {
        try {
            // Check if this data source is registered already.
            DBUtil.findJNDIDataSource(JNDI_ARTIFACT_DATASOURCE_NAME);
        } catch (NamingException e) {
            final ConnectionConfig cc = new ConnectionConfig(null, null,
                                                             properties.getProperty(JDBC_USERNAME_KEY),
                                                             properties.getProperty(JDBC_PASSWORD_KEY),
                                                             JDBC_DRIVER_CLASSNAME,
                                                             properties.getProperty(JDBC_URL_KEY));
            try {
                DBUtil.createJNDIDataSource(JNDI_ARTIFACT_DATASOURCE_NAME, cc);
            } catch (NamingException ne) {
                throw new IllegalStateException("Unable to access Inventory Database.", ne);
            }
        }

        return JNDI_ARTIFACT_DATASOURCE_NAME;
    }

    public void configure(final AbstractDAO<? extends Entity> abstractDAO) {
        final Map<String, Object> config = new HashMap<>();
        final String sqlGeneratorClassName = properties.getProperty(SQL_GEN_KEY, SQLGenerator.class.getCanonicalName());
        try {
            config.put(SQL_GEN_KEY, Class.forName(sqlGeneratorClassName));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("could not load SQLGenerator class: " + e.getMessage(), e);
        }

        config.put("jndiDataSourceName", registerDataSource());

        final String schemaName = properties.getProperty(JDBC_SCHEMA_KEY);

        if (StringUtil.hasLength(schemaName)) {
            config.put("schema", schemaName);
        } else {
            throw new IllegalStateException(
                    String.format("A value for %s is required in minoc.properties", JDBC_SCHEMA_KEY));
        }

        config.put("database", "inventory");

        abstractDAO.setConfig(config);
    }
}

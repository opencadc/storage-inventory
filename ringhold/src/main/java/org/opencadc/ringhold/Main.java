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

package org.opencadc.ringhold;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.naming.NamingException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.inventory.db.SQLGenerator;

/**
 * Main entry point for ringhold.
 *
 * @author pdowler
 */
public class Main {

    private static final Logger log = Logger.getLogger(Main.class);
    private static final String CONFIG_FILE_NAME = "ringhold.properties";
    private static final String CONFIG_PREFIX = Main.class.getPackage().getName();
    private static final String SQLGENERATOR_CONFIG_KEY = SQLGenerator.class.getName();
    private static final String DB_SCHEMA_CONFIG_KEY = CONFIG_PREFIX + ".db.schema";
    private static final String LOGGING_KEY = CONFIG_PREFIX + ".logging";
    private static final String DB_URL_CONFIG_KEY = CONFIG_PREFIX + ".db.url";
    private static final String DB_USERNAME_CONFIG_KEY = CONFIG_PREFIX + ".db.username";
    private static final String DB_PASSWORD_CONFIG_KEY = CONFIG_PREFIX + ".db.password";

    // Used to verify configuration items.  See the README for descriptions.
    private static final String[] MANDATORY_PROPERTY_KEYS = {
        DB_PASSWORD_CONFIG_KEY,
        DB_SCHEMA_CONFIG_KEY,
        DB_URL_CONFIG_KEY,
        DB_USERNAME_CONFIG_KEY,
        LOGGING_KEY,
        SQLGENERATOR_CONFIG_KEY
    };

    public static void main(final String[] args) {
        Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
        Log4jInit.setLevel("org.opencadc", Level.WARN);

        try {
            final PropertiesReader propertiesReader = new PropertiesReader(CONFIG_FILE_NAME);
            final MultiValuedProperties props = propertiesReader.getAllProperties();
            if (props == null) {
                log.fatal(String.format("Configuration file not found: %s\n", CONFIG_FILE_NAME));
                System.exit(2);
            }
            final String[] missingKeys = Main.verifyConfiguration(props);

            if (missingKeys.length > 0) {
                log.fatal(String.format("Configuration file %s missing one or more values: %s\n", CONFIG_FILE_NAME,
                                        Arrays.toString(missingKeys)));
                System.exit(2);
            }

            final String configuredLogging = props.getFirstPropertyValue(LOGGING_KEY);
            Log4jInit.setLevel("org.opencadc.ringhold", Level.toLevel(configuredLogging.toUpperCase()));
            Log4jInit.setLevel("org.opencadc.inventory", Level.toLevel(configuredLogging.toUpperCase()));

            // DAO Configuration
            final String username = props.getFirstPropertyValue(DB_USERNAME_CONFIG_KEY);
            final String password = props.getFirstPropertyValue(DB_PASSWORD_CONFIG_KEY);
            final String dbUrl = props.getFirstPropertyValue(DB_URL_CONFIG_KEY);
            final ConnectionConfig cc = new ConnectionConfig(null, null, username, password, "org.postgresql.Driver", dbUrl);
            try {
                DBUtil.createJNDIDataSource("jdbc/inventory-txn", cc);
                DBUtil.createJNDIDataSource("jdbc/inventory-iter", cc);
            } catch (NamingException ne) {
                throw new IllegalStateException(String.format("Unable to access database: %s", dbUrl), ne);
            }
            
            final Map<String, Object> daoConfig = new TreeMap<>();
            daoConfig.put("schema", props.getFirstPropertyValue(DB_SCHEMA_CONFIG_KEY));
            daoConfig.put("jndiDataSourceName", "jdbc/inventory-txn");
            final String configuredSQLGenerator = props.getFirstPropertyValue(SQLGENERATOR_CONFIG_KEY);
            daoConfig.put(SQLGENERATOR_CONFIG_KEY, Class.forName(configuredSQLGenerator));
            
            final Map<String, Object> iterConfig = new TreeMap<>();
            iterConfig.put("schema", daoConfig.get("schema"));
            iterConfig.put("jndiDataSourceName", "jdbc/inventory-iter");
            iterConfig.put(SQLGENERATOR_CONFIG_KEY, Class.forName(configuredSQLGenerator));

            final InventoryValidator doit = new InventoryValidator(daoConfig, iterConfig);
            doit.run();
        } catch (Throwable unexpected) {
            log.fatal("Unexpected failure", unexpected);
            System.exit(-1);
        }
    }

    /**
     * Verify all of the mandatory properties.
     * @param properties    The properties to check the mandatory keys against.
     * @return  An array of missing String keys, or empty array.  Never null.
     */
    private static String[] verifyConfiguration(final MultiValuedProperties properties) {
        final Set<String> keySet = properties.keySet();
        return Arrays.stream(MANDATORY_PROPERTY_KEYS).filter(k -> !keySet.contains(k)).toArray(String[]::new);
    }

    private Main() {
    }
}

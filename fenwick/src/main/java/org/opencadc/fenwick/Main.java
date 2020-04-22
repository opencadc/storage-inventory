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

package org.opencadc.fenwick;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.StringUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;
import javax.naming.NamingException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.db.SQLGenerator;



/**
 * Main entry point for fenwick.
 *
 * @author pdowler
 */
public class Main {

    private static final Logger log = Logger.getLogger(Main.class);
    private static final String CONFIG_FILE_NAME = "fenwick.properties";
    private static final String CONFIG_PREFIX = Main.class.getPackage().getName();
    private static final String SQLGENERATOR_CONFIG_KEY = SQLGenerator.class.getName();
    private static final String DB_SCHEMA_CONFIG_KEY = String.format("%s.db.schema", CONFIG_PREFIX);
    private static final String DB_URL_CONFIG_KEY = String.format("%s.db.url", CONFIG_PREFIX);
    private static final String DB_USERNAME_CONFIG_KEY = String.format("%s.db.username", CONFIG_PREFIX);
    private static final String DB_PASSWORD_CONFIG_KEY = String.format("%s.db.password", CONFIG_PREFIX);
    private static final String QUERY_SERVICE_CONFIG_KEY = String.format("%s.queryService", CONFIG_PREFIX);
    private static final String ARTIFACT_SELECTOR_CONFIG_KEY = ArtifactSelector.class.getName();

    public static void main(final String[] args) {
        try {
            final MultiValuedProperties props = new MultiValuedProperties();

            try {
                Main.configure(props);
            } catch (IOException e) {
                System.err.println(String.format("\nFile %s not found where expected. Ensure that %s is put into "
                                                 + "'/config' or '%s/config'.\n", CONFIG_FILE_NAME, CONFIG_FILE_NAME,
                                                 System.getProperty("user.home")));
                System.exit(1);
            }

            Main.setLogging(props);

            // Deal with missing required elements from the configuration file.
            final StringBuilder errorMessage = new StringBuilder();

            // DAO Configuration
            final Map<String, Object> daoConfig = new TreeMap<>();
            final String username = Main.readProperty(props, DB_USERNAME_CONFIG_KEY, errorMessage);
            final String password = Main.readProperty(props, DB_PASSWORD_CONFIG_KEY, errorMessage);
            final String dbUrl = Main.readProperty(props, DB_URL_CONFIG_KEY, errorMessage);

            daoConfig.put("schema", Main.readProperty(props, DB_SCHEMA_CONFIG_KEY, errorMessage));
            daoConfig.put("database", "inventory");

            if (StringUtil.hasText(username) && StringUtil.hasText(password) && StringUtil.hasText(dbUrl)) {
                final ConnectionConfig cc = new ConnectionConfig(null, null, username, password,
                                                                 "org.postgresql.Driver", dbUrl);

                try {
                    DBUtil.createJNDIDataSource("jdbc/inventory", cc);
                } catch (NamingException ne) {
                    throw new IllegalStateException(String.format("Unable to access database: %s", dbUrl), ne);
                }

                daoConfig.put("jndiDataSourceName", "jdbc/inventory");
            }

            final String configuredSQLGenerator = Main.readProperty(props, SQLGENERATOR_CONFIG_KEY, errorMessage);
            if (StringUtil.hasText(configuredSQLGenerator)) {
                daoConfig.put(SQLGENERATOR_CONFIG_KEY, Class.forName(configuredSQLGenerator));
            }

            final String configuredQueryService = Main.readProperty(props, QUERY_SERVICE_CONFIG_KEY, errorMessage);
            final URI resourceID = StringUtil.hasText(configuredQueryService)
                                   ? URI.create(configuredQueryService)
                                   : null;

            final String configuredArtifactSelector = Main.readProperty(props, ARTIFACT_SELECTOR_CONFIG_KEY,
                                                                        errorMessage);
            final ArtifactSelector selector = StringUtil.hasText(configuredArtifactSelector)
                                              ? InventoryUtil.loadPlugin(configuredArtifactSelector)
                                              : null;

            // Missing required elements.  Only the expectedFilters and logging are optional.
            if (errorMessage.length() > 0) {
                System.err.println(
                        String.format("\nConfiguration file %s missing one or more values: %s.\n", CONFIG_FILE_NAME,
                                      errorMessage.toString()));
                System.exit(2);
            }

            InventoryHarvester doit = new InventoryHarvester(daoConfig, resourceID, selector);
            doit.run();
            System.exit(0);
        } catch (Throwable unexpected) {
            log.error("Unexpected failure", unexpected);
            System.err.println("Unexpected failure.  Check log output.");
            System.exit(-1);
        }
    }

    /**
     * Read in the configuration file and load it into the given Properties.
     */
    private static void configure(final MultiValuedProperties properties) throws IOException {
        // Read in the configuration file, and create a usable Map from it.
        final String homeConfigDirectoryPath = String.format("%s/config", System.getProperty("user.home"));

        // Allow some flexibility to override.  This is useful for local testing as one would normally require root
        // access to create /config.
        final String configurationDirectory = new File(homeConfigDirectoryPath).isDirectory()
                                              ? homeConfigDirectoryPath : "/config";

        final String configurationFileLocation = String.format("%s/%s", configurationDirectory, CONFIG_FILE_NAME);

        final InputStream configFileInputStream = new FileInputStream(configurationFileLocation);
        properties.load(configFileInputStream);
    }

    /**
     * Convenience method to acquire required property values, or append to an error message builder if no property
     * exists for the given key.
     *
     * @param properties          The properties to read from.
     * @param key                 The key to look up.
     * @param errorMessageBuilder The error message builder to append to.  Can be null.
     * @return Value of the property, or null if non existent.
     */
    private static String readProperty(final MultiValuedProperties properties, final String key,
                                       final StringBuilder errorMessageBuilder) {
        final String configuredValue = properties.getFirstPropertyValue(key);
        final String returnValue;
        if (!StringUtil.hasText(configuredValue)) {
            if (errorMessageBuilder != null) {
                errorMessageBuilder.append("\n").append(key);
            }
            returnValue = null;
        } else {
            returnValue = configuredValue;
        }

        return returnValue;
    }

    /**
     * Set any package's logging level here.  Allows for finer grained messages.
     *
     * @param applicationProperties The properties read from a file.
     */
    private static void setLogging(final MultiValuedProperties applicationProperties) {
        Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
        Log4jInit.setLevel("org.opencadc", Level.WARN);

        final String loggingKey = ".logging";

        applicationProperties.keySet().stream().filter(key -> key.endsWith(loggingKey))
                             .forEach(key -> Log4jInit.setLevel(key.split(loggingKey)[0],
                                                                Level.toLevel(
                                                                        applicationProperties.getFirstPropertyValue(key)
                                                                                             .toUpperCase())));
    }

    private Main() {
    }
}

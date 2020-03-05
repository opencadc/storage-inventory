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

package org.opencadc.tantar;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.tantar.policy.ResolutionPolicy;


/**
 * Main application entry.  This class expects a tantar.properties file to be available and readable.
 */
public class Main {

    private static final String CONFIG_KEY_PREFIX = "org.opencadc.tantar";

    private static final String BUCKET_KEY = String.format("%s.bucket", CONFIG_KEY_PREFIX);
    private static final String CERTIFICATE_FILE_LOCATION = "/config/cadcproxy.pem";
    private static final String CONFIGURATION_FILE_LOCATION = "/config/tantar.properties";
    private static final String REPORT_ONLY_KEY = String.format("%s.reportOnly", CONFIG_KEY_PREFIX);
    private static final String RESOLUTION_POLICY_CLASS = System.getProperty(ResolutionPolicy.class.getCanonicalName());
    private static final String STORAGE_ADAPTOR_CLASS = System.getProperty(StorageAdapter.class.getCanonicalName());

    private static final Logger LOGGER = Logger.getLogger(Main.class);
    private static final Reporter REPORTER = new Reporter(LOGGER);

    public static void main(final String[] args) {
        Main.REPORTER.start();
        Main.configure();
        Main.setLogging();

        try {
            final StorageAdapter storageAdapter;

            try {
                @SuppressWarnings("unchecked") final Class<StorageAdapter> clazz =
                        (Class<StorageAdapter>) Class.forName(STORAGE_ADAPTOR_CLASS).asSubclass(StorageAdapter.class);
                storageAdapter = clazz.getDeclaredConstructor().newInstance();
            } catch (NoSuchMethodException | ClassNotFoundException | InstantiationException
                    | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(
                        String.format("Failed to load storage adapter: %s", STORAGE_ADAPTOR_CLASS), e);
            }

            final String bucket = System.getProperty(BUCKET_KEY);
            if (!StringUtil.hasLength(bucket)) {
                throw new IllegalArgumentException(String.format("Bucket is mandatory.  Please set the %s property.",
                                                                 BUCKET_KEY));
            }

            final boolean reportOnlyFlag = Boolean.parseBoolean(System.getProperty(REPORT_ONLY_KEY,
                                                                                   Boolean.FALSE.toString()));

            if (reportOnlyFlag) {
                LOGGER.info("*********");
                LOGGER.info("********* Reporting actions only.  No actions will be taken. *********");
                LOGGER.info("*********");
            }

            final ResolutionPolicy resolutionPolicy;

            try {
                @SuppressWarnings("unchecked") final Class<ResolutionPolicy> clazz =
                        (Class<ResolutionPolicy>) Class.forName(RESOLUTION_POLICY_CLASS).asSubclass(
                                ResolutionPolicy.class);
                resolutionPolicy = clazz.getDeclaredConstructor(Reporter.class, boolean.class).newInstance(
                        Main.REPORTER,
                        reportOnlyFlag);
                LOGGER.debug(String.format("Using policy %s.", resolutionPolicy.getClass()));
            } catch (NoSuchMethodException | ClassNotFoundException | InstantiationException
                    | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(String.format("Failed to load resolution policy implementation: %s",
                                                              RESOLUTION_POLICY_CLASS), e);
            }

            final BucketValidator bucketValidator = new BucketValidator(bucket, storageAdapter, resolutionPolicy,
                                                                        SSLUtil.createSubject(
                                                                                new File(System.getProperty(
                                                                                        CERTIFICATE_FILE_LOCATION))));
            bucketValidator.validate();
        } catch (IllegalStateException e) {
            // IllegalStateExceptions are thrown for missing but required configuration.
            LOGGER.fatal(e.getMessage());
            System.exit(3);
        } catch (Exception e) {
            // Used to catch everything else, such as a RuntimeException when a file cannot be obtained and put.
            LOGGER.fatal(e.getMessage());
            System.exit(4);
        } finally {
            Main.REPORTER.end();
        }
    }

    /**
     * Read in the configuration file and load it into the System properties.
     */
    private static void configure() {
        final Properties properties = new Properties();

        try {
            final Reader configFileReader = new FileReader(CONFIGURATION_FILE_LOCATION);
            properties.load(configFileReader);
            final Properties existingProperties = System.getProperties();
            existingProperties.putAll(properties);
            System.setProperties(existingProperties);
        } catch (FileNotFoundException e) {
            LOGGER.fatal(
                    String.format("Unable to locate configuration file.  Expected it to be at %s.",
                                  CONFIGURATION_FILE_LOCATION));
            System.exit(1);
        } catch (IOException e) {
            LOGGER.fatal(String.format("Unable to read file located at %s.", CONFIGURATION_FILE_LOCATION));
            System.exit(2);
        }
    }

    private static void setLogging() {
        Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
        Log4jInit.setLevel("org.opencadc", Level.WARN);

        final String loggingKey = ".logging";

        System.getProperties().entrySet().stream().filter(entry -> entry.getKey().toString().endsWith(loggingKey))
              .forEach(entry -> Log4jInit.setLevel(entry.getKey().toString().split(loggingKey)[0],
                                                   Level.toLevel(entry.getValue().toString().toUpperCase())));
    }
}

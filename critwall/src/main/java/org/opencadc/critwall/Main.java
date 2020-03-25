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

package org.opencadc.critwall;

import ca.nrc.cadc.util.Log4jInit;

import ca.nrc.cadc.util.StringUtil;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.inventory.storage.StorageAdapter;

/**
 * Main entry point for critwall.
 * 
 * @author pdowler
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class);
    private static final String CONFIGURATION_FILE_LOCATION = "/config/critwall.properties";
    private static final String SCHEMA_CONFIG_KEY = "org.opencadc.critwall.db.schema";
    private static final String DATABASE_CONFIG_KEY = "org.opencadc.critwall.db.url";
    private static final String BUCKETS_CONFIG_KEY = "org.opencadc.critwall.buckets";
    private static final String NTHREADS_CONFIG_KEY = "org.opencadc.critwall.threads";
    private static final String LOCATOR_SERVICE_CONFIG_KEY = "org.opencadc.critwall.locatorService";
    private static final String LOGGING_CONFIG_KEY = "org.opencadc.critwall.logging";

    public static void main(String[] args) {
        try {
            Log4jInit.setLevel("org.opencadc", Level.WARN);
            Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);

            Properties props = readConfig();

            // get log level from config
            String logCfg = props.getProperty(LOGGING_CONFIG_KEY);
            Level cfg = Level.INFO;
            if (StringUtil.hasLength(logCfg)) {
                cfg = Level.toLevel(logCfg);
            }

            Log4jInit.setLevel("org.opencadc.inventory", cfg);
            Log4jInit.setLevel("org.opencadc.tap", cfg);
            Log4jInit.setLevel("org.opencadc.reg", cfg);
            Log4jInit.setLevel("org.opencadc.critwall", cfg);

            log.debug("log level: " + cfg);
            log.debug("properties: " + props.toString());

            // parse config file and populate/assign these
            // which values from the config go in here?
            Map<String,Object> daoConfig = new TreeMap<>();
            daoConfig.put("schema", getSchema(props));
            daoConfig.put("database", getDatabase(props));
            daoConfig.put("jndiDataSourceName", getDatabase(props));

            StorageAdapter localStorage = getStorageAdapter(props);
            log.debug("storage adapter: " + localStorage);

            URI resourceID = getGlobalResourceId(props);
            log.debug("resourceID: " + resourceID.toString());

            BucketSelector selector = getBucketSelector(props);

            int nthreads = getNthreads(props);
            log.debug("nthreads: " + nthreads);
            
            FileSync doit = new FileSync(daoConfig, localStorage, resourceID, selector, nthreads);
            doit.run();
            System.exit(0);
        } catch (Throwable unexpected) {
            log.error("unexpected failure", unexpected);
            System.exit(-1);
        }
        log.debug("finished critwall run.");
    }
    
    private Main() { 
    }


    /**
     * Read in the configuration file and load it into the System properties.
     */
    private static Properties readConfig() {
        final Properties properties = new Properties();

        try {
            final Reader configFileReader = new FileReader(CONFIGURATION_FILE_LOCATION);
            properties.load(configFileReader);
            System.setProperties(properties);
        } catch (FileNotFoundException e) {
            log.fatal(
                String.format("Unable to locate configuration file.  Expected it to be at %s.",
                    CONFIGURATION_FILE_LOCATION));
            System.exit(1);
        } catch (IOException e) {
            log.fatal(String.format("Unable to read file located at %s.", CONFIGURATION_FILE_LOCATION));
            System.exit(2);
        }

        return properties;
    }

    private static String getSchema(Properties config) {
        String schema = config.getProperty(SCHEMA_CONFIG_KEY);
        log.debug("schema: " + schema);
        if (!StringUtil.hasLength(schema)) {
            throw new IllegalStateException("schema not specified in critwall.properties");
        }
        return schema;
    }

    private static String getDatabase(Properties config) {
        String schema = config.getProperty(DATABASE_CONFIG_KEY);
        if (!StringUtil.hasLength(schema)) {
            throw new IllegalStateException("schema not specified in critwall.properties");
        }
        return schema;
    }


    private static StorageAdapter getStorageAdapter(Properties config) {
        String adapterKey = StorageAdapter.class.getName();
        String adapterClass = config.getProperty(adapterKey);
        log.debug("adapter class: " + adapterClass);
        if (StringUtil.hasLength(adapterClass)) {
            try {
                Class c = Class.forName(adapterClass);
                Object o = c.newInstance();
                StorageAdapter sa = (StorageAdapter) o;
                log.debug("StorageAdapter: " + sa);
                return sa;
            } catch (Throwable t) {
                throw new IllegalStateException("failed to load storage adapter: " + adapterClass, t);
            }
        } else {
            throw new IllegalStateException("no storage adapter specified in critwall.properties");
        }
    }

    private static URI getGlobalResourceId(Properties config) {
        String locatorResourceIdStr = config.getProperty(LOCATOR_SERVICE_CONFIG_KEY);
        URI resourceID = null;

        if (StringUtil.hasLength(locatorResourceIdStr)) {
            try {
                resourceID = new URI(locatorResourceIdStr);
            } catch (URISyntaxException us) {
                throw new IllegalStateException("invalid locator service in critwall.properties: " + locatorResourceIdStr);
            }
        } else {
            throw new IllegalStateException("locator service not specified in critwall.properties");
        }
        return resourceID;
    }

    private static int getNthreads(Properties config) {
        String nthreadStr = config.getProperty(NTHREADS_CONFIG_KEY);
        int nthreads = -1;
        if (StringUtil.hasLength(nthreadStr)) {
            // TODO: if there's no int in the string, it's not caught
            nthreads = Integer.parseInt(nthreadStr);
        } else {
            // default of 1
            nthreads = 1;
        }
        return nthreads;
    }

    private static BucketSelector getBucketSelector(Properties config) {
        String bucketSelectorPrefix = config.getProperty(BUCKETS_CONFIG_KEY);
        BucketSelector bucketSel = null;

        if (StringUtil.hasLength(bucketSelectorPrefix)) {
             bucketSel = new BucketSelector(bucketSelectorPrefix);
        } else {
            throw new IllegalStateException("bucket selector not specified in critwall.properties");
        }
        return bucketSel;
    }

}

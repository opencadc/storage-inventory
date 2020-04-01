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

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;

import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.StringUtil;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.naming.NamingException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.storage.StorageAdapter;

/**
 * Main entry point for critwall.
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class);
    private static final String CONFIG_PREFIX = Main.class.getPackage().getName();
    private static final String SQLGENERATOR_CONFIG_KEY = SQLGenerator.class.getName();
    private static final String DB_SCHEMA_CONFIG_KEY = CONFIG_PREFIX + ".db.schema";
    private static final String DB_CONFIG_KEY = CONFIG_PREFIX + ".db.url";
    private static final String DB_USERNAME_CONFIG_KEY = CONFIG_PREFIX + ".db.username";
    private static final String DB_PASSWORD_CONFIG_KEY = CONFIG_PREFIX + ".db.password";
    private static final String BUCKETSEL_CONFIG_KEY = CONFIG_PREFIX + ".buckets";
    private static final String NTHREADS_CONFIG_KEY = CONFIG_PREFIX + ".threads";
    private static final String LOCATOR_SERVICE_CONFIG_KEY = CONFIG_PREFIX + ".locatorService";
    private static final String LOGGING_CONFIG_KEY = CONFIG_PREFIX + ".logging";

    public static void main(String[] args) {
        try {
            Log4jInit.setLevel("org.opencadc", Level.WARN);
            Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);

            PropertiesReader pr = new PropertiesReader("critwall.properties");
            MultiValuedProperties props = pr.getAllProperties();

            if (log.isDebugEnabled()) {
                log.debug("critwall.properties:");
                Set<String> keys = props.keySet();
                for (String key : keys) {
                    log.debug("    " + key + " = " + props.getProperty(key));
                }
            }

            // Set up logging before parsing file otherwise it's hard to report errors sanely
            String logCfg = props.getFirstPropertyValue(LOGGING_CONFIG_KEY);
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

            // parse config file
            StringBuilder errMsg = new StringBuilder();

            String schema = props.getFirstPropertyValue(DB_SCHEMA_CONFIG_KEY);
            if (!StringUtil.hasLength(schema)) {
                errMsg.append(DB_SCHEMA_CONFIG_KEY + " ");
            }

            String dbUrl = props.getFirstPropertyValue(DB_CONFIG_KEY);
            if (!StringUtil.hasLength(dbUrl)) {
                errMsg.append(DB_CONFIG_KEY + " ");
            }

            String generatorName = props.getFirstPropertyValue(SQLGENERATOR_CONFIG_KEY);
            if (!StringUtil.hasLength(generatorName)) {
                errMsg.append(SQLGENERATOR_CONFIG_KEY + " ");
            }

            String username = props.getFirstPropertyValue(DB_USERNAME_CONFIG_KEY);
            if (!StringUtil.hasLength(username)) {
                errMsg.append(DB_USERNAME_CONFIG_KEY + " ");
            }

            String password = props.getFirstPropertyValue(DB_PASSWORD_CONFIG_KEY);
            if (!StringUtil.hasLength(password)) {
                errMsg.append(DB_PASSWORD_CONFIG_KEY + " ");
            }

            String adapterClass = props.getFirstPropertyValue(StorageAdapter.class.getName());
            if (!StringUtil.hasLength(adapterClass)) {
                errMsg.append(StorageAdapter.class.getName() + " ");
            }

            String locatorResourceIdStr = props.getFirstPropertyValue(LOCATOR_SERVICE_CONFIG_KEY);
            if (!StringUtil.hasLength(locatorResourceIdStr)) {
                errMsg.append(LOCATOR_SERVICE_CONFIG_KEY + " ");
            }

            String bucketSelectorPrefix = props.getFirstPropertyValue(BUCKETSEL_CONFIG_KEY);
            if (!StringUtil.hasLength(bucketSelectorPrefix)) {
                errMsg.append(BUCKETSEL_CONFIG_KEY + " ");
            }

            String nthreadStr = props.getFirstPropertyValue(NTHREADS_CONFIG_KEY);
            if (!StringUtil.hasLength(nthreadStr)) {
                errMsg.append(NTHREADS_CONFIG_KEY + " ");
            }

            // Everything is required
            if (errMsg.length() > 0) {
                throw new IllegalStateException("critwall.properties missing one or more values: " + errMsg);
            }

            // populate/assign values to pass to FileSync
            Map<String, Object>daoConfig = new TreeMap<>();
            daoConfig.put("schema", schema);
            daoConfig.put("database", dbUrl);

            try {
                daoConfig.put(SQLGENERATOR_CONFIG_KEY, (Class<SQLGenerator>)Class.forName(generatorName));
            } catch (Exception ex) {
                throw new IllegalStateException("cannot instantiate SQLGenerator: " + generatorName, ex);
            }
            log.debug("SQL generator class made");

            String jndiSourceName = "jdbc/critwall";
            ConnectionConfig cc = new ConnectionConfig(null, null,
                username,
                password,
                "org.postgresql.Driver",
                dbUrl);

            try {
                DBUtil.createJNDIDataSource(jndiSourceName, cc);
            } catch (NamingException ne) {
                throw new IllegalStateException("unable to access database: " + dbUrl, ne);
            }
            daoConfig.put("jndiDataSourceName", jndiSourceName);
            log.debug("JNDIDataSource: " + jndiSourceName);

            StorageAdapter localStorage = null;
            try {
                Class c = Class.forName(adapterClass);
                Object o = c.newInstance();
                localStorage = (StorageAdapter) o;
                log.debug("StorageAdapter: " + localStorage);
            } catch (Throwable t) {
                throw new IllegalStateException("failed to create storage adapter: " + adapterClass, t);
            }
            log.debug("storage adapter: " + localStorage);

            URI resourceID = null;
            try {
                resourceID = new URI(locatorResourceIdStr);
            } catch (URISyntaxException us) {
                throw new IllegalStateException("invalid locator service in critwall.properties: " + locatorResourceIdStr);
            }
            log.debug("resourceID: " + resourceID.toString());

            int nthreads = Integer.parseInt(nthreadStr);
            log.debug("nthreads: " + nthreads);

            BucketSelector bucketSel = new BucketSelector(bucketSelectorPrefix);
            log.debug("bucket selector: " + bucketSel);

            FileSync doit = new FileSync(daoConfig, localStorage, resourceID, bucketSel, nthreads);
            doit.run();
            System.exit(0);
        } catch (Throwable unexpected) {
            log.error("failure", unexpected);
            System.exit(-1);

        }
        log.debug("finished critwall run.");
    }

    private Main() {
    }

}

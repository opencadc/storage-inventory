/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2026.                            (c) 2026.
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

package org.opencadc.eos;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.StringUtil;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.storage.eos.EosStorageAdapter;

/**
 *
 * @author pdowler
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class);

    private static final String CONFIGT_FILE = "eos-scanner.properties";
    private static final String CONFIG_BASE = Main.class.getPackage().getName();
    private static final String LOGGING_KEY = CONFIG_BASE + ".logging";
    private static final String DIRSCAN_KEY = CONFIG_BASE + ".dirScanOnly";
    private static final String BUCKET_KEY = CONFIG_BASE + ".bucket";
    
    private static final String DB_SCHEMA_KEY = CONFIG_BASE + ".schema";
    private static final String DB_USERNAME_KEY = CONFIG_BASE + ".username";
    private static final String DB_PASSWORD_KEY = CONFIG_BASE + ".password";
    private static final String JDBC_URL_KEY = CONFIG_BASE + ".url";
    
    private Main() {
    }
    
    public static void main(final String[] args) {
            
        try {
            Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
            Log4jInit.setLevel("org.opencadc", Level.WARN);
            PropertiesReader pr = new PropertiesReader(CONFIGT_FILE);
            MultiValuedProperties props = pr.getAllProperties();

            String logCfg = props.getFirstPropertyValue(LOGGING_KEY);
            Level logLevel = Level.INFO;
            if (StringUtil.hasLength(logCfg)) {
                logLevel = Level.toLevel(logCfg);
            }
            Log4jInit.setLevel("org.opencadc.inventory", logLevel);
            Log4jInit.setLevel("org.opencadc.eos", logLevel);
            
            // parse config file
            StringBuilder errMsg = new StringBuilder();

            String schema = props.getFirstPropertyValue(DB_SCHEMA_KEY);
            if (!StringUtil.hasLength(schema)) {
                errMsg.append(DB_SCHEMA_KEY).append(" ");
            }

            String dbUrl = props.getFirstPropertyValue(JDBC_URL_KEY);
            if (!StringUtil.hasLength(dbUrl)) {
                errMsg.append(JDBC_URL_KEY).append(" ");
            }

            String username = props.getFirstPropertyValue(DB_USERNAME_KEY);
            if (!StringUtil.hasLength(username)) {
                errMsg.append(DB_USERNAME_KEY).append(" ");
            }

            String password = props.getFirstPropertyValue(DB_PASSWORD_KEY);
            if (!StringUtil.hasLength(password)) {
                errMsg.append(DB_PASSWORD_KEY).append(" ");
            }
            
            String bucket = props.getFirstPropertyValue(BUCKET_KEY);
            if (!StringUtil.hasLength(bucket)) {
                errMsg.append(BUCKET_KEY).append(" ");
            }

            boolean dirScanOnly = false;
            String str = props.getFirstPropertyValue(DIRSCAN_KEY);
            if (str != null && "true".equals(str)) {
                dirScanOnly = true;
            }
            if (errMsg.length() > 0) {
                throw new IllegalStateException(CONFIGT_FILE + " missing one or more values: " + errMsg);
            }
            
            // populate/assign values to pass to FileSync
            Map<String, Object> daoConfig = new TreeMap<>();
            daoConfig.put("schema", schema);
            ConnectionConfig cc = new ConnectionConfig(null, null, username, password,
                "org.postgresql.Driver", dbUrl);
            
            EosScanner doit = new EosScanner(bucket, daoConfig, cc, dirScanOnly);
            
            
        } catch (InvalidConfigException e) {
            log.fatal(e.getMessage(), e);
            System.exit(2);
        } catch (Exception e) {
            // Used to catch everything else, such as a RuntimeException when a file cannot be obtained and put.
            log.fatal(e.getMessage(), e);
            System.exit(1);
        }
    }
}

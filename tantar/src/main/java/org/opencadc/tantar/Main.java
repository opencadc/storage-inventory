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
 *
 ************************************************************************
 */

package org.opencadc.tantar;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.StringUtil;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.tantar.policy.ResolutionPolicy;

/**
 * Main application entry.  This class expects a tantar.properties file to be available and readable.
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class);
    
    private static final String CONFIG_BASE = Main.class.getPackage().getName();
    private static final String LOGGING_KEY = CONFIG_BASE + ".logging";
    private static final String REPORT_ONLY_KEY = CONFIG_BASE + ".reportOnly";
    
    private static final String BUCKETS_KEY = CONFIG_BASE + ".buckets";
    private static final String INCLUDE_RECOVERABLE_KEY = CONFIG_BASE + ".includeRecoverable";
    private static final String RECOVERABLE_NS_KEY = CONFIG_BASE + ".recoverableNamespace";
    private static final String PURGE_NS_KEY = CONFIG_BASE + ".purgeNamespace";

    private static final String POLICY_KEY = ResolutionPolicy.class.getName();
    private static final String GENERATOR_KEY = SQLGenerator.class.getName();
    
    private static final String DB_SCHEMA_KEY = CONFIG_BASE + ".inventory.schema";
    private static final String DB_USERNAME_KEY = CONFIG_BASE + ".inventory.username";
    private static final String DB_PASSWORD_KEY = CONFIG_BASE + ".inventory.password";
    private static final String JDBC_URL_KEY = CONFIG_BASE + ".inventory.url";
    
    public static void main(final String[] args) {
            
        try {
            Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
            Log4jInit.setLevel("org.opencadc", Level.WARN);
            PropertiesReader pr = new PropertiesReader("tantar.properties");
            MultiValuedProperties props = pr.getAllProperties();

            String logCfg = props.getFirstPropertyValue(LOGGING_KEY);
            Level logLevel = Level.INFO;
            if (StringUtil.hasLength(logCfg)) {
                logLevel = Level.toLevel(logCfg);
            }
            Log4jInit.setLevel("org.opencadc.inventory", logLevel);
            Log4jInit.setLevel("org.opencadc.tantar", logLevel);
            
            // optional client cert
            File certFile = new File(System.getProperty("user.home") + "/.ssl/cadcproxy.pem");
            Subject s = AuthenticationUtil.getAnonSubject();
            if (certFile.exists()) {
                s = SSLUtil.createSubject(certFile);
            }
            
            StorageAdapter storageAdapter = null;
            String storageAdapterClassName = props.getFirstPropertyValue(StorageAdapter.class.getName());
            if (StringUtil.hasLength(storageAdapterClassName)) {
                storageAdapter = InventoryUtil.loadPlugin(storageAdapterClassName);
            } else {
                throw new InvalidConfigException("required config property missing: " + StorageAdapter.class.getName());
            }
            
            ResolutionPolicy validationPolicy = null;
            String policyClassName = props.getFirstPropertyValue(POLICY_KEY);
            policyClassName = addPackage(policyClassName);
            if (StringUtil.hasLength(policyClassName)) {
                validationPolicy = InventoryUtil.loadPlugin(policyClassName);
            } else {
                throw new InvalidConfigException("required config property missing: " + ResolutionPolicy.class.getName());
            }
            
            final String rawBucketRange = props.getFirstPropertyValue(BUCKETS_KEY);
            
            String reportOnlyStr = props.getFirstPropertyValue(REPORT_ONLY_KEY);
            final boolean reportOnly = StringUtil.hasText(reportOnlyStr) && Boolean.parseBoolean(reportOnlyStr);
            
            String rawIncludeRecovery = props.getFirstPropertyValue(INCLUDE_RECOVERABLE_KEY);
            final boolean includeRecoverable = StringUtil.hasText(rawIncludeRecovery) && Boolean.parseBoolean(rawIncludeRecovery);
            
            List<String> rawRecNS = props.getProperty(RECOVERABLE_NS_KEY);
            List<Namespace> recoverableNS = new ArrayList<>();
            for (String ns : rawRecNS) {
                recoverableNS.add(new Namespace(ns));
                log.info(RECOVERABLE_NS_KEY + " = " + ns);
            }
            storageAdapter.setRecoverableNamespaces(recoverableNS);
            
            List<String> rawPurgeNS = props.getProperty(PURGE_NS_KEY);
            List<Namespace> purgeNS = new ArrayList<>();
            for (String ns : rawPurgeNS) {
                purgeNS.add(new Namespace(ns));
                log.info(PURGE_NS_KEY + " = " + ns);
            }
            storageAdapter.setPurgeNamespaces(purgeNS);
            
            final Map<String,Object> daoConfig = new TreeMap<>();
            String sqlGeneratorClass = props.getFirstPropertyValue(SQLGenerator.class.getName());
            if (StringUtil.hasLength(sqlGeneratorClass)) {
                daoConfig.put(SQLGenerator.class.getName(), Class.forName(sqlGeneratorClass));
            } else {
                throw new InvalidConfigException("required config property missing: " + SQLGenerator.class.getName());
            }

            // database
            final String jdbcDriverClassname = "org.postgresql.Driver";
            final String schemaName = props.getFirstPropertyValue(DB_SCHEMA_KEY);
            if (StringUtil.hasLength(schemaName)) {
                daoConfig.put("invSchema", schemaName);
                daoConfig.put("genSchema", schemaName);
            } else {
                throw new InvalidConfigException("required config property missing: " + DB_SCHEMA_KEY);
            }

            final String dbUsername = props.getFirstPropertyValue(DB_USERNAME_KEY);
            final String dbPassword = props.getFirstPropertyValue(DB_PASSWORD_KEY);
            final String jdbcURL = props.getFirstPropertyValue(JDBC_URL_KEY);
            log.debug("database connection: " + jdbcURL);
            ConnectionConfig cc = new ConnectionConfig(null, null, dbUsername, dbPassword, jdbcDriverClassname, jdbcURL);
            
            BucketValidator bucketValidator = new BucketValidator(daoConfig, cc, storageAdapter, validationPolicy, rawBucketRange, reportOnly);
            bucketValidator.setIncludeRecoverable(includeRecoverable);
            
            Subject.doAs(s, (PrivilegedExceptionAction<Object>) () -> {
                bucketValidator.validate();
                return null;
            });
            
        } catch (InvalidConfigException e) {
            log.fatal(e.getMessage(), e);
            System.exit(2);
        } catch (Exception e) {
            // Used to catch everything else, such as a RuntimeException when a file cannot be obtained and put.
            log.fatal(e.getMessage(), e);
            System.exit(1);
        }
    }
    
    private static String addPackage(String cname) {
        if (cname.indexOf('.') == -1) {
            return ResolutionPolicy.class.getPackage().getName() + "." + cname;
        }
        throw new InvalidConfigException("using fully qualified class name for ResolutionPolicy is obsolete: " + cname);
    }
}


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


import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.StringUtil;

import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import javax.naming.NamingException;

import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.tantar.policy.ResolutionPolicy;
import org.opencadc.tantar.policy.ResolutionPolicyFactory;


/**
 * Main class to issue iterator requests to the Storage Adaptor and verify the contents.
 * Policies for conflicts, meaning situations where there is a discrepancy between what the Storage Adaptor relays
 * what is currently stored and what the Storage Inventory declares, are set at the properties level.
 */
public class BucketValidator implements Runnable {

    private static final String JNDI_ARTIFACT_DATASOURCE_NAME = "jdbc/inventory";
    private static final String BUCKET_KEY = "org.opencadc.tantar.bucket";
    private static final String POLICY_KEY = "org.opencadc.tantar.policy";
    private static final String SQL_GEN_KEY = SQLGenerator.class.getName();
    private static final String SCHEMA_KEY = SQLGenerator.class.getPackage().getName() + ".schema";
    private static final String STORAGE_ADAPTOR_CLASS = System.getProperty(StorageAdapter.class.getCanonicalName());

    private static final String JDBC_CONFIG_KEY_PREFIX = "org.opencadc.inventory.db";
    private static final String JDBC_USERNAME_KEY = String.format("%s.username", JDBC_CONFIG_KEY_PREFIX);
    private static final String JDBC_PASSWORD_KEY = String.format("%s.password", JDBC_CONFIG_KEY_PREFIX);
    private static final String JDBC_URL_KEY = String.format("%s.url", JDBC_CONFIG_KEY_PREFIX);
    private static final String JDBC_DRIVER_CLASSNAME = "org.postgresql.Driver";

    private final String bucket;
    private final StorageAdapter storageAdapter;
    private final BucketIteratorComparator bucketIteratorComparator;


    private BucketValidator(final String bucket, final StorageAdapter storageAdapter,
                            final BucketIteratorComparator bucketIteratorComparator) {
        this.bucket = bucket;
        this.storageAdapter = storageAdapter;
        this.bucketIteratorComparator = bucketIteratorComparator;
    }

    @SuppressWarnings("unchecked")
    public static BucketValidator create(final Reporter reporter) {
        final StorageAdapter storageAdapter;

        try {
            final Class<StorageAdapter> clazz =
                    (Class<StorageAdapter>) Class.forName(STORAGE_ADAPTOR_CLASS).asSubclass(StorageAdapter.class);
            storageAdapter = clazz.getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException | ClassNotFoundException | InstantiationException
                | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(String.format("Failed to load storage adapter: %s", STORAGE_ADAPTOR_CLASS),
                                            e);
        }

        final String bucket = System.getProperty(BUCKET_KEY);
        if (!StringUtil.hasLength(bucket)) {
            throw new IllegalArgumentException(String.format("Bucket is mandatory.  Please set the %s property.",
                                                             BUCKET_KEY));
        }

        final String policy = System.getProperty(POLICY_KEY);
        if (!StringUtil.hasLength(policy)) {
            throw new IllegalArgumentException(String.format("Policy is mandatory.  Please set the %s property.",
                                                             POLICY_KEY));
        }

        return new BucketValidator(bucket, storageAdapter,
                                   new BucketIteratorComparator(ResolutionPolicyFactory.createPolicy(policy),
                                                                reporter));
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        if (StringUtil.hasLength(bucket)) {
            try {
                validate(bucket);
            } catch (TransientException e) {
                throw new IllegalStateException("The Storage Adapter is not available.  Try again later.", e);
            } catch (StorageEngageException e) {
                throw new IllegalStateException("The back end storage is not available.", e);
            }
        } else {
            throw new IllegalStateException(
                    String.format("Configuration for Bucket name specified by %s is required.", BUCKET_KEY));
        }
    }

    Iterator<StorageMetadata> iterateStorage(final String bucket) throws TransientException, StorageEngageException {
        return storageAdapter.iterator(bucket);
    }

    Iterator<Artifact> iterateInventory(final String bucket) {
        // return getArtifactDAO().iterate(bucket)
        return null;
    }

    void validate(final String bucket) throws TransientException, StorageEngageException {
        final Iterator<StorageMetadata> storageMetadataIterator = iterateStorage(bucket);
        final Iterator<Artifact> inventoryIterator = iterateInventory(bucket);

        this.bucketIteratorComparator.compare(inventoryIterator, storageMetadataIterator);

        // TODO
        // TODO: List compare and validation.
        // TODO
    }

    ArtifactDAO getArtifactDAO() {
        final ArtifactDAO dao = new ArtifactDAO();
        dao.setConfig(getDAOConfig());
        return dao;
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
            final Properties systemProperties = System.getProperties();

            final ConnectionConfig cc = new ConnectionConfig(null, null,
                                                             systemProperties.getProperty(JDBC_USERNAME_KEY),
                                                             systemProperties.getProperty(JDBC_PASSWORD_KEY),
                                                             JDBC_DRIVER_CLASSNAME,
                                                             systemProperties.getProperty(JDBC_URL_KEY));
            try {
                DBUtil.createJNDIDataSource(JNDI_ARTIFACT_DATASOURCE_NAME, cc);
            } catch (NamingException ne) {
                throw new IllegalStateException("Unable to access Inventory Database.", ne);
            }
        }

        return JNDI_ARTIFACT_DATASOURCE_NAME;
    }

    private Map<String, Object> getDAOConfig() {
        final Map<String, Object> config = new HashMap<>();
        final String sqlGeneratorClassName = System.getProperty(SQL_GEN_KEY, SQLGenerator.class.getCanonicalName());
        try {
            config.put(SQL_GEN_KEY, Class.forName(sqlGeneratorClassName));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("could not load SQLGenerator class: " + e.getMessage(), e);
        }

        config.put("jndiDataSourceName", registerDataSource());

        final String schemaName = System.getProperty(SCHEMA_KEY);

        if (StringUtil.hasLength(schemaName)) {
            config.put("schema", schemaName);
        } else {
            throw new IllegalStateException(
                    String.format("A value for %s is required in minoc.properties", SCHEMA_KEY));
        }

        config.put("database", "inventory");

        return config;
    }
}

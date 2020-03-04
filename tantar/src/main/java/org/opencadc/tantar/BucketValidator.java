
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

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.StringUtil;

import java.io.File;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import javax.naming.NamingException;
import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.storage.NewArtifact;
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
public class BucketValidator implements ValidateEventListener {

    private static final Logger LOGGER = Logger.getLogger(BucketValidator.class);

    private static final String JNDI_ARTIFACT_DATASOURCE_NAME = "jdbc/inventory";
    private static final String CONFIG_KEY_PREFIX = "org.opencadc.tantar";
    private static final String BUCKET_KEY = String.format("%s.bucket", CONFIG_KEY_PREFIX);
    private static final String CERTFILE_KEY = String.format("%s.cert", CONFIG_KEY_PREFIX);
    private static final String RESOLUTION_POLICY_KEY = String.format("%s.resolutionPolicy", CONFIG_KEY_PREFIX);
    private static final String REPORT_ONLY_KEY = String.format("%s.reportOnly", CONFIG_KEY_PREFIX);
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
    private final ResolutionPolicy resolutionPolicy;
    private final Subject runUser;

    // The ArtifactDAO can be set later to allow lazy loading.
    private ArtifactDAO artifactDAO;


    BucketValidator(final String bucket, final StorageAdapter storageAdapter, final ResolutionPolicy resolutionPolicy,
                    final Subject runUser) {
        this.bucket = bucket;
        this.storageAdapter = storageAdapter;
        this.resolutionPolicy = resolutionPolicy;
        this.runUser = runUser;

        this.resolutionPolicy.subscribe(this);
    }

    /**
     * Factory style creation.  This relies on the System Properties being properly set by the caller.
     *
     * @param reporter The Reporter object that will be used to report steps along the way.
     * @return A BucketValidator instance.
     */
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

        final String policy = System.getProperty(RESOLUTION_POLICY_KEY);
        if (!StringUtil.hasLength(policy)) {
            throw new IllegalArgumentException(String.format("Policy is mandatory.  Please set the %s property.",
                                                             RESOLUTION_POLICY_KEY));
        }

        final boolean reportOnlyFlag = Boolean.parseBoolean(System.getProperty(REPORT_ONLY_KEY,
                                                                               Boolean.FALSE.toString()));

        if (reportOnlyFlag) {
            LOGGER.info("*********");
            LOGGER.info("********* Reporting actions only.  No actions will be taken. *********");
            LOGGER.info("*********");
        }

        final ResolutionPolicy resolutionPolicy = ResolutionPolicyFactory.createPolicy(policy, reporter,
                                                                                       reportOnlyFlag);

        LOGGER.debug(String.format("Using policy %s.", resolutionPolicy.getClass()));

        return new BucketValidator(bucket, storageAdapter, resolutionPolicy,
                                   SSLUtil.createSubject(new File(System.getProperty(CERTFILE_KEY))));
    }

    /**
     * Main functionality.  This will obtain the iterators necessary to validate, and delegate to the Policy to take
     * action and/or report.
     *
     * @throws Exception Pass up any errors to the caller, which is most likely the Main.
     */
    void validate() throws Exception {
        final Iterator<StorageMetadata> storageMetadataIterator = iterateStorage();
        final Iterator<Artifact> inventoryIterator = iterateInventory();

        LOGGER.debug("START validating iterators.");

        Artifact unresolvedArtifact = null;
        StorageMetadata unresolvedStorageMetadata = null;

        while ((inventoryIterator.hasNext() || unresolvedArtifact != null)
               && (storageMetadataIterator.hasNext() || unresolvedStorageMetadata != null)) {
            final Artifact artifact = (unresolvedArtifact == null) ? inventoryIterator.next() : unresolvedArtifact;
            final StorageMetadata storageMetadata =
                    (unresolvedStorageMetadata == null) ? storageMetadataIterator.next() : unresolvedStorageMetadata;

            LOGGER.debug(String.format("Comparing Inventory Storage Location %s with Storage Adapter Location %s",
                                       artifact.storageLocation, storageMetadata.getStorageLocation()));
            final int comparison = artifact.storageLocation.compareTo(storageMetadata.getStorageLocation());

            if (comparison == 0) {
                // Same storage location.  Test the metadata.
                unresolvedArtifact = null;
                unresolvedStorageMetadata = null;
                resolutionPolicy.resolve(artifact, storageMetadata);
            } else if (comparison > 0) {
                // Exists in Storage but not in inventory.
                unresolvedArtifact = artifact;
                unresolvedStorageMetadata = null;
                resolutionPolicy.resolve(null, storageMetadata);
            } else {
                // Exists in Inventory but not in Storage.
                unresolvedArtifact = null;
                unresolvedStorageMetadata = storageMetadata;
                resolutionPolicy.resolve(artifact, null);
            }
        }

        // *** Perform some mop up.  These loops will take effect when one of the iterators is empty but the other is
        // not, like in the case of a fresh load from another site.
        while (inventoryIterator.hasNext()) {
            resolutionPolicy.resolve(inventoryIterator.next(), null);
        }

        while (storageMetadataIterator.hasNext()) {
            resolutionPolicy.resolve(null, storageMetadataIterator.next());
        }

        LOGGER.debug("END validating iterators.");
    }

    /**
     * Obtain a file from its Storage Location as deemed by the Artifact.  This is used by the Policy when the
     * Artifact is determined to be accurate.
     * <p />
     * A RuntimeException is thrown for any issues that occur within the Threads.
     *
     * @param artifact The base artifact.  This MUST have a Storage Location.
     * @throws Exception Anything IO/Thread related.
     */
    @Override
    public void retrieveFile(final Artifact artifact) throws Exception {
        try (final PipedInputStream pipedInputStream = new PipedInputStream();
             final PipedOutputStream pipedOutputStream = new PipedOutputStream(pipedInputStream)) {
            final Thread writeThread = new Thread(() -> {
                try {
                    final NewArtifact newArtifact = new NewArtifact(artifact.getURI());
                    newArtifact.contentLength = artifact.getContentLength();
                    newArtifact.contentChecksum = artifact.getContentChecksum();

                    storageAdapter.put(newArtifact, pipedInputStream);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            final Thread readThread = new Thread(() -> {
                try {
                    storageAdapter.get(artifact.storageLocation, pipedOutputStream);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            readThread.start();
            writeThread.start();

            readThread.join();
            writeThread.join();
        }
    }

    /**
     * Remove a file based on the determination of the Policy.
     *
     * @param storageMetadata The StorageMetadata containing a Storage Location to remove.
     * @throws Exception Covering numerous exceptions from the StorageAdapter.
     */
    @Override
    public void deleteFile(final StorageMetadata storageMetadata) throws Exception {
        storageAdapter.delete(storageMetadata.getStorageLocation());
    }

    @Override
    public void addArtifact(StorageMetadata storageMetadata) {
        final Artifact artifact = new Artifact(storageMetadata.artifactURI, storageMetadata.getContentChecksum(),
                                               storageMetadata.contentLastModified, storageMetadata.getContentLength());

        artifact.storageLocation = storageMetadata.getStorageLocation();
        artifact.contentType = storageMetadata.contentType;
        artifact.contentEncoding = storageMetadata.contentEncoding;

        getArtifactDAO().put(artifact, false);
    }

    @Override
    public void deleteArtifact(Artifact artifact) {
        getArtifactDAO().delete(artifact.getID());
    }

    /**
     * Iterate over the StorageMetadata instances from the Storage Adapter.  The consumer of this Iterator will assume
     * that the StorageLocation is never null.
     * <p/>
     * Tests can override this for convenience.
     *
     * @return Iterator instance of StorageMetadata objects
     *
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException     If an unexpected, temporary exception occurred.
     */
    Iterator<StorageMetadata> iterateStorage() throws Exception {
        LOGGER.debug(String.format("Getting iterator for %s", bucket));
        return Subject.doAs(runUser,
                            (PrivilegedExceptionAction<Iterator<StorageMetadata>>) () -> storageAdapter
                                    .iterator(bucket));
    }

    /**
     * Iterate over the Artifact instances from the Storage Inventory database.  The consumer of this Iterator will
     * assume that the StorageLocation is never null in the Artifact.
     * <p/>
     * Tests can override this for convenience.
     *
     * @return Iterator instance of Artifact objects
     */
    Iterator<Artifact> iterateInventory() {
        return getArtifactDAO().iterator(bucket);
    }

    /**
     * This will method exists to allow a lazy load of the Artifact DAO.  No DAO configuration is performed until
     * this method is called.
     *
     * @return An Artifact DAO instance.
     */
    ArtifactDAO getArtifactDAO() {
        if (artifactDAO == null) {
            artifactDAO = new ArtifactDAO();
            artifactDAO.setConfig(getDAOConfig());
        }

        return artifactDAO;
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

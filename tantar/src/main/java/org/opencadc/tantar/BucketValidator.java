
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
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.StringUtil;

import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import javax.naming.NamingException;
import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.Entity;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedEventDAO;
import org.opencadc.inventory.db.ObsoleteStorageLocation;
import org.opencadc.inventory.db.ObsoleteStorageLocationDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.tantar.db.AutoCloseableTransactionManager;
import org.opencadc.tantar.policy.ResolutionPolicy;


/**
 * Main class to issue iterator requests to the Storage Adaptor and verify the contents.
 * Policies for conflicts, meaning situations where there is a discrepancy between what the Storage Adaptor relays
 * what is currently stored and what the Storage Inventory declares, are set at the properties level.
 */
public class BucketValidator implements ValidateEventListener {

    private static final Logger LOGGER = Logger.getLogger(BucketValidator.class);

    private static final String JNDI_ARTIFACT_DATASOURCE_NAME = "jdbc/inventory";
    private static final String SQL_GEN_KEY = SQLGenerator.class.getName();
    private static final String SCHEMA_KEY = String.format("%s.schema", SQLGenerator.class.getPackage().getName());

    private static final String JDBC_CONFIG_KEY_PREFIX = "org.opencadc.inventory.db";
    private static final String JDBC_USERNAME_KEY = String.format("%s.username", JDBC_CONFIG_KEY_PREFIX);
    private static final String JDBC_PASSWORD_KEY = String.format("%s.password", JDBC_CONFIG_KEY_PREFIX);
    private static final String JDBC_URL_KEY = String.format("%s.url", JDBC_CONFIG_KEY_PREFIX);
    private static final String JDBC_DRIVER_CLASSNAME = "org.postgresql.Driver";

    private final String bucket;
    private final StorageAdapter storageAdapter;
    private final Subject runUser;
    private final boolean reportOnlyFlag;

    // The DAOs can be set later to allow lazy loading.
    private ArtifactDAO artifactDAO;
    private ObsoleteStorageLocationDAO obsoleteStorageLocationDAO;


    BucketValidator(final String bucket, final StorageAdapter storageAdapter, final Subject runUser,
                    final boolean reportOnlyFlag) {
        this.bucket = bucket;
        this.storageAdapter = storageAdapter;
        this.runUser = runUser;
        this.reportOnlyFlag = reportOnlyFlag;
    }

    /**
     * Main functionality.  This will obtain the iterators necessary to validate, and delegate to the Policy to take
     * action and/or report.
     *
     * @throws Exception Pass up any errors to the caller, which is most likely the Main.
     */
    void validate(final ResolutionPolicy resolutionPolicy) throws Exception {
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

    private boolean canTakeAction() {
        return !reportOnlyFlag;
    }

    /**
     * Reset the given Artifact by removing its Storage Location.  This will force the file-sync application to assume
     * it's a new insert and force a re-download of the file.
     *
     * @param artifact The base artifact.  This MUST have a Storage Location.
     * @throws Exception Anything IO/Thread related.
     */
    @Override
    public void reset(final Artifact artifact) throws Exception {
        if (canTakeAction()) {
            artifact.storageLocation = null;
            final ArtifactDAO artifactDAO = getArtifactDAO();
            artifactDAO.getTransactionManager().startTransaction();
            artifactDAO.lock(artifact);
            artifactDAO.put(artifact, true);
            artifactDAO.getTransactionManager().commitTransaction();
        }
    }

    /**
     * Remove a file based on the determination of the Policy.
     *
     * @param storageMetadata The StorageMetadata containing a Storage Location to remove.
     * @throws Exception Covering numerous exceptions from the StorageAdapter.
     */
    @Override
    public void delete(final StorageMetadata storageMetadata) throws Exception {
        if (canTakeAction()) {
            final StorageLocation storageLocation = storageMetadata.getStorageLocation();
            final ObsoleteStorageLocationDAO obsoleteStorageLocationDAO = getObsoleteStorageLocationDAO();
            final ObsoleteStorageLocation obsoleteStorageLocation = obsoleteStorageLocationDAO.get(storageLocation);
            if (obsoleteStorageLocation != null) {
                obsoleteStorageLocationDAO.delete(obsoleteStorageLocation.getID());
            }
        }
    }

    /**
     * Delete the given Artifact.
     *
     * @param artifact The Artifact to remove.
     * @throws Exception Anything that went wrong.
     */
    @Override
    public void delete(final Artifact artifact) throws Exception {
        if (canTakeAction()) {
            final ArtifactDAO artifactDAO = getArtifactDAO();
            try (final AutoCloseableTransactionManager transactionManager =
                         new AutoCloseableTransactionManager(artifactDAO.getTransactionManager())) {
                final DeletedEventDAO<DeletedArtifactEvent> deletedEventDAO = getDeleteEventDAO();
                final DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(artifact.getID());
                artifactDAO.delete(artifact.getID());
                deletedEventDAO.put(deletedArtifactEvent);
                transactionManager.commit();
            }
        }
    }

    /**
     * The Artifact from the given StorageMetadata does not exist but it should.
     *
     * @param storageMetadata The StorageMetadata to create an Artifact from.
     */
    @Override
    public void addArtifact(final StorageMetadata storageMetadata) throws Exception {
        if (canTakeAction()) {
            if (storageMetadata.artifactURI != null) {
                final Artifact artifact = new Artifact(storageMetadata.artifactURI,
                                                       storageMetadata.getContentChecksum(),
                                                       storageMetadata.contentLastModified,
                                                       storageMetadata.getContentLength());

                artifact.storageLocation = storageMetadata.getStorageLocation();
                artifact.contentType = storageMetadata.contentType;
                artifact.contentEncoding = storageMetadata.contentEncoding;

                final ArtifactDAO artifactDAO = getArtifactDAO();
                try (final AutoCloseableTransactionManager transactionManager =
                             new AutoCloseableTransactionManager(artifactDAO.getTransactionManager())) {
                    artifactDAO.put(artifact, false);
                    transactionManager.commit();
                }
            } else {
                LOGGER.warn(String.format(
                        "Policy would like to create an Artifact, but no Artifact URI exists in StorageMetadata "
                        + "located at %s.  This StorageLocation will be skipped for now but will be present in future"
                        + "runs and will likely require manual intervention.",
                        storageMetadata.getStorageLocation()));
            }
        }
    }

    /**
     * Replace the given Artifact with one that represents the given StorageMetadata.  This is used when the
     * policy dictates that in the event of a conflict, the Artifact is no longer valid.
     * @param artifact          The Artifact to replace.
     * @param storageMetadata   The StorageMetadata whose metadata to use.
     * @throws Exception        For any unhandled exceptions.
     */
    @Override
    public void replaceArtifact(final Artifact artifact, final StorageMetadata storageMetadata) throws Exception {
        if (canTakeAction()) {
            final ArtifactDAO artifactDAO = getArtifactDAO();
            try (final AutoCloseableTransactionManager transactionManager =
                         new AutoCloseableTransactionManager(artifactDAO.getTransactionManager())) {
                // Add an obsolete marker to signify that the Storage Location known to the Artifact is no longer
                // valid and assumed deleted at the Storage level.  Remove the Artifact after that.
                final ObsoleteStorageLocation obsoleteStorageLocation =
                        new ObsoleteStorageLocation(artifact.storageLocation);
                final ObsoleteStorageLocationDAO obsoleteStorageLocationDAO = getObsoleteStorageLocationDAO();
                obsoleteStorageLocationDAO.put(obsoleteStorageLocation);
                artifactDAO.delete(artifact.getID());

                final DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(artifact.getID());
                getDeleteEventDAO().put(deletedArtifactEvent);

                // Create a replacement Artifact with information from the StorageMetadata, as it's assumed to hold
                // the correct state of this Storage Entity.
                final Artifact replacementArtifact = new Artifact(artifact.getURI(),
                                                                  storageMetadata.getContentChecksum(),
                                                                  new Date(), storageMetadata.getContentLength());
                replacementArtifact.contentEncoding = storageMetadata.contentEncoding;
                replacementArtifact.contentType = storageMetadata.contentType;
                replacementArtifact.storageLocation = storageMetadata.getStorageLocation();
                artifactDAO.put(replacementArtifact);

                transactionManager.commit();
            }
        }
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

    ObsoleteStorageLocationDAO getObsoleteStorageLocationDAO() {
        if (obsoleteStorageLocationDAO == null) {
            obsoleteStorageLocationDAO = new ObsoleteStorageLocationDAO();
            obsoleteStorageLocationDAO.setConfig(getDAOConfig());
        }

        return obsoleteStorageLocationDAO;
    }

    <T extends Entity> DeletedEventDAO<T> getDeleteEventDAO() {
        return new DeletedEventDAO<>(getArtifactDAO());
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

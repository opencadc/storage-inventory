
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
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.log.EventLifeCycle;
import ca.nrc.cadc.log.EventLogInfo;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.profiler.Profiler;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.StringUtil;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.DeletedStorageLocationEventDAO;
import org.opencadc.inventory.db.EntityNotFoundException;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.version.InitDatabase;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.util.BucketSelector;
import org.opencadc.tantar.policy.ResolutionPolicy;

/**
 * Main class to issue iterator requests to the Storage Adaptor and verify the contents.
 * Policies for conflicts, meaning situations where there is a discrepancy between what the Storage Adaptor relays
 * what is currently stored and what the Storage Inventory declares, are set at the properties level.
 */
public class BucketValidator implements ValidateEventListener {

    private static final Logger LOGGER = Logger.getLogger(BucketValidator.class);

    private static final String APPLICATION_CONFIG_KEY_PREFIX = "org.opencadc.tantar";

    private final List<String> bucketPrefixes = new ArrayList<>();
    private final StorageAdapter storageAdapter;
    private final Subject runUser;
    private final boolean reportOnlyFlag;
    private final ResolutionPolicy resolutionPolicy;

    // Cached ArtifactDAO used for transactional access.
    private final ArtifactDAO artifactDAO;
    private final ArtifactDAO iteratorDAO;


    /**
     * Constructor to allow configuration via some provided properties.  Expect an IllegalStateException for any
     * mis-configured or missing values.
     *
     * @param properties The Properties object.
     * @param reporter   The Reporter to use in the policy.  Can be used elsewhere.
     * @param runUser    The user to run as.
     */
    BucketValidator(final MultiValuedProperties properties, final Reporter reporter, final Subject runUser) {
        this.runUser = runUser;

        final String storageAdapterClassName =
                properties.getFirstPropertyValue(StorageAdapter.class.getCanonicalName());
        if (StringUtil.hasLength(storageAdapterClassName)) {
            this.storageAdapter = InventoryUtil.loadPlugin(storageAdapterClassName);
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Storage Adapter is mandatory.  Please set the %s property to a fully qualified class "
                            + "name.",
                            StorageAdapter.class.getCanonicalName()));
        }

        final String bucketRange =
                properties.getFirstPropertyValue(String.format("%s.buckets", APPLICATION_CONFIG_KEY_PREFIX));
        if (!StringUtil.hasLength(bucketRange)) {
            throw new IllegalStateException(String.format("Bucket(s) is/are mandatory.  Please set the %s property.",
                                                          String.format("%s.buckets",
                                                                        APPLICATION_CONFIG_KEY_PREFIX)));
        } else {
            // Ugly hack to support single Bucket names.
            if (this.storageAdapter.getClass().getName().endsWith("AdStorageAdapter")) {
                this.bucketPrefixes.add(bucketRange.trim());
            } else {
                final BucketSelector bucketSelector = new BucketSelector(bucketRange.trim());
                for (final Iterator<String> bucketIterator = bucketSelector.getBucketIterator();
                     bucketIterator.hasNext();) {
                    this.bucketPrefixes.add(bucketIterator.next().trim());
                }
            }
        }

        final String configuredReportOnly =
                properties.getFirstPropertyValue(String.format("%s.reportOnly", APPLICATION_CONFIG_KEY_PREFIX));
        this.reportOnlyFlag = StringUtil.hasText(configuredReportOnly) && Boolean.parseBoolean(configuredReportOnly);

        final String policyClassName = properties.getFirstPropertyValue(ResolutionPolicy.class.getCanonicalName());
        if (StringUtil.hasLength(policyClassName)) {
            this.resolutionPolicy = InventoryUtil.loadPlugin(policyClassName, this, reporter);
        } else {
            throw new IllegalStateException(
                    String.format("Policy is mandatory.  Please set the %s property to a fully qualified class name.",
                                  ResolutionPolicy.class.getCanonicalName()));
        }

        // DAO configuration
        final Map<String, Object> config = new HashMap<>();
        final String sqlGeneratorKey = SQLGenerator.class.getName();
        final String sqlGeneratorClass = properties.getFirstPropertyValue(sqlGeneratorKey);

        if (StringUtil.hasLength(sqlGeneratorClass)) {
            try {
                config.put(sqlGeneratorKey, Class.forName(sqlGeneratorClass));
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("could not load SQLGenerator class: " + e.getMessage(), e);
            }
        } else {
            throw new IllegalStateException(String.format("A value for %s is required in tantar.properties",
                                                          sqlGeneratorKey));
        }

        final String jdbcConfigKeyPrefix = "org.opencadc.tantar.db";
        final String jdbcSchemaKey = String.format("%s.schema", jdbcConfigKeyPrefix);
        final String jdbcUsernameKey = String.format("%s.username", jdbcConfigKeyPrefix);
        final String jdbcPasswordKey = String.format("%s.password", jdbcConfigKeyPrefix);
        final String jdbcURLKey = String.format("%s.url", jdbcConfigKeyPrefix);
        final String jdbcDriverClassname = "org.postgresql.Driver";

        final String schemaName = properties.getFirstPropertyValue(jdbcSchemaKey);

        final ConnectionConfig cc = new ConnectionConfig(null, null,
                                                         properties.getFirstPropertyValue(jdbcUsernameKey),
                                                         properties.getFirstPropertyValue(jdbcPasswordKey),
                                                         jdbcDriverClassname,
                                                         properties.getFirstPropertyValue(jdbcURLKey));
        try {
            // Register two datasources.
            DBUtil.createJNDIDataSource("jdbc/inventory", cc);
            DBUtil.createJNDIDataSource("jdbc/txinventory", cc);
        } catch (NamingException ne) {
            throw new IllegalStateException("Unable to access Inventory Database.", ne);
        }

        if (StringUtil.hasLength(schemaName)) {
            config.put("schema", schemaName);
        } else {
            throw new IllegalStateException(
                    String.format("A value for %s is required in tantar.properties", jdbcSchemaKey));
        }

        // not currently used in the SQL; correct database must be in the JDBC URL
        //config.put("database", "inventory");

        config.put("jndiDataSourceName", "jdbc/txinventory");
        this.artifactDAO = new ArtifactDAO();
        this.artifactDAO.setConfig(config);

        config.put("jndiDataSourceName", "jdbc/inventory");
        this.iteratorDAO = new ArtifactDAO();
        this.iteratorDAO.setConfig(config);
        
        try {
            String database = (String) config.get("database");
            String schema = (String) config.get("schema");
            DataSource ds = ca.nrc.cadc.db.DBUtil.findJNDIDataSource("jdbc/inventory");
            InitDatabase init = new InitDatabase(ds, database, schema);
            init.doInit();
            LOGGER.info("initDatabase: " + schema + " OK");
        } catch (Exception ex) {
            throw new IllegalStateException("check/init database failed", ex);
        }
    }

    /**
     * Complete constructor.  Useful for unit testing.
     *
     * @param bucketPrefixes   The bucket key(s) to query.
     * @param storageAdapter   The StorageAdapter instance to interact with a Site.
     * @param runUser          The Subject to run as when iterating over the Site's StorageMetadata.
     * @param reportOnlyFlag   Whether to take action or not.  Default is false.
     * @param resolutionPolicy The policy that dictates handling conflicts.
     * @param artifactDAO      The Transactional artifact DAO for CRUD operations.
     * @param iteratorDAO      The bare artifact DAO for iterating.
     */
    BucketValidator(final List<String> bucketPrefixes, final StorageAdapter storageAdapter, final Subject runUser,
                    final boolean reportOnlyFlag, final ResolutionPolicy resolutionPolicy,
                    final ArtifactDAO artifactDAO, final ArtifactDAO iteratorDAO) {
        this.bucketPrefixes.addAll(bucketPrefixes);
        this.storageAdapter = storageAdapter;
        this.runUser = runUser;
        this.reportOnlyFlag = reportOnlyFlag;
        this.resolutionPolicy = resolutionPolicy;
        this.artifactDAO = artifactDAO;
        this.iteratorDAO = iteratorDAO;
    }

    /**
     * Main functionality.  This will obtain the iterators necessary to validate, and delegate to the Policy to take
     * action and/or report.
     *
     * @throws Exception Pass up any errors to the caller, which is most likely the Main.
     */
    void validate() throws Exception {
        final Profiler profiler = new Profiler(BucketValidator.class);
        LOGGER.debug("Acquiring iterators.");
        final Iterator<StorageMetadata> storageMetadataIterator = iterateStorage();
        final Iterator<Artifact> inventoryIterator = iterateInventory();
        profiler.checkpoint("iterators: ok");
        LOGGER.debug(String.format("Acquired iterators: \nHas Artifacts (%b)\nHas Storage Metadata (%b).",
                                   inventoryIterator.hasNext(), storageMetadataIterator.hasNext()));

        LOGGER.debug("START validating iterators.");

        Artifact unresolvedArtifact = null;
        StorageMetadata unresolvedStorageMetadata = null;

        while ((inventoryIterator.hasNext() || unresolvedArtifact != null)
               && (storageMetadataIterator.hasNext() || unresolvedStorageMetadata != null)) {
            final Artifact artifact = (unresolvedArtifact == null) ? inventoryIterator.next() : unresolvedArtifact;
            final StorageMetadata storageMetadata =
                    (unresolvedStorageMetadata == null) ? storageMetadataIterator.next() : unresolvedStorageMetadata;

            final int comparison = artifact.storageLocation.compareTo(storageMetadata.getStorageLocation());

            LOGGER.debug(String.format("Comparing Inventory Storage Location %s with Storage Adapter Location %s (%d)",
                                       artifact.storageLocation, storageMetadata.getStorageLocation(), comparison));

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
            final Artifact artifact = inventoryIterator.next();
            LOGGER.debug(String.format("Artifact %s exists with no Storage Metadata.", artifact.storageLocation));
            resolutionPolicy.resolve(artifact, null);
        }

        while (storageMetadataIterator.hasNext()) {
            final StorageMetadata storageMetadata = storageMetadataIterator.next();
            LOGGER.debug(String.format("Storage Metadata %s exists with no Artifact.",
                                       storageMetadata.getStorageLocation()));
            resolutionPolicy.resolve(null, storageMetadata);
        }

        LOGGER.debug("END validating iterators.");
    }

    private boolean canTakeAction() {
        return !reportOnlyFlag;
    }

    /**
     * Mark the given Artifact as new by removing its Storage Location.  This will force the file-sync application to
     * assume it's a new insert and force a re-download of the file.
     *
     * @param artifact The artifact to clear
     */
    @Override
    public void clearStorageLocation(final Artifact artifact) {
        if (canTakeAction()) {
            final TransactionManager transactionManager = artifactDAO.getTransactionManager();
            String label = "Artifact";
            EventLogInfo clearEventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, label, "CLEAR STORAGE LOCATION");

            try {
                LOGGER.debug("Start transaction.");
                transactionManager.startTransaction();
                
                artifactDAO.lock(artifact);
                Artifact curArtifact = artifactDAO.get(artifact.getID());
                clearEventLogInfo.setArtifactURI(artifact.getURI());
                clearEventLogInfo.setEntityID(curArtifact.getID());
                clearEventLogInfo.setLifeCycle(EventLifeCycle.PROPAGATE);
                final DeletedStorageLocationEventDAO deletedEventDAO = new DeletedStorageLocationEventDAO(artifactDAO);
                long startTime = System.currentTimeMillis();
                deletedEventDAO.put(new DeletedStorageLocationEvent(artifact.getID()));
                artifactDAO.setStorageLocation(curArtifact, null);
                clearEventLogInfo.setElapsedTime(System.currentTimeMillis() - startTime);
                
                transactionManager.commitTransaction();
            } catch (EntityNotFoundException ex) {
                // failed to lock: artifact deleted since start of iteration
                transactionManager.rollbackTransaction();
                LOGGER.debug("Rollback Transaction: OK");
            } catch (Exception e) {
                clearEventLogInfo.setSuccess(false);
                clearEventLogInfo.singleEvent();
                LOGGER.error(String.format("Failed to mark Artifact as new %s.", artifact.getURI()), e);
                transactionManager.rollbackTransaction();
                LOGGER.debug("Rollback Transaction: OK");
                throw e;
            } finally {
                if (transactionManager.isOpen()) {
                    clearEventLogInfo.setSuccess(false);
                    clearEventLogInfo.singleEvent();
                    LOGGER.error("BUG - Open transaction in finally");
                    transactionManager.rollbackTransaction();
                    LOGGER.error("Transaction rolled back successfully.");
                } else {
                    clearEventLogInfo.setSuccess(true);
                    clearEventLogInfo.singleEvent();
                }
            }
        }
    }

    /**
     * Remove a file based on the determination of the Policy.  The use case driving this is when the Policy dictates
     * that the Inventory is correct, but the Artifact is missing, and the Storage Metadata is present.
     * @param storageMetadata The StorageMetadata containing a Storage Location to remove.
     */
    @Override
    public void delete(final StorageMetadata storageMetadata) throws Exception {
        if (canTakeAction()) {
            final StorageLocation storageLocation = storageMetadata.getStorageLocation();
            LOGGER.debug("Deleting from storage...");
            storageAdapter.delete(storageLocation);
            LOGGER.debug("Delete from storage: OK");
        }
    }

    /**
     * Delete the given Artifact.  This method is called in the event that the policy deems the Artifact should not
     * exist given that there is no matching StorageMetadata.
     *
     * @param artifact The Artifact to remove.
     */
    @Override
    public void delete(final Artifact artifact) {
        if (canTakeAction()) {
            final TransactionManager transactionManager = artifactDAO.getTransactionManager();
            String label = "Artifact";
            EventLogInfo putEventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, label, "DELETE");
            putEventLogInfo.setLifeCycle(EventLifeCycle.CREATE);
            putEventLogInfo.setEntityID(artifact.getID());
            putEventLogInfo.setArtifactURI(artifact.getURI());
            try {
                LOGGER.debug("start transaction...");
                transactionManager.startTransaction();
                LOGGER.debug("start transaction... OK");

                artifactDAO.lock(artifact);
                
                final DeletedArtifactEventDAO deletedEventDAO = new DeletedArtifactEventDAO(artifactDAO);
                deletedEventDAO.put(new DeletedArtifactEvent(artifact.getID()));
                
                long startTime = System.currentTimeMillis();
                artifactDAO.delete(artifact.getID());
                putEventLogInfo.setElapsedTime(System.currentTimeMillis() - startTime);

                LOGGER.debug("commit transaction...");
                transactionManager.commitTransaction();
                LOGGER.debug("commit transaction... OK");
            } catch (EntityNotFoundException ex) {
                LOGGER.debug("failed to lock Artifact " + artifact.getID() + " : already deleted");
            } catch (Exception e) {
                putEventLogInfo.setSuccess(false);
                putEventLogInfo.singleEvent();
                LOGGER.error(String.format("Failed to delete Artifact %s.", artifact.getURI()), e);
                transactionManager.rollbackTransaction();
                LOGGER.debug("Rollback Transaction: OK");
                throw e;
            } finally {
                if (transactionManager.isOpen()) {
                    putEventLogInfo.setSuccess(false);
                    putEventLogInfo.singleEvent();
                    LOGGER.error("BUG - Open transaction in finally");
                    transactionManager.rollbackTransaction();
                    LOGGER.error("Transaction rolled back successfully.");
                } else {
                    putEventLogInfo.setSuccess(true);
                    putEventLogInfo.singleEvent();
                }
            }
        }
    }

    // used by createArtifact and replaceArtifact
    private Artifact toArtifact(StorageMetadata storageMetadata) {
        if (storageMetadata.artifactURI != null) {
            final Date contentLastModified =
                storageMetadata.contentLastModified == null ? new Date() : storageMetadata.contentLastModified;

            final Artifact artifact = new Artifact(storageMetadata.artifactURI,
                                                   storageMetadata.getContentChecksum(),
                                                   contentLastModified,
                                                   storageMetadata.getContentLength());
            artifact.storageLocation = storageMetadata.getStorageLocation();
            artifact.contentType = storageMetadata.contentType;
            artifact.contentEncoding = storageMetadata.contentEncoding;
            return artifact;
        }
        LOGGER.warn(String.format(
            "Policy would like to create an Artifact, but no Artifact URI exists in StorageMetadata "
            + "located at %s.  This StorageLocation will be skipped for now but will be present in future"
            + "runs and will likely require manual intervention.",
            storageMetadata.getStorageLocation()));
        return null;
    }
    
    /**
     * The Artifact from the given StorageMetadata does not exist but it should.
     *
     * @param storageMetadata The StorageMetadata to create an Artifact from.
     */
    @Override
    public void createArtifact(final StorageMetadata storageMetadata) {
        if (canTakeAction()) {
            Artifact artifact = toArtifact(storageMetadata);
            if (artifact == null) {
                return; // nothing to do
            }
                
            final TransactionManager transactionManager = artifactDAO.getTransactionManager();
            String label = "Artifact";
            EventLogInfo putEventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, label, "CREATE PUT");
            putEventLogInfo.setLifeCycle(EventLifeCycle.CREATE);
            putEventLogInfo.setEntityID(artifact.getID());
            putEventLogInfo.setArtifactURI(artifact.getURI());

            try {
                LOGGER.debug("Start transaction.");
                transactionManager.startTransaction();
                long startTime = System.currentTimeMillis();
                artifactDAO.put(artifact);
                putEventLogInfo.setElapsedTime(System.currentTimeMillis() - startTime);
                transactionManager.commitTransaction();
            } catch (Exception e) {
                putEventLogInfo.setSuccess(false);
                putEventLogInfo.singleEvent();
                LOGGER.error(String.format("Failed to create Artifact %s.", artifact.getURI()), e);
                transactionManager.rollbackTransaction();
                LOGGER.debug("Rollback Transaction: OK");
                throw e;
            } finally {
                if (transactionManager.isOpen()) {
                    putEventLogInfo.setSuccess(false);
                    putEventLogInfo.singleEvent();
                    LOGGER.error("BUG - Open transaction in finally");
                    transactionManager.rollbackTransaction();
                    LOGGER.error("Transaction rolled back successfully.");
                } else {
                    putEventLogInfo.setSuccess(true);
                    putEventLogInfo.singleEvent();
                }
            }
        }
    }

    /**
     * Replace the given Artifact with one that represents the given StorageMetadata.  This is used when the
     * policy dictates that in the event of a conflict, the Artifact is no longer valid (StorageMetadata takes
     * precedence).  This method will be called if both the Artifact and StorageMetadata have the same Storage Location.
     *
     * @param artifact        The Artifact to replace.
     * @param storageMetadata The StorageMetadata whose metadata to use.
     */
    @Override
    public void replaceArtifact(final Artifact artifact, final StorageMetadata storageMetadata) {
        if (canTakeAction()) {
            final TransactionManager transactionManager = artifactDAO.getTransactionManager();
            boolean putStarted = false;
            String label = "Artifact";
            EventLogInfo deleteEventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, label, "REPLACE DELETE");
            EventLogInfo putEventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, label, "REPLACE PUT");

            try {
                LOGGER.debug("Start transaction.");
                transactionManager.startTransaction();
                
                try {
                    artifactDAO.lock(artifact);
                    
                    DeletedArtifactEventDAO deletedEventDAO = new DeletedArtifactEventDAO(artifactDAO);
                    long startTime = System.currentTimeMillis();
                    deletedEventDAO.put(new DeletedArtifactEvent(artifact.getID()));
                    
                    artifactDAO.delete(artifact.getID());
                    deleteEventLogInfo.setElapsedTime(System.currentTimeMillis() - startTime);
                    deleteEventLogInfo.setLifeCycle(EventLifeCycle.PROPAGATE);
                    deleteEventLogInfo.setEntityID(artifact.getID());
                    deleteEventLogInfo.setArtifactURI(artifact.getURI());
                    deleteEventLogInfo.setSuccess(true);
                    deleteEventLogInfo.singleEvent();
                } catch (EntityNotFoundException ex) {
                    deleteEventLogInfo.setSuccess(false);
                    deleteEventLogInfo.singleEvent();
                    // artifact deleted since start of iteration - continue 
                    LOGGER.debug("artifact to be replaced was already deleted... continuing to create replacement");
                }

                final Artifact replacementArtifact = toArtifact(storageMetadata);
                
                putStarted = true;
                putEventLogInfo.setArtifactURI(replacementArtifact.getURI());
                putEventLogInfo.setEntityID(replacementArtifact.getID());
                putEventLogInfo.setLifeCycle(EventLifeCycle.PROPAGATE);
                long startTime = System.currentTimeMillis();
                artifactDAO.put(replacementArtifact);
                putEventLogInfo.setElapsedTime(System.currentTimeMillis() - startTime);

                transactionManager.commitTransaction();
            } catch (Exception e) {
                if (putStarted) {
                    putEventLogInfo.setSuccess(false);
                    putEventLogInfo.singleEvent();
                }

                LOGGER.error(String.format("Failed to create Artifact %s.", storageMetadata.artifactURI), e);
                transactionManager.rollbackTransaction();
                LOGGER.debug("Rollback Transaction: OK");
                throw e;
            } finally {
                if (transactionManager.isOpen()) {
                    if (putStarted) {
                        putEventLogInfo.setSuccess(false);
                        putEventLogInfo.singleEvent();
                    }
                    LOGGER.error("BUG - Open transaction in finally");
                    transactionManager.rollbackTransaction();
                    LOGGER.error("Transaction rolled back successfully.");
                } else {
                    putEventLogInfo.setSuccess(true);
                    putEventLogInfo.singleEvent();
                }
            }
        }
    }

    /**
     * Update the values of the given Artifact with those from the given StorageMetadata.  This differs from a replace
     * as it will not delete the original Artifact first, but rather update the values and issue a PUT.
     *
     * @param artifact        The Artifact to update.
     * @param storageMetadata The StorageMetadata from which to update the Artifact's fields.
     * @throws Exception Any unexpected error.
     */
    @Override
    public void updateArtifact(final Artifact artifact, final StorageMetadata storageMetadata) throws Exception {
        if (canTakeAction()) {
            final TransactionManager transactionManager = artifactDAO.getTransactionManager();
            String label = "Artifact";
            EventLogInfo updateEventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, label, "UPDATE");
            updateEventLogInfo.setArtifactURI(artifact.getURI());
            updateEventLogInfo.setEntityID(artifact.getID());
            updateEventLogInfo.setLifeCycle(EventLifeCycle.PROPAGATE);

            try {
                LOGGER.debug("Start transaction.");
                transactionManager.startTransaction();
                
                artifactDAO.lock(artifact);

                // By reusing the Artifact instance's ID we can have the original Artifact but reset the mutable
                // fields below.
                artifact.contentEncoding = storageMetadata.contentEncoding;
                artifact.contentType = storageMetadata.contentType;
                artifact.storageLocation = storageMetadata.getStorageLocation();

                long startTime = System.currentTimeMillis();
                artifactDAO.put(artifact);
                updateEventLogInfo.setElapsedTime(System.currentTimeMillis() - startTime);

                transactionManager.commitTransaction();
            } catch (EntityNotFoundException ex) {
                LOGGER.debug("failed to lock Artifact " + artifact.getID() + " : already deleted");
            } catch (Exception e) {
                updateEventLogInfo.setSuccess(false);
                updateEventLogInfo.singleEvent();
                LOGGER.error(String.format("Failed to update Artifact %s.", storageMetadata.artifactURI), e);
                transactionManager.rollbackTransaction();
                LOGGER.debug("Rollback Transaction: OK");
                throw e;
            } finally {
                if (transactionManager.isOpen()) {
                    updateEventLogInfo.setSuccess(false);
                    updateEventLogInfo.singleEvent();
                    LOGGER.error("BUG - Open transaction in finally");
                    transactionManager.rollbackTransaction();
                    LOGGER.error("Transaction rolled back successfully.");
                } else {
                    updateEventLogInfo.setSuccess(true);
                    updateEventLogInfo.singleEvent();
                }
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
        LOGGER.debug(String.format("Getting iterator for %s running as %s", this.bucketPrefixes, this.runUser.getPrincipals()));
        return new Iterator<StorageMetadata>() {
            final Iterator<String> bucketPrefixIterator = bucketPrefixes.iterator();

            // The bucket range should have at least one value, so calling next() should be safe here.
            Iterator<StorageMetadata> storageMetadataIterator =
                    Subject.doAs(runUser, (PrivilegedExceptionAction<Iterator<StorageMetadata>>) () ->
                                                      storageAdapter.iterator(bucketPrefixIterator.next()));

            @Override
            public boolean hasNext() {
                if (storageMetadataIterator.hasNext()) {
                    return true;
                } else if (bucketPrefixIterator.hasNext()) {
                    try {
                        storageMetadataIterator =
                                Subject.doAs(runUser, (PrivilegedExceptionAction<Iterator<StorageMetadata>>) () ->
                                                                  storageAdapter.iterator(bucketPrefixIterator.next()));
                    } catch (PrivilegedActionException exception) {
                        throw new IllegalStateException(exception);
                    }
                    return hasNext();
                } else {
                    return false;
                }
            }

            @Override
            public StorageMetadata next() {
                return storageMetadataIterator.next();
            }
        };
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
        return new Iterator<Artifact>() {
            final Iterator<String> bucketPrefixIterator = bucketPrefixes.iterator();

            // The bucket range should have at least one value, so calling next() should be safe here.
            ResourceIterator<Artifact> artifactIterator = iteratorDAO.storedIterator(bucketPrefixIterator.next());

            @Override
            public boolean hasNext() {
                if (artifactIterator.hasNext()) {
                    return true;
                } else if (bucketPrefixIterator.hasNext()) {
                    artifactIterator = iteratorDAO.storedIterator(bucketPrefixIterator.next());
                    return hasNext();
                } else {
                    return false;
                }
            }

            @Override
            public Artifact next() {
                return artifactIterator.next();
            }
        };
    }
}

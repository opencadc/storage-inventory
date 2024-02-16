
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

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.profiler.Profiler;
import ca.nrc.cadc.util.BucketSelector;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.ObsoleteStorageLocation;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.StorageLocationEvent;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.DeletedStorageLocationEventDAO;
import org.opencadc.inventory.db.ObsoleteStorageLocationDAO;
import org.opencadc.inventory.db.StorageLocationEventDAO;
import org.opencadc.inventory.db.version.InitDatabaseSI;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.tantar.policy.ResolutionPolicy;

/**
 * Main class to issue iterator requests to the Storage Adaptor and verify the contents.
 * Policies for conflicts, meaning situations where there is a discrepancy between what the Storage Adaptor relays
 * what is currently stored and what the Storage Inventory declares, are set at the properties level.
 */
public class BucketValidator implements ValidateActions {

    private static final Logger log = Logger.getLogger(BucketValidator.class);

    private final List<String> bucketPrefixes = new ArrayList<>();
    private final StorageAdapter storageAdapter;
    private final ResolutionPolicy validationPolicy;
    private final boolean reportOnlyFlag;
    
    private boolean includeRecoverable = false;

    // Cached ArtifactDAO used for transactional access.
    private final ArtifactDAO artifactDAO;
    private final ArtifactDAO iteratorDAO;
    private final ObsoleteStorageLocationDAO obsoleteStorageLocationDAO;
    
    // occasional summary logging
    private final long summaryLogInterval = 5 * 60L; // 5 minutes
    private long lastSummary = 0L;
    private long numValidated = 0L;
    private long numValid = 0L;
    private long numDelay = 0L;
    private long numClearStorageLocation = 0L;
    private long numDeleteStorageLocation = 0L;
    private long numDeleteObsoleteStorageLocation = 0L;
    private long numCreateArtifact = 0L;
    private long numDeleteArtifact = 0L;
    private long numReplaceArtifact = 0L;
    private long numUpdateArtifact = 0L;

    /**
     * Constructor.
     * 
     * @param daoConfig DAO config map
     * @param connectionConfig database connection info
     * @param adapter StorageAdapter
     * @param validationPolicy the discrepancy resolution policy
     * @param bucketRange raw bucket range
     * @param reportOnly true for dry-run, false to take actions
     */
    public BucketValidator(Map<String, Object> daoConfig, ConnectionConfig connectionConfig, StorageAdapter adapter, 
            ResolutionPolicy validationPolicy, String bucketRange, boolean reportOnly) {
        this.reportOnlyFlag = reportOnly;
        this.storageAdapter = adapter;
        this.validationPolicy = validationPolicy;
        
        switch (storageAdapter.getBucketType()) {
            case NONE:
                break;
            case HEX:
                if (bucketRange == null) {
                    throw new IllegalArgumentException("invalid bucket range: null");
                }
                final BucketSelector bucketSelector = new BucketSelector(bucketRange.trim());
                for (final Iterator<String> bucketIterator = bucketSelector.getBucketIterator();
                     bucketIterator.hasNext();) {
                    this.bucketPrefixes.add(bucketIterator.next().trim());
                }
                break;
            case PLAIN:
                if (bucketRange == null) {
                    throw new IllegalArgumentException("invalid bucket range: null");
                }
                this.bucketPrefixes.add(bucketRange.trim());
                break;
            default:
                throw new RuntimeException("BUG: unexpected BucketType " + storageAdapter.getBucketType());
        }
        
        try {
            // Register two datasources.
            DBUtil.createJNDIDataSource("jdbc/inventory", connectionConfig);
            DBUtil.createJNDIDataSource("jdbc/txinventory", connectionConfig);
        } catch (NamingException ne) {
            throw new IllegalStateException("Unable to access Inventory Database.", ne);
        }
        daoConfig.put("jndiDataSourceName", "jdbc/txinventory");
        this.artifactDAO = new ArtifactDAO();
        this.artifactDAO.setConfig(daoConfig);

        daoConfig.put("jndiDataSourceName", "jdbc/inventory");
        this.iteratorDAO = new ArtifactDAO();
        this.iteratorDAO.setConfig(daoConfig);
        this.obsoleteStorageLocationDAO = new ObsoleteStorageLocationDAO(this.artifactDAO);
        
        try {
            String database = (String) daoConfig.get("database");
            String schema = (String) daoConfig.get("invSchema");
            DataSource ds = ca.nrc.cadc.db.DBUtil.findJNDIDataSource("jdbc/inventory");
            InitDatabaseSI init = new InitDatabaseSI(ds, database, schema);
            init.doInit();
            log.info("initDatabase: " + schema + " OK");
        } catch (Exception ex) {
            throw new IllegalStateException("check/init database failed", ex);
        }
    }
    
    // minimal unit test use
    BucketValidator(ResolutionPolicy validationPolicy) {
        this.reportOnlyFlag = false;
        this.storageAdapter = null;
        this.validationPolicy = validationPolicy;
        this.artifactDAO = null;
        this.obsoleteStorageLocationDAO = null;
        this.iteratorDAO = null;
    }

    public void setIncludeRecoverable(boolean enabled) {
        this.includeRecoverable = enabled;
    }
    
    /**
     * Main functionality.  This will obtain the iterators necessary to validate, and delegate to the Policy to take
     * action and/or report.
     *
     * @throws Exception Pass up any errors to the caller, which is most likely the Main.
     */
    public void validate() throws Exception {
        log.info("BucketValidator.validate phase=start reportOnly=" + reportOnlyFlag
            + " includeRecoverable=" + includeRecoverable);
        validationPolicy.setValidateActions(this);
        try {
            doit(validationPolicy);
        } finally {
            logSummary(validationPolicy, true, false);
            log.info("BucketValidator.validate phase=end reportOnly=" + reportOnlyFlag
                + " includeRecoverable=" + includeRecoverable);
        }
    }
    
    private void doit(ResolutionPolicy validationPolicy) throws Exception {
        final Profiler profiler = new Profiler(BucketValidator.class);
        
        long t1 = System.currentTimeMillis();

        final Iterator<StorageMetadata> storageMetadataIterator = getStorageIterator();
        long t2 = System.currentTimeMillis();
        log.info("BucketValidator.storageQuery duration=" + (t2 - t1));

        final Iterator<Artifact> inventoryIterator = getInventoryIterator();
        long t3 = System.currentTimeMillis();
        log.info("BucketValidator.inventoryQuery duration=" + (t3 - t2));

        log.debug(String.format("Acquired iterators: \nHas Artifacts (%b)\nHas Storage Metadata (%b).",
                                   inventoryIterator.hasNext(), storageMetadataIterator.hasNext()));

        Artifact unvalidatedArtifact = null;
        StorageMetadata unvalidatedStorageMetadata = null;
        logSummary(validationPolicy, true, true);
        while ((inventoryIterator.hasNext() || unvalidatedArtifact != null)
               && (storageMetadataIterator.hasNext() || unvalidatedStorageMetadata != null)) {
            final Artifact artifact = (unvalidatedArtifact == null) ? inventoryIterator.next() : unvalidatedArtifact;
            final StorageMetadata storageMetadata =
                    (unvalidatedStorageMetadata == null) ? storageMetadataIterator.next() : unvalidatedStorageMetadata;

            final int comparison = artifact.storageLocation.compareTo(storageMetadata.getStorageLocation());

            log.debug(String.format("compare (I vs S): %s  vs %s (%d)",
                                       artifact.storageLocation, storageMetadata.getStorageLocation(), comparison));

            if (comparison == 0) {
                // Same storage location.  Test the metadata.
                unvalidatedArtifact = null;
                unvalidatedStorageMetadata = null;
                validationPolicy.validate(artifact, storageMetadata);
            } else if (comparison > 0) {
                // Exists in Storage but not in inventory.
                unvalidatedArtifact = artifact;
                unvalidatedStorageMetadata = null;
                validationPolicy.validate(null, storageMetadata);
            } else {
                // Exists in Inventory but not in Storage.
                unvalidatedArtifact = null;
                unvalidatedStorageMetadata = storageMetadata;
                validationPolicy.validate(artifact, null);
            }
            numValidated++;
            logSummary(validationPolicy);
        }

        // deal with one unvalidated object when one iterator is exhausted
        if (unvalidatedArtifact != null) {
            log.debug("unvalidatedArtifact: " + unvalidatedArtifact);
            validationPolicy.validate(unvalidatedArtifact, null);
            numValidated++;
        }
        if (unvalidatedStorageMetadata != null) {
            log.debug("unvalidatedStorageMetadata: " + unvalidatedStorageMetadata);
            validationPolicy.validate(null, unvalidatedStorageMetadata);
            numValidated++;
        }
        
        // when one iterator is exhausted, the other can have multiple remaining
        // items, eg synced artifact database but no/minimal synced files
        // or ingesting/recovering from storage... so we do need to process that
        // iterator to completion, but if there is a undetected failure of
        // one iterator, then the loops below are potentially dangerous
        
        // this loop is dangerous with StorageIsAlwaysRight because it is 
        // deleting all these artifacts
        while (inventoryIterator.hasNext()) {
            final Artifact artifact = inventoryIterator.next();
            validationPolicy.validate(artifact, null);
            numValidated++;
            logSummary(validationPolicy);
        }
        // this loop is dangerous with InventoryisAlwaysRight because it is 
        // deleting all these storageLocations
        while (storageMetadataIterator.hasNext()) {
            final StorageMetadata storageMetadata = storageMetadataIterator.next();
            validationPolicy.validate(null, storageMetadata);
            numValidated++;
            logSummary(validationPolicy);
        }
    }

    // default per-item invocation
    private void logSummary(ResolutionPolicy pol) {
        logSummary(pol, false, true);
    }
    
    // initial: true, true
    // final:   true, false
    private void logSummary(ResolutionPolicy pol, boolean force, boolean showNext) {
        long sec = System.currentTimeMillis() / 1000L;
        long dt = (sec - lastSummary);
        if (!force && dt < summaryLogInterval) {
            return;
        }
        lastSummary = sec;
        
        StringBuilder sb = new StringBuilder();
        sb.append(pol.getClass().getSimpleName()).append(".summary");
        if (numValidated > 0) {
            sb.append(" numValidated=").append(numValidated);
        }
        if (numValid > 0) {
            sb.append(" numValid=").append(numValid);
        }
        if (numDelay > 0) {
            sb.append(" numDelay=").append(numDelay);
        }
        if (numClearStorageLocation > 0) {
            sb.append(" numClearStorageLocation=").append(numClearStorageLocation);
        }
        if (numDeleteStorageLocation > 0) {
            sb.append(" numDeleteStorageLocation=").append(numDeleteStorageLocation);
        }
        if (numDeleteObsoleteStorageLocation > 0) {
            sb.append(" numDeleteObsoleteStorageLocation=").append(numDeleteObsoleteStorageLocation);
        }
        if (numCreateArtifact > 0) {
            sb.append(" numCreateArtifact=").append(numCreateArtifact);
        }
        if (numDeleteArtifact > 0) {
            sb.append(" numDeleteArtifact=").append(numDeleteArtifact);
        }
        if (numReplaceArtifact > 0) {
            sb.append(" numReplaceArtifact=").append(numReplaceArtifact);
        }
        if (numUpdateArtifact > 0) {
            sb.append(" numUpdateArtifact=").append(numUpdateArtifact);
        }
        if (showNext) {
            sb.append(" nextSummaryIn=").append(summaryLogInterval).append("sec");
        }
        log.info(sb.toString());
    }
    
    private boolean canTakeAction() {
        return !reportOnlyFlag;
    }
    
    @Override
    public void delayAction() {
        numDelay++;
    }

    @Override
    public Artifact getArtifact(URI uri) {
        return artifactDAO.get(uri);
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

            try {
                log.debug("Start transaction.");
                transactionManager.startTransaction();
                
                Artifact curArtifact = artifactDAO.lock(artifact);
                if (curArtifact != null) {
                    final DeletedStorageLocationEventDAO deletedEventDAO = new DeletedStorageLocationEventDAO(artifactDAO);
                    deletedEventDAO.put(new DeletedStorageLocationEvent(artifact.getID()));
                    artifactDAO.setStorageLocation(curArtifact, null);
                    
                    transactionManager.commitTransaction();
                    numClearStorageLocation++;
                } else {
                    transactionManager.rollbackTransaction();
                    log.debug("failed to lock artifact, assume deleted: " + artifact.getID());
                }
            } catch (Exception e) {
                log.error(String.format("Failed to mark Artifact as new %s.", artifact.getURI()), e);
                transactionManager.rollbackTransaction();
                log.debug("Rollback Transaction: OK");
                throw e;
            } finally {
                if (transactionManager.isOpen()) {
                    log.error("BUG - Open transaction in finally");
                    transactionManager.rollbackTransaction();
                    log.error("Transaction rolled back successfully.");
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
            try {
                log.debug("delete from storage: " + storageLocation);
                storageAdapter.delete(storageLocation, includeRecoverable);
                numDeleteStorageLocation++;
            } catch (ResourceNotFoundException ex) {
                log.warn("delete from storage failed: " + storageLocation + " reason: " + ex);
            }
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
            try {
                log.debug("start transaction...");
                transactionManager.startTransaction();
                log.debug("start transaction... OK");

                Artifact cur = artifactDAO.lock(artifact);
                if (cur != null) {
                    final DeletedArtifactEventDAO deletedEventDAO = new DeletedArtifactEventDAO(artifactDAO);
                    deletedEventDAO.put(new DeletedArtifactEvent(artifact.getID()));
                    artifactDAO.delete(artifact.getID());
                    
                    transactionManager.commitTransaction();
                    numDeleteArtifact++;
                } else {
                    transactionManager.rollbackTransaction();
                    log.debug("failed to lock artifact, assume deleted: " + artifact.getID());
                }
            } catch (Exception e) {
                log.error(String.format("Failed to delete Artifact %s.", artifact.getURI()), e);
                transactionManager.rollbackTransaction();
                log.debug("Rollback Transaction: OK");
                throw e;
            } finally {
                if (transactionManager.isOpen()) {
                    log.error("BUG - Open transaction in finally");
                    transactionManager.rollbackTransaction();
                    log.error("Transaction rolled back successfully.");
                }
            }
        }
    }

    // used by createArtifact and replaceArtifact
    private Artifact toArtifact(StorageMetadata storageMetadata) {
        final Artifact artifact = new Artifact(storageMetadata.getArtifactURI(),
                                                   storageMetadata.getContentChecksum(),
                                                   storageMetadata.getContentLastModified(),
                                                   storageMetadata.getContentLength());
        artifact.storageLocation = storageMetadata.getStorageLocation();
        artifact.contentType = storageMetadata.contentType;
        artifact.contentEncoding = storageMetadata.contentEncoding;
        return artifact;
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

            try {
                log.debug("Start transaction.");
                transactionManager.startTransaction();
                artifactDAO.put(artifact);
                transactionManager.commitTransaction();
                numCreateArtifact++;
            } catch (Exception e) {
                log.error(String.format("Failed to create Artifact %s.", artifact.getURI()), e);
                transactionManager.rollbackTransaction();
                log.debug("Rollback Transaction: OK");
                throw e;
            } finally {
                if (transactionManager.isOpen()) {
                    log.error("BUG - Open transaction in finally");
                    transactionManager.rollbackTransaction();
                    log.error("Transaction rolled back successfully.");
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

            try {
                log.debug("Start transaction.");
                transactionManager.startTransaction();
                
                Artifact cur = artifactDAO.lock(artifact);
                if (cur != null) {
                    DeletedArtifactEventDAO deletedEventDAO = new DeletedArtifactEventDAO(artifactDAO);
                    deletedEventDAO.put(new DeletedArtifactEvent(artifact.getID()));
                    artifactDAO.delete(artifact.getID());
                }

                final Artifact replacementArtifact = toArtifact(storageMetadata);
                artifactDAO.put(replacementArtifact);

                transactionManager.commitTransaction();
                numReplaceArtifact++;
            } catch (Exception e) {
                log.error(String.format("Failed to create Artifact %s.", storageMetadata.getArtifactURI()), e);
                transactionManager.rollbackTransaction();
                log.debug("Rollback Transaction: OK");
                throw e;
            } finally {
                if (transactionManager.isOpen()) {
                    log.error("BUG - Open transaction in finally");
                    transactionManager.rollbackTransaction();
                    log.error("Transaction rolled back successfully.");
                }
            }
        }
    }

    /**
     * Update the StorageLocation of the given Artifact
     *
     * @param artifact        The Artifact to update
     * @param smeta         StorageMetadata from which to get the StorageLocation to assign
     * @throws Exception    Any unexpected error.
     */
    public void updateArtifact(final Artifact artifact, final StorageMetadata smeta) throws Exception {
        if (canTakeAction()) {
            final TransactionManager transactionManager = artifactDAO.getTransactionManager();

            try {
                log.debug("Start transaction.");
                transactionManager.startTransaction();
                
                Artifact cur = artifactDAO.lock(artifact);
                if (cur != null) {
                    cur.storageLocation = smeta.getStorageLocation();
                    artifactDAO.setStorageLocation(cur, smeta.getStorageLocation());
                    
                    StorageLocationEventDAO sleDAO = new StorageLocationEventDAO(artifactDAO);
                    StorageLocationEvent sle = new StorageLocationEvent(cur.getID());
                    sleDAO.put(sle);
                    
                    if (smeta.deleteRecoverable) {
                        // TODO: there is no intTest that verifies that this was called correctly
                        storageAdapter.recover(smeta.getStorageLocation(), artifact.getContentLastModified());
                    }
                    
                    transactionManager.commitTransaction();
                    numUpdateArtifact++;
                } else {
                    transactionManager.rollbackTransaction();
                    log.debug("failed to lock artifact, assume deleted: " + artifact.getID());
                }
            } catch (Exception e) {
                log.error(String.format("Failed to update Artifact %s.", artifact.getURI()), e);
                transactionManager.rollbackTransaction();
                log.debug("Rollback Transaction: OK");
                throw e;
            } finally {
                if (transactionManager.isOpen()) {
                    log.error("BUG - Open transaction in finally");
                    transactionManager.rollbackTransaction();
                    log.error("Transaction rolled back successfully.");
                }
            }
        }
    }

    /**
     * Iterate over the Artifact instances from the Storage Inventory database.  The consumer of this Iterator will
     * assume that the StorageLocation is never null in the Artifact.
     * <p/>
     * Tests can override this for convenience.
     *
     * @return Iterator instance of Artifact objects
     */
    Iterator<Artifact> getInventoryIterator() {
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

    /**
     * Check the StorageMetadata for a matching ObsoleteStorageLocation and if found
     * delete the file from storage and delete the ObsoleteStorageLocation.
     */
    boolean isObsoleteStorageLocation(StorageMetadata storageMetadata) {
        
        // TODO: since there are typically very few ObsoleteStorageLocation sitting around: 
        //    query for the whole list and keep in memory??
        // otherwise, this query occurs once per stored object (many small fast queries, but still...)
        
        ObsoleteStorageLocation obsoleteStorageLocation;
        try {
            obsoleteStorageLocation = this.obsoleteStorageLocationDAO.get(storageMetadata.getStorageLocation());
        } catch (Exception e) {
            throw new IllegalStateException(
                String.format("query failed for ObsoleteStorageLocation %s for file %s",
                              storageMetadata.getStorageLocation().getStorageID().toASCIIString(),
                              storageMetadata.getArtifactURI().toASCIIString()), e);
        }
        if (obsoleteStorageLocation != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(this.getClass().getSimpleName());
            sb.append(".deleteStorageLocation");
            sb.append(" Artifact.uri=").append(storageMetadata.getArtifactURI());
            sb.append(" loc=").append(storageMetadata.getStorageLocation());
            sb.append(" reason=obsolete");
            log.info(sb.toString());
            
            if (canTakeAction()) {
                try {
                    delete(storageMetadata);
                    this.obsoleteStorageLocationDAO.delete(obsoleteStorageLocation.getID());
                    numDeleteObsoleteStorageLocation++;
                } catch (Exception e) {
                    throw new IllegalStateException("failed to cleanup obsolete " + obsoleteStorageLocation, e);
                }
            }
        }
        return obsoleteStorageLocation != null;
    }

    /**
     * Overrideable method to return a StorageMetadataIterator instance. Useful in unit tests.
     *
     * @return StorageMetadataIterator instance.
     */
    Iterator<StorageMetadata> getStorageIterator() throws StorageEngageException {
        return new StorageMetadataIterator();
    }

    /**
     * Class to iterate over the StorageMetadata instances from the Storage Adapter.
     * The consumer of this Iterator will assume that the StorageLocation is never null.
     */
    private class StorageMetadataIterator implements Iterator<StorageMetadata> {

        final Iterator<String> bucketPrefixIterator;
        Iterator<StorageMetadata> storageMetadataIterator;
        StorageMetadata storageMetadata;

        /**
         * Constructor
         */
        StorageMetadataIterator() throws StorageEngageException {
            this.bucketPrefixIterator = bucketPrefixes.iterator();

            // The bucket range should have at least one value, so calling next() should be safe here.
            this.storageMetadataIterator = storageAdapter.iterator(bucketPrefixIterator.next(), includeRecoverable);
            advance();
        }

        private void advance() {
            try {
                if (this.storageMetadataIterator.hasNext()) {
                    this.storageMetadata = this.storageMetadataIterator.next();
                    if (isObsoleteStorageLocation(this.storageMetadata)) {
                        advance();
                    }
                } else if (this.bucketPrefixIterator.hasNext()) {
                    this.storageMetadataIterator = storageAdapter.iterator(this.bucketPrefixIterator.next(), includeRecoverable);
                    advance();
                } else {
                    this.storageMetadata = null;
                }
            } catch (StorageEngageException ex) {
                throw new RuntimeException("unexpected failure after iteration started", ex);
            }
        }

        @Override
        public boolean hasNext() {
            return storageMetadata != null;
        }

        @Override
        public StorageMetadata next() {
            StorageMetadata ret = this.storageMetadata;
            advance();
            return ret;
        }
    }

}

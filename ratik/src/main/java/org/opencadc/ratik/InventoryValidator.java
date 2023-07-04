/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2023.                            (c) 2023.
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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.ratik;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.BucketSelector;
import ca.nrc.cadc.util.StringUtil;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import javax.naming.NamingException;
import javax.security.auth.Subject;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.inventory.db.version.InitDatabase;
import org.opencadc.inventory.query.ArtifactRowMapper;
import org.opencadc.inventory.util.ArtifactSelector;
import org.opencadc.tap.TapClient;
import org.opencadc.tap.TapRowMapper;

/**
 * Validate local inventory.
 */
public class InventoryValidator implements Runnable {
    private static final Logger log = Logger.getLogger(InventoryValidator.class);

    public static final String CERTIFICATE_FILE_LOCATION = System.getProperty("user.home") + "/.ssl/cadcproxy.pem";

    private final ArtifactDAO artifactDAO;
    private final URI resourceID;
    
    private final boolean trackSiteLocations;
    private final ArtifactSelector artifactSelector;
    private final BucketSelector bucketSelector;
    private final ArtifactValidator artifactValidator;
    private final MessageDigest messageDigest;
    
    private StorageSite remoteSite;
    
    // package access so tests can reduce this
    long raceConditionDelta = 5 * 60 * 60 * 1000L; // 5 min ago
    boolean enableSubBucketQuery = true;
    boolean allowEmptyIterator = false;
    
    private final long summaryLogInterval = 5 * 60L; // 5 minutes
    private long lastSummary = 0L;
    private long numLocalArtifacts = 0L;
    private long numRemoteArtifacts = 0L;
    private long numMatchedArtifacts = 0L;
    
    private int numValidBuckets = 0;
    private int numFailedBuckets = 0;
    
    /**
     * Constructor.
     *
     * @param connectionConfig   database connection config
     * @param daoConfig          config map to pass to cadc-inventory-db DAO classes
     * @param resourceID         identifier for the remote query service
     * @param artifactSelector   artifact selector implementation
     * @param bucketSelector     uri buckets
     * @param trackSiteLocations local site type
     */
    public InventoryValidator(ConnectionConfig connectionConfig, Map<String, Object> daoConfig, 
            URI resourceID, ArtifactSelector artifactSelector,
            BucketSelector bucketSelector, boolean trackSiteLocations) {
        InventoryUtil.assertNotNull(InventoryValidator.class, "connectionConfig", connectionConfig);
        InventoryUtil.assertNotNull(InventoryValidator.class, "daoConfig", daoConfig);
        InventoryUtil.assertNotNull(InventoryValidator.class, "resourceID", resourceID);
        InventoryUtil.assertNotNull(InventoryValidator.class, "artifactSelector", artifactSelector);

        // copy config
        Map<String,Object> txnConfig = new TreeMap<>();
        txnConfig.putAll(daoConfig);
        
        try {
            DBUtil.createJNDIDataSource("jdbc/inventory", connectionConfig);
        } catch (NamingException ne) {
            throw new IllegalStateException(String.format("Unable to access database: %s", connectionConfig.getURL()), ne);
        }
        daoConfig.put("jndiDataSourceName", "jdbc/inventory");
        
        try {
            DBUtil.createJNDIDataSource("jdbc/inventory-txn", connectionConfig);
        } catch (NamingException ne) {
            throw new IllegalStateException(String.format("Unable to access database: %s", connectionConfig.getURL()), ne);
        }
        txnConfig.put("jndiDataSourceName", "jdbc/inventory-txn");

        try {
            String jndiDataSourceName = (String) daoConfig.get("jndiDataSourceName");
            String database = (String) daoConfig.get("database");
            String schema = (String) daoConfig.get("schema");
            DataSource ds = DBUtil.findJNDIDataSource(jndiDataSourceName);
            InitDatabase init = new InitDatabase(ds, database, schema);
            init.doInit();
            log.info(String.format("initDatabase: %s %s", jndiDataSourceName, schema));
        } catch (Exception ex) {
            throw new IllegalStateException("check/init database failed", ex);
        }

        this.artifactDAO = new ArtifactDAO(false);
        this.artifactDAO.setConfig(daoConfig);
        this.resourceID = resourceID;
        this.trackSiteLocations = trackSiteLocations;
        this.artifactSelector = artifactSelector;
        this.bucketSelector = bucketSelector;
        
        try {
            RegistryClient rc = new RegistryClient();
            URL capURL = rc.getAccessURL(resourceID);
            if (capURL == null) {
                throw new IllegalArgumentException("invalid config: query service not found: " + resourceID);
            }
        } catch (ResourceNotFoundException ex) {
            throw new IllegalArgumentException("invalid config: query service not found: " + resourceID);
        } catch (IOException ex) {
            throw new IllegalArgumentException("invalid config", ex);
        }

        ArtifactDAO txnDAO = new ArtifactDAO(false);
        txnDAO.setConfig(txnConfig);
        this.artifactValidator = new ArtifactValidator(txnDAO, resourceID, this.artifactSelector);

        try {
            this.messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("error creating MessageDigest with MD5 algorithm", e);
        }
    }

    // Package access constructor for unit testing.
    InventoryValidator() {
        this.artifactDAO = null;
        this.resourceID = null;
        this.trackSiteLocations = false;
        this.artifactSelector = null;
        this.bucketSelector = null;
        this.artifactValidator = null;
        this.messageDigest = null;
    }

    @Override 
    public void run() {
        try {
            Subject subject = AuthenticationUtil.getAnonSubject();
            File certFile = new File(CERTIFICATE_FILE_LOCATION);
            if (certFile.exists()) {
                subject = SSLUtil.createSubject(certFile);
            } else {
                log.info("not found: " + certFile + " -- proceeding anonymously");
            }
            Subject.doAs(subject, (PrivilegedExceptionAction<Void>) () -> {
                doit();
                logSummary(true, false);
                return null;
            });
        } catch (PrivilegedActionException privilegedActionException) {
            final Exception exception = privilegedActionException.getException();
            log.error(InventoryValidator.class.getSimpleName() + ".ABORT", exception);
        }
    }

    void doit() throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                       InterruptedException {

        if (trackSiteLocations) {
            try {
                StorageSite ss = getRemoteStorageSite(resourceID);
                // verify with local
                StorageSiteDAO sdao = new StorageSiteDAO(artifactDAO);
                StorageSite knownSite = sdao.get(ss.getID());
                if (knownSite == null) {
                    throw new IllegalStateException("remote site " + ss.getID() + " (" + ss.getResourceID() + ") "
                        + "not found in local database -- cannot validate unsynced site");
                }
                this.remoteSite = ss;
                artifactValidator.setRemoteSite(remoteSite);
            } catch (ResourceNotFoundException ex) {
                throw new IllegalArgumentException("query service not found: " + resourceID, ex);
            } catch (TransientException | IOException | InterruptedException ex) {
                throw new IllegalArgumentException("remote StorageSite query failed", ex);
            }
        } else {
            this.remoteSite = null;
            artifactValidator.setRemoteSite(null);
        }
        
        
        List<String> buckets = new ArrayList<>();
        BucketSelector allBuckets = new BucketSelector("0-f");
        if (this.bucketSelector == null) {
            if (enableSubBucketQuery) {
                Iterator<String> inner = allBuckets.getBucketIterator();
                while (inner.hasNext()) {
                    String b = inner.next();
                    buckets.add(b);
                }
            }
        } else {
            Iterator<String> outer = bucketSelector.getBucketIterator();
            while (outer.hasNext()) {
                String b1 = outer.next();
                if (enableSubBucketQuery) {
                    Iterator<String> inner = allBuckets.getBucketIterator();
                    while (inner.hasNext()) {
                        String b2 = inner.next();
                        String b = b1 + b2;
                        buckets.add(b);
                    }
                } else {
                    buckets.add(b1);
                }
            }
        }
        
        if (buckets.isEmpty()) {
            iterateBucket(null);
        } else {
            Iterator<String> bucketIterator = buckets.iterator();
            while (bucketIterator.hasNext()) {
                String bucket = bucketIterator.next();
                log.info(InventoryValidator.class.getSimpleName() + ".START bucket=" + bucket);
                int retries = 0;
                boolean done = false;
                while (!done && retries < 3) {
                    long nloc = numLocalArtifacts;
                    long nrem = numRemoteArtifacts;
                    long nmatch = numMatchedArtifacts;
                    try {
                        iterateBucket(bucket);
                        log.info(InventoryValidator.class.getSimpleName() + ".END bucket=" + bucket);
                        numValidBuckets++;
                        done = true;
                    } catch (IOException | TransientException ex) {
                        log.error(InventoryValidator.class.getSimpleName() + ".FAIL bucket=" + bucket, ex);
                        numFailedBuckets++;
                        retries++;
                    } catch (RuntimeException ex) {
                        // TODO: probably not a great idea to retry on these...
                        log.error(InventoryValidator.class.getSimpleName() + ".FAIL bucket=" + bucket, ex);
                        numFailedBuckets++;
                        retries++;
                    } catch (Exception ex) {
                        log.error(InventoryValidator.class.getSimpleName() + ".FAIL bucket=" + bucket, ex);
                        numFailedBuckets++;
                        throw ex;
                    } finally {
                        if (!done) {
                            // revert count changes from this failed bucket
                            numLocalArtifacts = nloc;
                            numRemoteArtifacts = nrem;
                            numMatchedArtifacts = nmatch;
                        }
                    }
                }
            }
        }
    }

    /**
     * Validates sets of Artifacts from the given bucket.
     *
     * @param bucket    The Artifact bucket to validate.
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    void iterateBucket(String bucket)
        throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
               InterruptedException {
        log.debug("processing bucket: " + bucket);
        // set this before query
        artifactValidator.setRaceConditionStart(new Date(System.currentTimeMillis() - raceConditionDelta));
        logSummary(true, true);
        try (final ResourceIterator<Artifact> localIterator = getLocalIterator(bucket);
            final ResourceIterator<Artifact> remoteIterator = getRemoteIterator(bucket)) {
            
            Artifact local = null;
            Artifact remote = null;
            boolean artifactsToValidate = true;
            while (artifactsToValidate) {
                if (local == null) {
                    local = localIterator.hasNext() ? localIterator.next() : null;
                }
                if (remote == null) {
                    remote = remoteIterator.hasNext() ? remoteIterator.next() : null;
                }
                // TODO sanity check? if either iterator has no results in the first loop, exit?
                if (local == null && remote == null) {
                    artifactsToValidate = false;
                    continue;
                }

                if (remote != null) {
                    final URI computedChecksum = remote.computeMetaChecksum(this.messageDigest);
                    if (!remote.getMetaChecksum().equals(computedChecksum)) {
                        throw new IllegalStateException(
                            "remote checksum mismatch: " + remote.getID() + " " + remote.getURI() + " provided="
                                + remote.getMetaChecksum() + " actual=" + computedChecksum);
                    }
                }
                log.debug(String.format("comparing Artifacts:\n local - %s\nremote - %s", local, remote));

                // check if Artifacts are the same, or if the local Artifact
                // precedes or follows the remote Artifact.
                int order = orderArtifacts(local, remote);
                switch (order)  {
                    case -1:
                        validate(local, null);
                        local = null;
                        break;
                    case 0:
                        validate(local, remote);
                        local = null;
                        remote = null;
                        break;
                    case 1:
                        validate(null, remote);
                        remote = null;
                        break;
                    default:
                        String message = String.format("Illegal Artifact order %s for:\n local - %s\remote - %s",
                                                       order, local, remote);
                        throw new IllegalStateException(message);
                }
            }
        } catch (IOException ex) {
            //log.error("Error closing iterator", ex);
            throw new RuntimeException("error while closing ResourceIterator(s)", ex);
        }
    }

    /**
     * Useful for overriding in tests.
     *
     * @param local the local Artifact.
     * @param remote the remote Artifact.
     */
    void validate(Artifact local, Artifact remote)
        throws InterruptedException, ResourceNotFoundException, TransientException, IOException {
        log.debug(String.format("validating:\n local - %s\nremote - %s", local, remote));
        if (local != null) {
            numLocalArtifacts++;
        }
        if (remote != null) {
            numRemoteArtifacts++;
        }
        if (local != null && remote != null) {
            numMatchedArtifacts++;
        }
        artifactValidator.validate(local, remote);
        
        logSummary(false, true);
        
    }
    
    private void logSummary(boolean force, boolean next) {
        long sec = System.currentTimeMillis() / 1000L;
        long dt = (sec - lastSummary);
        if (!force && dt < summaryLogInterval) {
            return;
        }
        this.lastSummary = sec;
        
        StringBuilder sb = new StringBuilder();
        sb.append(InventoryValidator.class.getSimpleName()).append(".summary");
        sb.append(" numLocal=").append(numLocalArtifacts);
        sb.append(" numRemote=").append(numRemoteArtifacts);
        sb.append(" numMatched=").append(numMatchedArtifacts);
        sb.append(" numValidBuckets=").append(numValidBuckets);
        sb.append(" numFailedBuckets=").append(numFailedBuckets);
        
        if (next) {
            sb.append(" nextSummaryIn=").append(summaryLogInterval).append("sec");
        }
        log.info(sb.toString());
    }

    /**
     * Order two Artifacts on the String representation of Artifact.uri.
     * Must match the ordering of a postgresql ORDER BY ASC on Artifact.uri.
     * If one Artifact is null it is considered to follow the other Artifact.
     *
     * <p>returns -1:
     *  - if remote is null (remote orders after local)
     *  - if local uri lexicographically orders before remote uri
     * return 0:
     *  - if local and remote are null
     *  - if local uri equals remote uri
     * return 1:
     *  - if local is null (local orders after remote)
     *  - if local uri lexicographically orders after remote uri
     */
    int orderArtifacts(Artifact local, Artifact remote) {
        log.debug(String.format("order artifact uri's:\n local - %s\nremote - %s",
                                local == null ? "null" : local.getURI(),
                                remote == null ? "null" : remote.getURI()));
        int order;
        if (local == null && remote == null) {
            order = 0;
        } else if (remote == null) {
            order = -1;
        } else if (local == null) {
            order = 1;
        } else {
            // returns int < 0 if local precedes remote
            // returns 0 if local equals remote
            // returns int > 0 if local follows remote
            int uriOrder = local.getURI().compareTo(remote.getURI());

            // returns -1 if uriOrder < 0, 0 if uriOrder = 0, 1 if uriOrder > 0
            order = Integer.compare(uriOrder, 0);
        }
        return order;
    }

    /**
     * Get local artifacts matching the uriBuckets.
     *
     * @param bucket The bucket prefix.
     * @return ResourceIterator over Artifact's matching the uri buckets.
     *
     * @throws IOException               For unreadable configuration files.
     */
    ResourceIterator<Artifact> getLocalIterator(final String bucket)
        throws IOException {
        // order query results by Artifact.uri
        boolean ordered = true;
        final long t1 = System.currentTimeMillis();
        log.debug(InventoryValidator.class.getSimpleName() + ".localQuery bucket=" + bucket);
        UUID remoteSiteID = null;
        if (this.remoteSite != null) {
            remoteSiteID = this.remoteSite.getID();
        }
        ResourceIterator<Artifact> ret = this.artifactDAO.iterator(remoteSiteID, bucket, ordered);
        if (!ret.hasNext() && !allowEmptyIterator) {
            throw new TransientException("something looks sketchy: local query found empty bucket");
        }
        long dt = System.currentTimeMillis() - t1;
        log.info(InventoryValidator.class.getSimpleName() + ".localQuery bucket=" + bucket + " duration=" + dt);
        return ret;
    }

    /**
     * Execute the query and return the iterator back.
     *
     * @param bucket The bucket prefix.
     * @param retryCount track number of attempts in recursive retry
     * @return ResourceIterator over Artifact's matching the remote filter policy and the uri buckets.
     *
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    ResourceIterator<Artifact> getRemoteIterator(final String bucket)
        throws ResourceNotFoundException, IOException, IllegalStateException, TransientException, InterruptedException {
        final TapClient<Artifact> tapClient = new TapClient<>(this.resourceID);
        tapClient.setConnectionTimeout(12000); // 12 sec
        tapClient.setReadTimeout(120000);      // 120 sec
        final String query = buildRemoteQuery(bucket);
        log.debug(InventoryValidator.class.getSimpleName() + ".remoteQuery bucket=" + bucket 
                + "query: \n'" + query + "\n");

        TransientException tex = null;
        int retryCount = 0;
        while (retryCount <= 3) {
            final long t1 = System.currentTimeMillis();
            try {
                log.debug(InventoryValidator.class.getSimpleName() + ".remoteQuery bucket=" + bucket);
                ResourceIterator<Artifact> ret = tapClient.query(query, new CountingArtifactRowMapper(), true);
                long dt = System.currentTimeMillis() - t1;
                log.info(InventoryValidator.class.getSimpleName() + ".remoteQuery bucket=" + bucket + " duration=" + dt);
                if (!ret.hasNext() && !allowEmptyIterator) {
                    throw new TransientException("something looks sketchy: remote query found empty bucket");
                }
                return ret;
            } catch (TransientException ex) {
                tex = ex;
                long dt = System.currentTimeMillis() - t1;
                log.warn(InventoryValidator.class.getSimpleName() + ".remoteQuery bucket=" + bucket + " duration=" + dt
                    + " success=false reason=" + ex);
                retryCount++;
                Thread.sleep(retryCount * 2000L);
            }
        }
        if (tex == null) {
            throw new RuntimeException("BUG: finished retry loop without success or TransientException");
        }
        throw tex;
    }
    
    private class CountingArtifactRowMapper extends ArtifactRowMapper {
        private long cur = 0L;
        
        @Override
        public Artifact mapRow(List<Object> row) {
            cur++;
            Artifact ret = super.mapRow(row);
            Long rownum = (Long) row.get(row.size() - 1);
            if (rownum == null || rownum != cur) {
                throw new TransientException("detected broken query result stream: expected row " + cur + "found: " + rownum);
            }
            return ret;
        }
    }

    /**
     * Assemble the WHERE clause and return the full query.  Very useful for testing separately.
     *
     * @param bucket The current bucket.
     * @return  String where clause. Never null.
     */
    String buildRemoteQuery(final String bucket)
        throws ResourceNotFoundException, IOException {
        final StringBuilder query = new StringBuilder();
        query.append(String.format("%s, row_counter() %s",  ArtifactRowMapper.SELECT,  ArtifactRowMapper.FROM));

        if (StringUtil.hasText(this.artifactSelector.getConstraint())) {
            if (query.indexOf("WHERE") < 0) {
                query.append(" WHERE ");
            } else {
                query.append(" AND ");
            }
            query.append("(").append(this.artifactSelector.getConstraint().trim()).append(")");
        }

        if (StringUtil.hasText(bucket)) {
            if (query.indexOf("WHERE") < 0) {
                query.append(" WHERE ");
            } else {
                query.append(" AND ");
            }
            query.append("(uriBucket LIKE '").append(bucket.trim()).append("%')");
            log.debug("where clause: " + query.toString());
        }

        query.append(" ORDER BY uri ASC");
        return query.toString();
    }

    /**
     * Get the StorageSite for the remote instance resourceID;
     */
    private StorageSite getRemoteStorageSite(URI resourceID)
            throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final TapClient<StorageSite> tapClient = new TapClient<>(resourceID);
        final String query = "SELECT id, resourceID, name, allowRead, allowWrite, lastModified, metaChecksum "
            + "FROM inventory.StorageSite";
        log.debug("\nExecuting query '" + query + "'\n");
        StorageSite storageSite = null;
        ResourceIterator<StorageSite> results = tapClient.query(query, new StorageSiteRowMapper());
        if (results.hasNext()) {
            storageSite = results.next();
            if (results.hasNext()) {
                throw new IllegalStateException(String.format("Multiple StorageSite's found for site %s",
                                                              resourceID.toASCIIString()));
            }
        }
        return storageSite;
    }

    /**
     * Class to map the query results to a StorageSite.
     */
    class StorageSiteRowMapper implements TapRowMapper<StorageSite> {
        @Override
        public StorageSite mapRow(List<Object> row) {
            int index = 0;
            final UUID id = (UUID) row.get(index++);
            final URI resourceID = (URI) row.get(index++);
            final String name = (String) row.get(index++);
            final boolean allowRead = (Boolean) row.get(index++);
            final boolean allowWrite = (Boolean) row.get(index++);

            final StorageSite storageSite = new StorageSite(id, resourceID, name, allowRead, allowWrite);
            InventoryUtil.assignLastModified(storageSite, (Date) row.get(index++));
            InventoryUtil.assignMetaChecksum(storageSite, (URI) row.get(index));
            return storageSite;
        }
    }

}

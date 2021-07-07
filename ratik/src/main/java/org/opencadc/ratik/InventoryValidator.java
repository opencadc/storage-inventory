/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2021.                            (c) 2021.
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

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.StringUtil;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.version.InitDatabase;
import org.opencadc.inventory.query.ArtifactRowMapper;
import org.opencadc.inventory.util.ArtifactSelector;
import org.opencadc.inventory.util.BucketSelector;
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
    private final StorageSite remoteSite;
    private final ArtifactSelector artifactSelector;
    private final BucketSelector bucketSelector;
    private final ArtifactValidator artifactValidator;
    private final MessageDigest messageDigest;
    
    private long numLocalArtifacts = 0L;
    private long numRemoteArtifacts = 0L;
    private long numMatchedArtifacts = 0L;

    /**
     * Constructor.
     *
     * @param daoConfig          config map to pass to cadc-inventory-db DAO classes
     * @param resourceID         identifier for the remote query service
     * @param artifactSelector   artifact selector implementation
     * @param bucketSelector     uri buckets
     * @param trackSiteLocations local site type
     */
    public InventoryValidator(Map<String, Object> daoConfig, URI resourceID, ArtifactSelector artifactSelector,
                              BucketSelector bucketSelector, boolean trackSiteLocations) {
        InventoryUtil.assertNotNull(InventoryValidator.class, "daoConfig", daoConfig);
        InventoryUtil.assertNotNull(InventoryValidator.class, "resourceID", resourceID);
        InventoryUtil.assertNotNull(InventoryValidator.class, "artifactSelector", artifactSelector);

        this.artifactDAO = new ArtifactDAO(false);
        this.artifactDAO.setConfig(daoConfig);
        this.resourceID = resourceID;
        this.artifactSelector = artifactSelector;
        this.bucketSelector = bucketSelector;

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

        try {
            RegistryClient rc = new RegistryClient();
            Capabilities caps = rc.getCapabilities(resourceID);
            // above call throws IllegalArgumentException... should be ResourceNotFoundException but out of scope to fix
            Capability capability = caps.findCapability(Standards.TAP_10);
            if (capability == null) {
                throw new IllegalArgumentException(
                    "invalid config: remote query service " + resourceID + " does not implement " + Standards.TAP_10);
            }
        } catch (ResourceNotFoundException ex) {
            throw new IllegalArgumentException("query service not found: " + resourceID, ex);
        } catch (IOException ex) {
            throw new IllegalArgumentException("invalid config", ex);
        }

        if (trackSiteLocations) {
            try {
                this.remoteSite = getRemoteStorageSite(resourceID);
            } catch (ResourceNotFoundException ex) {
                throw new IllegalArgumentException("query service not found: " + resourceID, ex);
            } catch (Exception ex) {
                throw new IllegalArgumentException("remote StorageSite query failed", ex);
            }
        } else {
            this.remoteSite = null;
        }
        this.artifactValidator = new ArtifactValidator(this.artifactDAO, resourceID, remoteSite);

        try {
            this.messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("error creating MessageDigest with MD5 algorithm", e);
        }
    }

    // Package access constructor for testing.
    InventoryValidator(ArtifactDAO artifactDAO, URI resourceID, StorageSite remoteSite,
                       ArtifactSelector artifactSelector, BucketSelector bucketSelector,
                       ArtifactValidator artifactValidator, MessageDigest messageDigest) {
        this.artifactDAO = artifactDAO;
        this.resourceID = resourceID;
        this.remoteSite = remoteSite;
        this.artifactSelector = artifactSelector;
        this.bucketSelector = bucketSelector;
        this.artifactValidator = artifactValidator;
        this.messageDigest = messageDigest;
    }

    @Override public void run() {
        try {
            final Subject subject = SSLUtil.createSubject(new File(CERTIFICATE_FILE_LOCATION));
            Subject.doAs(subject, (PrivilegedExceptionAction<Void>) () -> {
                doit();
                log.info(this.getClass().getSimpleName() + ".summary numLocal= " + numLocalArtifacts
                    + " numRemote=" + numRemoteArtifacts + " numMatched=" + numMatchedArtifacts);
                return null;
            });
        } catch (PrivilegedActionException privilegedActionException) {
            final Exception exception = privilegedActionException.getException();
            throw new IllegalStateException(exception.getMessage(), exception);
        }
    }

    /**
     * Validates local and remote sets of Artifacts.
     *
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    void doit() throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                       InterruptedException {

        if (this.bucketSelector == null) {
            iterateBucket(null);
        } else {
            Iterator<String> bucketIterator = this.bucketSelector.getBucketIterator();
            while (bucketIterator.hasNext()) {
                iterateBucket(bucketIterator.next());
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

                // overridable method for int-tests
                testAction();

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
        } catch (IOException e) {
            log.error("Error closing iterator: " + e.getMessage());
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
     * @return ResourceIterator over Artifact's matching the remote filter policy and the uri buckets.
     *
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     */
    ResourceIterator<Artifact> getLocalIterator(final String bucket)
        throws ResourceNotFoundException, IOException {
        String constraint = null;
        if (StringUtil.hasText(this.artifactSelector.getConstraint())) {
            constraint = this.artifactSelector.getConstraint().trim();
        }
        // order query results by Artifact.uri
        boolean ordered = true;
        long t1 = System.currentTimeMillis();
        log.debug(this.getClass().getSimpleName() + ".localQuery bucket=" + bucket);
        ResourceIterator<Artifact> ret = this.artifactDAO.iterator(constraint, bucket, ordered);
        long dt = System.currentTimeMillis() - t1;
        log.info(this.getClass().getSimpleName() + ".localQuery bucket=" + bucket + " duration=" + dt);
        return ret;
    }

    /**
     * Execute the query and return the iterator back.
     *
     * @param bucket The bucket prefix.
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
        final String query = buildRemoteQuery(bucket);
        log.debug("\nExecuting query '" + query + "'\n");
        long t1 = System.currentTimeMillis();
        log.debug(this.getClass().getSimpleName() + ".remoteQuery bucket=" + bucket);
        ResourceIterator<Artifact> ret = tapClient.execute(query, new ArtifactRowMapper());
        long dt = System.currentTimeMillis() - t1;
        log.info(this.getClass().getSimpleName() + ".remoteQuery bucket=" + bucket + " duration=" + dt);
        return ret;
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
        query.append(ArtifactRowMapper.BASE_QUERY);

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
            query.append("(uribucket LIKE '").append(bucket.trim()).append("%')");
            log.debug("where clause: " + query.toString());
        }

        query.append(" ORDER BY uri ASC");
        return query.toString();
    }

    /**
     * Method that can be overridden in tests to insert Artifact's after the iterator queries.
     */
    void testAction() { }

    /**
     * Get the StorageSite for the remote instance resourceID;
     */
    private StorageSite getRemoteStorageSite(URI resourceID)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        try {
            final Subject subject = SSLUtil.createSubject(new File(CERTIFICATE_FILE_LOCATION));
            return Subject.doAs(subject, (PrivilegedExceptionAction<StorageSite>) () -> {
                final TapClient<StorageSite> tapClient = new TapClient<>(resourceID);
                final String query = "SELECT id, resourceID, name, allowRead, allowWrite, lastModified, metaChecksum "
                    + "FROM inventory.StorageSite";
                log.debug("\nExecuting query '" + query + "'\n");
                StorageSite storageSite = null;
                ResourceIterator<StorageSite> results = tapClient.execute(query, new StorageSiteRowMapper());
                if (results.hasNext()) {
                    storageSite = results.next();
                    if (results.hasNext()) {
                        throw new IllegalStateException(String.format("Multiple StorageSite's found for site %s",
                                                                      resourceID.toASCIIString()));
                    }
                }
                return storageSite;
            });
        } catch (PrivilegedActionException privilegedActionException) {
            final Exception exception = privilegedActionException.getException();
            throw new IllegalStateException(exception.getMessage(), exception);
        }
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

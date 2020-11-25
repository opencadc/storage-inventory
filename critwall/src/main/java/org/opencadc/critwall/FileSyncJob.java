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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.log.EventLogInfo;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferParsingException;
import ca.nrc.cadc.vos.TransferReader;
import ca.nrc.cadc.vos.TransferWriter;
import ca.nrc.cadc.vos.VOS;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.EntityNotFoundException;
import org.opencadc.inventory.db.ObsoleteStorageLocation;
import org.opencadc.inventory.db.ObsoleteStorageLocationDAO;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;


public class FileSyncJob implements Runnable {
    private static final Logger log = Logger.getLogger(FileSyncJob.class);

    private static final long[] RETRY_DELAY = new long[] { 6000L, 12000L };
    private static final String LABEL = FileSyncJob.class.getName();

    private final ArtifactDAO artifactDAO;
    private final UUID artifactID;
    private final URI locatorService;
    private final StorageAdapter storageAdapter;
    private final Subject subject;
    
    /**
     * Construct a job to sync the specified artifact.
     * 
     * @param artifactID entity ID of artifact to sync
     * @param locatorServiceID locator service to use
     * @param storageAdapter back end storage
     * @param artifactDAO database persistence
     * @param subject caller with credentials for downloads
     */
    public FileSyncJob(UUID artifactID, URI locatorServiceID, StorageAdapter storageAdapter, ArtifactDAO artifactDAO, Subject subject) {
        InventoryUtil.assertNotNull(FileSyncJob.class, "artifactID", artifactID);
        InventoryUtil.assertNotNull(FileSyncJob.class, "locatorServiceID", locatorServiceID);
        InventoryUtil.assertNotNull(FileSyncJob.class, "storageAdapter", storageAdapter);
        InventoryUtil.assertNotNull(FileSyncJob.class, "artifactDAO", artifactDAO);

        this.artifactID = artifactID;
        this.locatorService = locatorServiceID;
        this.storageAdapter = storageAdapter;

        this.artifactDAO = artifactDAO;
        this.subject = subject;
    }

    @Override
    public void run() {
        Subject currentSubject = new Subject();

        // Also synchronized in FileSync.run()
        synchronized (subject) {
            currentSubject.getPrincipals().addAll(subject.getPrincipals());
            currentSubject.getPublicCredentials().addAll(subject.getPublicCredentials());
        }
        Subject.doAs(currentSubject, new RunnableAction(this::doSync));
    }

    // approach here is conservative: if the input artifact changed|deleted in the database, 
    // the job will abort
    // in cases where the artifact changed, it will be picked up again and syn'ed later
    // - note: we only have to worry about changes in Artifact.uri because it may be made mutable in future
    //         Artifact.contentChecksum and Artifact.contentLength are immutable      
    private void doSync() {
        
        EventLogInfo syncEventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, LABEL, "SYNC");
        syncEventLogInfo.setEntityID(artifactID);
        log.info(syncEventLogInfo.start());
        long start = System.currentTimeMillis();
        boolean success = false;
        String msg = "";
        
        try {
            // get current artifact to sync
            final Artifact artifact = artifactDAO.get(artifactID);
            if (artifact == null || artifact.storageLocation != null) {
                msg = "artifact " + artifactID + " changed|deleted since job created";
                return;
            }
            // from here on we care about URI change since that's the only potentially mutable metadata 
            // that could get persisted in the back end
            String artifactLabel = artifact.getID().toString() + "|" + artifact.getURI().toASCIIString();
            List<URL> urlList;
            
            try {
                urlList = getDownloadURLs(this.locatorService, artifact.getURI());
                if (urlList.isEmpty()) {
                    msg = " locator returned 0 URLs";
                    return;
                }
            } catch (Exception ex) {
                log.debug("transfer negotiation failed: " + artifactLabel, ex);
                msg = " transfer negotiation failed: " + artifactLabel + " (" + ex + ")";
                return;
            }
            
            int retryCount = 0;
            try {
                while (!success && !urlList.isEmpty() && retryCount < RETRY_DELAY.length) {
                    Artifact curArtifact = artifactDAO.get(artifactID);
                    if (curArtifact == null 
                            || !curArtifact.getURI().equals(artifact.getURI())
                            || curArtifact.storageLocation != null) {
                        msg = "artifact " + artifactID + " changed|deleted since job started [before sync]";
                        return;
                    }
                    
                    // attempt to sync file
                    EventLogInfo retryEventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, LABEL, "SYNC");
                    retryEventLogInfo.setArtifactURI(artifact.getURI());
                    retryEventLogInfo.setEntityID(artifact.getID());
                    retryEventLogInfo.setAttempts(retryCount);
                    log.info(retryEventLogInfo.singleEvent());
                    StorageMetadata storageMeta = syncArtifact(curArtifact, urlList);

                    // sync succeeded: update inventory
                    if (storageMeta != null) {
                        ObsoleteStorageLocationDAO locDAO = new ObsoleteStorageLocationDAO(artifactDAO);
                        TransactionManager txnMgr = artifactDAO.getTransactionManager();
                        try {
                            // this transaction is ~equivalent to minoc/PutAction
                            log.debug("starting transaction");
                            txnMgr.startTransaction();
                            log.debug("start txn: OK");
                            
                            artifactDAO.lock(artifact);
                            curArtifact = artifactDAO.get(artifactID);
                            
                            ObsoleteStorageLocation prevOSL = locDAO.get(storageMeta.getStorageLocation());
                            if (prevOSL != null) {
                                // no longer obsolete
                                locDAO.delete(prevOSL.getID());
                            }

                            ObsoleteStorageLocation obsLoc = null;
                            if (curArtifact == null || !curArtifact.getURI().equals(artifact.getURI())) {
                                msg = "artifact " + artifactID + " changed|deleted since job started [after sync]";
                                obsLoc = new ObsoleteStorageLocation(storageMeta.getStorageLocation());
                                locDAO.put(obsLoc);
                            } else {
                                // just in case someone else assigned a StorageLocation: one is now obsolete
                                if (curArtifact.storageLocation != null) {
                                    if (!artifact.storageLocation.equals(curArtifact.storageLocation)) {
                                        obsLoc = new ObsoleteStorageLocation(curArtifact.storageLocation);
                                        locDAO.put(obsLoc);
                                    }
                                }
                                artifactDAO.setStorageLocation(curArtifact, storageMeta.getStorageLocation());
                                success = true;
                                msg = "uri=" + artifact.getURI();
                            }

                            txnMgr.commitTransaction();
                            log.debug("commit txn: OK");
                            
                            if (obsLoc != null) {
                                try {
                                    log.debug("deleting obsolete stored object: " + obsLoc.getLocation());
                                    storageAdapter.delete(obsLoc.getLocation());
                                    // obsolete tracker record no longer needed
                                    locDAO.delete(obsLoc.getID()); // outside txn, auto-commit mode
                                } catch (Exception ex) {
                                    // OK to continue in this case
                                    log.error("failed to remove obsolete stored object: " + obsLoc.getLocation(), ex);
                                }
                            }
                            
                        } catch (Exception e) {
                            log.error("failed to persist " + artifactID, e);
                            txnMgr.rollbackTransaction();
                            log.debug("rollback txn: OK");
                            throw e;
                        } finally {
                            if (txnMgr.isOpen()) {
                                log.error("BUG - open transaction in finally");
                                txnMgr.rollbackTransaction();
                                log.error("rollback txn: OK");
                            }
                        }
                    }
                    
                    if (!success) {
                        EventLogInfo sleepEventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, LABEL, "SLEEP");
                        sleepEventLogInfo.setElapsedTime(RETRY_DELAY[retryCount]);
                        log.info(sleepEventLogInfo.singleEvent());
                        Thread.sleep(RETRY_DELAY[retryCount++]);
                    }
                }
                if (!success) {
                    msg = " all URLs tried, retryCount=" + retryCount;
                }
            } catch (IllegalStateException | EntityNotFoundException ex) {
                log.debug("artifact sync aborted: " + artifactLabel, ex);
                msg = " artifact sync aborted: " + artifactLabel + " (" + ex + ")";
            } catch (IllegalArgumentException | InterruptedException | StorageEngageException | WriteException ex) {
                log.debug("artifact sync error: " + artifactLabel, ex);
                msg = " artifact sync error: " + artifactLabel + " (" + ex + ")";
            } catch (Exception ex) {
                log.debug("unexpected fail: " + artifactLabel, ex);
                msg = " unexpected sync error: " + artifactLabel + " (" + ex + ")";
            }
        } finally {
            long dt = System.currentTimeMillis() - start;
            EventLogInfo syncEndEventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, LABEL, "SYNC");
            syncEndEventLogInfo.setElapsedTime(dt);
            syncEndEventLogInfo.setSuccess(success);
            syncEndEventLogInfo.setMessage(msg);
            log.info(syncEndEventLogInfo.end());
        }
    }

    // Use transfer negotiation at resource URI to get list of download URLs for the artifact.
    private List<URL> getDownloadURLs(URI resource, URI artifact)
        throws IOException, InterruptedException,
        ResourceAlreadyExistsException, ResourceNotFoundException,
        TransientException, TransferParsingException {

        RegistryClient regClient = new RegistryClient();
        Subject subject = AuthenticationUtil.getCurrentSubject();
        AuthMethod am = AuthenticationUtil.getAuthMethodFromCredentials(subject);
        log.debug("resource id: " + resource);
        URL transferURL = regClient.getServiceURL(resource, Standards.SI_LOCATE, am);
        if (transferURL == null) {
            transferURL = regClient.getServiceURL(resource, Standards.VOSPACE_SYNC_21, am);
        }
        log.debug("certURL: " + transferURL);

        // request all protocols that can be used
        List<Protocol> protocolList = new ArrayList<>();
        protocolList.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));
        protocolList.add(new Protocol(VOS.PROTOCOL_HTTP_GET));
        if (!AuthMethod.ANON.equals(am)) {
            Protocol httpsAuth = new Protocol(VOS.PROTOCOL_HTTPS_GET);
            httpsAuth.setSecurityMethod(Standards.getSecurityMethod(am));
            protocolList.add(httpsAuth);
        }

        Transfer transfer = new Transfer(artifact, Direction.pullFromVoSpace, protocolList);
        transfer.version = VOS.VOSPACE_21;

        TransferWriter writer = new TransferWriter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.write(transfer, out);
        FileContent content = new FileContent(out.toByteArray(), "text/xml");
        log.debug("xml file content to be posted: " + transfer);

        log.debug("artifact path: " + artifact.getPath());
        HttpPost post = new HttpPost(transferURL, content, true);
        post.prepare();
        log.debug("post prepare done");

        TransferReader reader = new TransferReader();
        Transfer t = reader.read(post.getInputStream(), null);
        List<String> urlStrList = t.getAllEndpoints();
        log.debug("endpoints returned: " + urlStrList);

        List<URL> urlList = new ArrayList<>();

        // Create URL list to return
        for (final String s : urlStrList) {
            try {
                urlList.add(new URL(s));
            } catch (MalformedURLException mue) {
                String msg = "malformed URL returned from transfer negotiation: " + s + " skipping... ";
                EventLogInfo urlEventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, LABEL, "CREATE");
                urlEventLogInfo.setMessage(msg);
                log.info(urlEventLogInfo.singleEvent());
            }
        }

        return urlList;
    }

    private StorageMetadata syncArtifact(Artifact a, List<URL> urls)
        throws ByteLimitExceededException, StorageEngageException, InterruptedException, WriteException, IllegalArgumentException {

        StorageMetadata storageMeta = null;
        Iterator<URL> urlIterator = urls.iterator();

        while (urlIterator.hasNext()) {
            URL u = urlIterator.next();
            log.debug("trying " + u);

            try {
                ByteArrayOutputStream dest = new ByteArrayOutputStream();
                HttpGet get = new HttpGet(u, dest);
                get.prepare();

                // Note: the storage adapter 'put' below does checksum and content length
                // checks, but only after downloading the entire file.
                // Making the checks here is more efficient.
                String getContentMD5 = get.getContentMD5();
                if (getContentMD5 != null
                    && !getContentMD5.equals(a.getContentChecksum().getSchemeSpecificPart())) {
                    throw new PreconditionFailedException("contentChecksum mismatch: " + a.getURI());
                }

                long getContentLen = get.getContentLength();
                if (getContentLen != -1
                    && getContentLen != a.getContentLength()) {
                    throw new PreconditionFailedException("contentLength mismatch: " + a.getURI()
                        + " artifact: " + a.getContentLength() + " header: " + getContentLen);
                }

                NewArtifact na = new NewArtifact(a.getURI());
                na.contentChecksum = a.getContentChecksum();
                na.contentLength = a.getContentLength();

                storageMeta = this.storageAdapter.put(na, get.getInputStream());
                log.debug("storage meta returned: " + storageMeta.getStorageLocation());
                return storageMeta;

            } catch (ByteLimitExceededException | WriteException ex) {
                // IOException will capture this if not explicitly caught and rethrown
                log.error("FileSyncJob.FAIL fatal: " + ex);
                throw ex;
            } catch (IOException | TransientException ex) {
                // includes ReadException
                // - prepare or put throwing this error
                // - will move to next url
                log.error("FileSyncJob.FAIL transient: " + ex);
            } catch (ResourceNotFoundException | ResourceAlreadyExistsException | PreconditionFailedException ex) {
                log.error("FileSyncJob.FAIL remove " + u + " reason: " + ex);
                urlIterator.remove();
            }
        }
        
        return null;
    }

}

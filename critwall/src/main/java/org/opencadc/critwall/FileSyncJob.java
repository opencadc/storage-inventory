/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2022.                            (c) 2022.
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
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.RangeNotSatisfiableException;
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
import java.security.AccessControlException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
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
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * Single file sync instance.
 * 
 * @author pdowler
 */
public class FileSyncJob implements Runnable {
    private static final Logger log = Logger.getLogger(FileSyncJob.class);

    private static final long[] RETRY_DELAY = new long[] { 6000L, 12000L };
    // adjustable by test code
    static long SEGMENT_SIZE_PREF = 2 * 1024L * 1024L * 1024L; // 2 GiB

    private int syncArtifactAttempts = 0; // total count of attempted download
    
    private final ArtifactDAO artifactDAO;
    private final UUID artifactID;
    private final URI locatorService;
    private final StorageAdapter storageAdapter;
    private final Subject subject;
    private final Subject anonSubject = AuthenticationUtil.getAnonSubject();
    
    private String auth;
    private long byteTransferTime;
    private String artifactLabel;
    private final List<Exception> fails = new ArrayList<>();
    
    /**
     * Construct a job to sync the specified artifact.
     * 
     * @param artifact artifact to sync
     * @param locatorServiceID locator service to use
     * @param storageAdapter back end storage
     * @param artifactDAO database persistence
     * @param subject caller with credentials for downloads
     */
    public FileSyncJob(Artifact artifact, URI locatorServiceID, StorageAdapter storageAdapter, ArtifactDAO artifactDAO, Subject subject) {
        InventoryUtil.assertNotNull(FileSyncJob.class, "artifact", artifact);
        InventoryUtil.assertNotNull(FileSyncJob.class, "locatorServiceID", locatorServiceID);
        InventoryUtil.assertNotNull(FileSyncJob.class, "storageAdapter", storageAdapter);
        InventoryUtil.assertNotNull(FileSyncJob.class, "artifactDAO", artifactDAO);

        this.artifactID = artifact.getID();
        this.locatorService = locatorServiceID;
        this.storageAdapter = storageAdapter;

        this.artifactDAO = artifactDAO;
        this.subject = subject;
        
        this.artifactLabel = "Artifact.id=" + artifactID + " Artifact.uri=" + artifact.getURI();
        this.byteTransferTime = 0;
        this.auth = "";
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
    // in cases where the artifact changed, it will be picked up again and sync'ed later
    // - note: we only have to worry about changes in Artifact.uri because it may be made mutable in future
    //         Artifact.contentChecksum and Artifact.contentLength are immutable      
    private void doSync() {
        
        log.info("FileSyncJob.START " + artifactLabel);
        long start = System.currentTimeMillis();
        boolean success = false;
        String msg = "";
        
        try {
            // get current artifact to sync
            final Artifact artifact = artifactDAO.get(artifactID);
            if (artifact == null) {
                success = false;
                msg = "reason=obsolete-artifact";
                return;
            }
            if  (artifact.storageLocation != null) {
                success = false;
                msg = "reason=artifact-already-synced";
                return;
            }
            
            this.artifactLabel = "Artifact.id=" + artifactID + " Artifact.uri=" + artifact.getURI();            

            List<Protocol> urlList;
            try {
                urlList = getDownloadURLs(this.locatorService, artifact.getURI());
                if (urlList.isEmpty()) {
                    success = false;
                    msg = "reason=no-transfer-urls";
                    return;
                }
            } catch (Exception ex) {
                log.debug("transfer negotiation failed: " + artifactLabel, ex);
                success = false;
                msg = "reason=transfer-negotiation-failed " + ex;
                return;
            }
            
            int retryCount = 0;
            try {
                while (!success && !urlList.isEmpty() && retryCount < RETRY_DELAY.length) {
                    Artifact curArtifact = artifactDAO.get(artifactID);
                    if (curArtifact == null) {
                        success = false;
                        msg = "reason=obsolete-artifact";
                        return;
                    }
                    if  (artifact.storageLocation != null) {
                        success = false;
                        msg = "reason=artifact-already-synced";
                        return;
                    }
                    if (!curArtifact.getURI().equals(artifact.getURI())) {
                        success = false;
                        msg = "reason=artifact-change-since-job-started";
                        return;
                    }
                    
                    // attempt to sync file
                    log.debug("FileSyncJob.SYNC " + artifactLabel + " urls=" + urlList.size() + " attempts=" + retryCount);
                    StorageMetadata storageMeta = syncArtifactTxn(curArtifact, urlList);
                    
                    // sync succeeded: update inventory
                    if (storageMeta != null) {
                        ObsoleteStorageLocationDAO locDAO = new ObsoleteStorageLocationDAO(artifactDAO);
                        TransactionManager txnMgr = artifactDAO.getTransactionManager();
                        try {
                            // this transaction is ~equivalent to minoc/PutAction
                            log.debug("starting transaction");
                            txnMgr.startTransaction();
                            log.debug("start txn: OK");
                            
                            curArtifact = artifactDAO.lock(artifact);
                            
                            ObsoleteStorageLocation prevOSL = locDAO.get(storageMeta.getStorageLocation());
                            if (prevOSL != null) {
                                // no longer obsolete
                                locDAO.delete(prevOSL.getID());
                            }

                            ObsoleteStorageLocation obsLoc = null;
                            if (curArtifact == null || !curArtifact.getURI().equals(artifact.getURI())) {
                                // newly written object is obsolete
                                success = false;
                                msg = "reason=change-since-job-started";
                                obsLoc = new ObsoleteStorageLocation(storageMeta.getStorageLocation());
                                locDAO.put(obsLoc);
                            } else {
                                // previous (~simultaneous) written object is obsolete
                                if (curArtifact.storageLocation != null) {
                                    if (!artifact.storageLocation.equals(curArtifact.storageLocation)) {
                                        obsLoc = new ObsoleteStorageLocation(curArtifact.storageLocation);
                                        locDAO.put(obsLoc);
                                    }
                                }
                                artifactDAO.setStorageLocation(curArtifact, storageMeta.getStorageLocation());
                                success = true;
                                msg += "bytes=" + storageMeta.getContentLength();
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
                    
                    if (!success && !urlList.isEmpty()) {
                        log.info("FileSyncJob.SLEEP dt=" + RETRY_DELAY[retryCount]);
                        Thread.sleep(RETRY_DELAY[retryCount++]);
                    }
                }
                if (!success && urlList.isEmpty()) {
                    Exception commonFail = null;
                    for (Exception e : fails) {
                        if (commonFail == null) {
                            commonFail = e;
                        }
                        if (!commonFail.getClass().equals(e.getClass())) {
                            commonFail = null;
                            break;
                        }
                    }
                    if (commonFail != null) {
                        msg = "reason=" + commonFail;
                    } else {
                        msg = "reason=no-remaining-urls";
                    }
                }
            } catch (ByteLimitExceededException | IllegalStateException | EntityNotFoundException ex) {
                log.debug("artifact sync aborted: " + artifactLabel, ex);
                msg = "reason=" + ex.getClass().getName() + " " + ex.getMessage();
            } catch (IllegalArgumentException | InterruptedException | StorageEngageException | WriteException ex) {
                log.debug("artifact sync error: " + artifactLabel, ex);
                msg = "reason=" + ex.getClass().getName() + " " + ex.getMessage();
            } catch (Exception ex) {
                log.debug("unexpected fail: " + artifactLabel, ex);
                msg = "reason=" + ex.getClass().getName() + " " + ex.getMessage();
            }
        } finally {
            long dt = System.currentTimeMillis() - start;
            long overheadTime = dt - byteTransferTime;
            StringBuilder sb = new StringBuilder();
            sb.append("FileSyncJob.END ").append(artifactLabel);
            sb.append(" success=").append(success);
            sb.append(" duration=").append(dt);
            sb.append(" attempts=").append(syncArtifactAttempts);
            sb.append(" auth=").append(auth);
            if (byteTransferTime > 0) {
                sb.append(" transfer=").append(byteTransferTime);
                sb.append(" overhead=").append(overheadTime);
            }
            sb.append(" ").append(msg);
            log.info(sb.toString());
        }
    }

    // Use transfer negotiation at resource URI to get list of download URLs for the artifact.
    private List<Protocol> getDownloadURLs(URI resource, URI artifact)
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

        Transfer transfer = new Transfer(artifact, Direction.pullFromVoSpace);
        transfer.version = VOS.VOSPACE_21;
        transfer.getProtocols().addAll(protocolList);

        TransferWriter writer = new TransferWriter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.write(transfer, out);
        FileContent content = new FileContent(out.toByteArray(), "text/xml");
        log.debug("transfer request to be posted: " + transfer);

        log.debug("artifact path: " + artifact.getPath());
        HttpPost post = new HttpPost(transferURL, content, true);
        post.setConnectionTimeout(6000); // ms
        post.setReadTimeout(60000);      // ms
        post.prepare();
        log.debug("post prepare done");

        TransferReader reader = new TransferReader();
        Transfer t = reader.read(post.getInputStream(), null);
        return t.getProtocols();
    }

    // no longer used but kept for reference: see syncArtifactTxn
    private StorageMetadata syncArtifact(Artifact a, List<Protocol> urls) throws Exception {
        StorageMetadata storageMeta = null;
        Iterator<Protocol> urlIterator = urls.iterator();

        while (urlIterator.hasNext()) {
            Protocol p = urlIterator.next();
            URL u = new URL(p.getEndpoint());
            final String logURL = getLoggableString(u, p.getSecurityMethod());
            boolean postPrepare = false;
            try {
                syncArtifactAttempts++;
                HttpGet get = new HttpGet(u, true);
                get.setConnectionTimeout(6000); // ms
                get.setReadTimeout(60000);      // ms
                if (p.getSecurityMethod() == null || p.getSecurityMethod().equals(Standards.getSecurityMethod(AuthMethod.ANON))) {
                    log.debug("download: " + u + " as " + anonSubject);
                    doPrepareAnon(get);
                } else {
                    log.debug("download: " + u + " as " + AuthenticationUtil.getCurrentSubject());
                    get.prepare();
                }
                
                postPrepare = true;
                
                verifyMetadata(a, get, null);

                NewArtifact na = new NewArtifact(a.getURI());
                na.contentChecksum = a.getContentChecksum();
                na.contentLength = a.getContentLength();

                storageMeta = this.storageAdapter.put(na, get.getInputStream(), null);
                log.debug("storage meta returned: " + storageMeta.getStorageLocation());
                return storageMeta;

            } catch (ByteLimitExceededException | StorageEngageException | WriteException ex) {
                // IOException will capture this if not explicitly caught and rethrown
                log.debug("FileSyncJob.FAIL", ex);
                log.error("FileSyncJob.FAIL " + artifactLabel + " reason=" + ex);
                throw ex;
            } catch (MalformedURLException | ResourceNotFoundException | ResourceAlreadyExistsException
                     | PreconditionFailedException | RangeNotSatisfiableException 
                     | AccessControlException | NotAuthenticatedException ex) {
                log.debug("FileSyncJob.ERROR remove=" + u, ex);
                log.warn("FileSyncJob.ERROR " + artifactLabel + " remove=" + logURL + " auth=" + auth + "] reason=" + ex);
                fails.add(ex);
                urlIterator.remove();
            } catch (IOException | TransientException ex) {
                // includes ReadException
                // - prepare or put throwing this error
                log.debug("FileSyncJob.ERROR keep=" + u, ex);
                log.warn("FileSyncJob.ERROR " + artifactLabel + " keep=" + logURL + " auth=" + auth + "] reason=" + ex);
                fails.add(ex);
            } catch (Exception ex) {
                if (!postPrepare) {
                    // remote server 5xx response: discard
                    log.debug("FileSyncJob.ERROR remove=" + u, ex);
                    log.warn("FileSyncJob.ERROR " + artifactLabel + " remove=" + logURL + " auth=" + auth + "] reason=" + ex);
                    urlIterator.remove();
                    fails.add(ex);
                } else {
                    // StorageAdapter.put internal fail: abort
                    log.debug("FileSyncJob.FAIL", ex);
                    log.warn("FileSyncJob.FAIL " + artifactLabel + " reason=" + ex);
                    throw ex;
                }
            }
        }
        
        return null;
    }
    
    static class PutSegment {
        long start;
        long end;
        long contentLength;
        
        public String getRangeHeaderVal() {
            return "bytes=" + start + "-" + end; 
        }

        @Override
        public String toString() {
            return PutSegment.class.getSimpleName() + "[" + start + "," + end + "," + contentLength + "]";
        }
    }
    
    static List<PutSegment> getSegmentPlan(Artifact a, PutTransaction pt) {
        List<FileSyncJob.PutSegment> segs = new ArrayList<>();
        
        long segmentSize = Math.min(SEGMENT_SIZE_PREF, a.getContentLength()); // client preference
        if (pt.getMinSegmentSize() != null) {
            segmentSize = Math.max(segmentSize, pt.getMinSegmentSize());
        }
        if (pt.getMaxSegmentSize() != null) {
            segmentSize = Math.min(segmentSize, pt.getMaxSegmentSize());
        }
        
        long numWholeSegments = a.getContentLength() / segmentSize;
        long lastSegment = a.getContentLength() - (segmentSize * numWholeSegments);
        long numSegments = (lastSegment > 0L ? numWholeSegments + 1 : numWholeSegments);

        for (int i = 0; i < numSegments; i++) {
            FileSyncJob.PutSegment s = new FileSyncJob.PutSegment();

            s.start = i * segmentSize;
            s.end = s.start + segmentSize - 1;
            s.contentLength = segmentSize;
            if (i + 1 == numSegments && lastSegment > 0L) {
                s.end = s.start + lastSegment - 1;
                s.contentLength = lastSegment;
            }
            segs.add(s);
        }
        return segs;
    }
    
    private StorageMetadata syncArtifactTxn(Artifact a, List<Protocol> urls) throws Exception {
        Iterator<Protocol> urlIterator = urls.iterator();
        while (urlIterator.hasNext()) {
            Protocol p = urlIterator.next();
            URL u = new URL(p.getEndpoint());
            final String logURL = getLoggableString(u, p.getSecurityMethod());
            if (p.getSecurityMethod() == null) {
                auth = "anon";
            } else {
                auth = p.getSecurityMethod().getFragment();
            }

            String txnID = null;
            boolean postPrepare = false;
            try {
                syncArtifactAttempts++;
                
                // figure out txn params
                PutTransaction pt = storageAdapter.startTransaction(a.getURI(), a.getContentLength());
                txnID = pt.getID();
                List<PutSegment> segs = FileSyncJob.getSegmentPlan(a, pt);
                if (segs.size() == 1) {
                    storageAdapter.abortTransaction(pt.getID());
                    txnID = null;
                    // proceed without txn
                }
                
                long transferTime = 0;
                for (PutSegment seg : segs) {
                    log.debug("get: " + seg);
                    postPrepare = false;
                    HttpGet get = new HttpGet(u, true);
                    get.setConnectionTimeout(6000); // ms
                    get.setReadTimeout(60000);      // ms
                    if (txnID != null) {
                        get.setRequestProperty("range", seg.getRangeHeaderVal());
                    }

                    if (p.getSecurityMethod() == null || p.getSecurityMethod().equals(Standards.getSecurityMethod(AuthMethod.ANON))) {
                        log.debug("download: " + u + " as " + anonSubject);
                        doPrepareAnon(get);
                    } else {
                        log.debug("download: " + u + " as " + AuthenticationUtil.getCurrentSubject());
                        get.prepare();
                    }
                    postPrepare = true;
                    if (txnID != null && get.getResponseCode() != 206) {
                        // TODO have to fall back to complete download somehow
                        throw new RuntimeException("OOPS: " + logURL + " auth=" + auth + "] does not support range requests");
                    }

                    // when there is only one segment, pt==null but the seg.contentLength is correct
                    verifyMetadata(a, get, seg);

                    NewArtifact na = new NewArtifact(a.getURI());
                    na.contentChecksum = a.getContentChecksum();
                    na.contentLength = a.getContentLength();
                    if (txnID != null) {
                        na.contentLength = seg.contentLength;
                    }
                    
                    // accumulate time spent on actual byte transfer
                    long startPut = System.currentTimeMillis();
                    StorageMetadata storageMeta = this.storageAdapter.put(na, get.getInputStream(), txnID);
                    transferTime = transferTime + System.currentTimeMillis() - startPut;
                    log.debug("put ok: " + storageMeta);
                    
                    if (txnID == null) {
                        byteTransferTime = transferTime;
                        return storageMeta;
                    }
                    // TODO: verify partial put, maybe revert and try chunk again?
                }
                
                PutTransaction status = storageAdapter.getTransactionStatus(txnID);
                verifyMetadata(a, status.storageMetadata);

                log.debug("committing " + txnID);
                StorageMetadata ret = storageAdapter.commitTransaction(txnID);
                txnID = null;
                byteTransferTime = transferTime;
                return ret;
            } catch (ByteLimitExceededException | StorageEngageException | WriteException ex) {
                // IOException will capture this if not explicitly caught and rethrown
                log.debug("FileSyncJob.FAIL", ex);
                log.error("FileSyncJob.FAIL " + artifactLabel + " reason=" + ex);
                throw ex;
            } catch (MalformedURLException | ResourceNotFoundException | ResourceAlreadyExistsException
                     | PreconditionFailedException | RangeNotSatisfiableException 
                     | AccessControlException | NotAuthenticatedException ex) {
                log.error("FileSyncJob.ERROR remove=" + u, ex);
                log.warn("FileSyncJob.ERROR " + artifactLabel + " remove=" + logURL + " auth=" + auth + "] reason=" + ex);
                fails.add(ex);
                urlIterator.remove();
            } catch (IOException | TransientException ex) {
                // includes ReadException
                // - prepare or put throwing this error
                log.debug("FileSyncJob.ERROR keep=" + u, ex);
                log.warn("FileSyncJob.ERROR " + artifactLabel + " keep=" + logURL + " auth=" + auth + "] reason=" + ex);
                fails.add(ex);
            } catch (Exception ex) {
                if (!postPrepare) {
                    // remote server 5xx response: discard
                    log.debug("FileSyncJob.ERROR remove=" + u, ex);
                    log.warn("FileSyncJob.ERROR " + artifactLabel + " remove=" + logURL + " auth=" + auth + "] reason=" + ex);
                    urlIterator.remove();
                    fails.add(ex);
                } else {
                    // StorageAdapter.put internal fail: abort
                    log.debug("FileSyncJob.FAIL", ex);
                    log.warn("FileSyncJob.FAIL " + artifactLabel + " reason=" + ex);
                    throw ex;
                }
            } finally {
                if (txnID != null) {
                    // not comitted at end of try { }
                    try {
                        storageAdapter.abortTransaction(txnID);
                    } catch (IllegalArgumentException ignore) {
                        // txn already aborted by StorageAdapter
                    }
                }
            }
        }
        
        return null;
    }
    
    private void doPrepareAnon(final HttpGet get) throws Exception {
        try {
            Subject.doAs(anonSubject, (PrivilegedExceptionAction<Void>) () -> {
                get.prepare();
                return null;
            });
        } catch (PrivilegedActionException pex) {
            throw pex.getException();
        }
    }
    
    private void verifyMetadata(Artifact a, HttpGet get, PutSegment seg) throws PreconditionFailedException {
        log.debug("verify artifact: " + a);
        log.debug("verify headers: " + get.getContentLength() + " " + get.getDigest());
        log.debug("verify seg: " + seg);
        URI hdrContentChecksum = get.getDigest();
        if (hdrContentChecksum != null
            && !hdrContentChecksum.equals(a.getContentChecksum())) {
            throw new PreconditionFailedException("contentChecksum artifact: " 
                + a.getContentChecksum() + " header: " + hdrContentChecksum);
        }

        long hdrContentLen = get.getContentLength();
        if (hdrContentLen != -1) {
            if (seg != null) {
                if (hdrContentLen != seg.contentLength) {
                    throw new PreconditionFailedException("contentLength segment: " 
                        + seg.contentLength + " header: " + hdrContentLen);
                }
            } else if (hdrContentLen != a.getContentLength()) {
                throw new PreconditionFailedException("contentLength artifact: " 
                    + a.getContentLength() + " header: " + hdrContentLen);
            }
        }
    }
    
    private void verifyMetadata(Artifact a, StorageMetadata sm) throws PreconditionFailedException {
        log.debug("verify artifact: " + a);
        log.debug("verify storageMetadata: " + sm);
        if (!sm.getContentChecksum().equals(a.getContentChecksum())) {
            throw new PreconditionFailedException("contentChecksum artifact: " 
                + a.getContentChecksum() + " storage: " + sm.getContentChecksum());
        }

        if (!sm.getContentLength().equals(a.getContentLength())) {
            throw new PreconditionFailedException("contentLength artifact: " 
                + a.getContentLength() + " storage: " + sm.getContentLength());
        }
    }
    
    private String getLoggableString(URL url, URI sm) {
        String surl = url.toExternalForm();
        String path = url.getPath();
        int i = surl.indexOf(path);
        String[] ss = path.split("/");
        
        StringBuilder sb = new StringBuilder();
        sb.append(surl.substring(0, i));
        
        // leading / means ss[0] is empty string
        sb.append("/");
        if (ss.length > 1) {
            sb.append(ss[1]).append("/");
        }
        if (ss.length > 2) {
            sb.append(ss[2]).append("/");
        }
        sb.append("...");
       
        return sb.toString();
    }
}

/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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

package org.opencadc.inventory.storage.fs;

import ca.nrc.cadc.io.MultiBufferIO;
import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.IncorrectContentChecksumException;
import ca.nrc.cadc.net.IncorrectContentLengthException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.MessageDigestAPI;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import static org.opencadc.inventory.storage.fs.LogicalFileSystemStorageAdapter.CHECKSUM_ATTRIBUTE_NAME;
import static org.opencadc.inventory.storage.fs.LogicalFileSystemStorageAdapter.MD5_CHECKSUM_SCHEME;
import static org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.XATTR_EXEC;
import org.opencadc.util.fs.XAttrCommandExecutor;

/**
 * base class with common code for file system StorageAdapter implementations.
 * 
 * @author pdowler
 */
abstract class AbstractStorageAdapter implements StorageAdapter {
    private static final Logger log = Logger.getLogger(AbstractStorageAdapter.class);

    public static final String DEFAULT_CHECKSUM_ALGORITHM = "MD5";
    
    static final String ARTIFACTID_ATTR = "artifactID";
    static final String CHECKSUM_ATTR = "contentChecksum";
    static final String EXP_LENGTH_ATTR = "contentLength";
    static final Long PT_MIN_BYTES = 1L;
    static final Long PT_MAX_BYTES = null;
    static final String CUR_DIGEST_ATTR = "curDigest";
    static final String PREV_DIGEST_ATTR = "prevDigest";
    static final String PREV_LENGTH_ATTR = "prevLength";
    
    private static final int CIRC_BUFFERS = 3;
    private static final int CIRC_BUFFERSIZE = 64 * 1024;
    static final String DELETED_PRESERVED = "deleted-preserved";
    
    protected Path txnPath;
    protected Path contentPath;
    
    protected final List<Namespace> recoverableNamespaces = new ArrayList<>();
    protected final List<Namespace> purgeNamespaces = new ArrayList<>();
    
    protected AbstractStorageAdapter() {
    }
    
    static void setFileAttribute(Path path, String attributeKey, String attributeValue) throws IOException {
        log.debug("setFileAttribute: " + path);
        
        if (XATTR_EXEC) {
            String namespace = "user";
            String key = namespace + "." + attributeKey;
            if (StringUtil.hasText(attributeValue)) {
                XAttrCommandExecutor.set(path, key, attributeValue);
            } else {
                XAttrCommandExecutor.remove(path, key);
            }
            return;
        }
        
        UserDefinedFileAttributeView udv = Files.getFileAttributeView(path,
            UserDefinedFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        String key = attributeKey;
        if (attributeValue != null) {
            attributeValue = attributeValue.trim();
            log.debug("attribute write: " + key + " = " + attributeValue);
            ByteBuffer buf = ByteBuffer.wrap(attributeValue.getBytes(Charset.forName("UTF-8")));
            udv.write(key, buf);
        } else {
            try {
                log.debug("attribute delete: " + attributeKey);
                udv.delete(attributeKey);
            } catch (FileSystemException ex) {
                log.debug("assume no such attr: " + ex);
            }
        }
    }
    
    static String getFileAttribute(Path path, String attributeKey) throws IOException {
        if (XATTR_EXEC) {
            String namespace = "user";
            String key = namespace + "." + attributeKey;
            return XAttrCommandExecutor.get(path, key);
        }

        try {
            UserDefinedFileAttributeView udv = Files.getFileAttributeView(path,
                UserDefinedFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);

            int sz = udv.size(attributeKey);
            ByteBuffer buf = ByteBuffer.allocate(2 * sz);
            udv.read(attributeKey, buf);
            return new String(buf.array(), Charset.forName("UTF-8")).trim();
        } catch (FileSystemException ex) {
            log.debug("assume no such attr: " + ex);
            return null;
        }
    }

    @Override
    public void setRecoverableNamespaces(List<Namespace> recoverable) {
        this.recoverableNamespaces.clear();
        this.recoverableNamespaces.addAll(recoverable);
    }

    @Override
    public List<Namespace> getRecoverableNamespaces() {
        return recoverableNamespaces;
    }

    @Override
    public void setPurgeNamespaces(List<Namespace> purge) {
        this.purgeNamespaces.clear();
        this.purgeNamespaces.addAll(purge);
    }

    @Override
    public List<Namespace> getPurgeNamespaces() {
        return purgeNamespaces;
    }

    @Override
    public Iterator<PutTransaction> transactionIterator() throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(OpaqueFileSystemStorageAdapter.class, "storageLocation", storageLocation);
        InventoryUtil.assertNotNull(OpaqueFileSystemStorageAdapter.class, "dest", dest);
        log.debug("get: " + storageLocation);

        Path path = storageLocationToPath(storageLocation);
        if (!Files.exists(path)) {
            throw new ResourceNotFoundException("not found: " + storageLocation);
        }
        try {
            String delAttr = getFileAttribute(path, DELETED_PRESERVED);
            if ("true".equals(delAttr)) {
                log.debug("skip " + DELETED_PRESERVED + ": " + storageLocation);
                throw new ResourceNotFoundException("not found: " + storageLocation);
            }
        } catch (IOException ex) {
            throw new StorageEngageException("failed to read attributes for stored file: " + storageLocation, ex);
        }
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("not a file: " + storageLocation);
        }
        InputStream source = null;
        try {
            source = Files.newInputStream(path, StandardOpenOption.READ);
        } catch (IOException ex) {
            throw new StorageEngageException("failed to create input stream for stored file: " + storageLocation, ex);
        }

        MultiBufferIO io = new MultiBufferIO(CIRC_BUFFERS, CIRC_BUFFERSIZE);
        try {
            io.copy(source, dest);
        } catch (InterruptedException ex) {
            log.debug("get interrupted", ex);
        }
    }

    @Override
    public void get(StorageLocation storageLocation, OutputStream dest, ByteRange byteRange)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(OpaqueFileSystemStorageAdapter.class, "storageLocation", storageLocation);
        InventoryUtil.assertNotNull(OpaqueFileSystemStorageAdapter.class, "dest", dest);
        InventoryUtil.assertNotNull(OpaqueFileSystemStorageAdapter.class, "byteRange", byteRange);
        log.debug("get: " + storageLocation + " " + byteRange);

        Path path = storageLocationToPath(storageLocation);
        if (!Files.exists(path)) {
            throw new ResourceNotFoundException("not found: " + storageLocation);
        }
        try {
            String delAttr = getFileAttribute(path, DELETED_PRESERVED);
            if ("true".equals(delAttr)) {
                log.debug("skip " + DELETED_PRESERVED + ": " + storageLocation);
                throw new ResourceNotFoundException("not found: " + storageLocation);
            }
        } catch (IOException ex) {
            throw new StorageEngageException("failed to read attributes for stored file: " + storageLocation, ex);
        }
        
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("not a file: " + storageLocation);
        }
        
        InputStream source = null;
        try {
            if (byteRange != null) {
                RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r");
                SortedSet<ByteRange> brs = new TreeSet<>();
                brs.add(byteRange);
                source = new PartialReadInputStream(raf, brs);
            } else {
                source = Files.newInputStream(path, StandardOpenOption.READ);
            }
        } catch (IOException ex) {
            throw new StorageEngageException("failed to create input stream for stored file: " + storageLocation, ex);
        }
        
        MultiBufferIO io = new MultiBufferIO(CIRC_BUFFERS, CIRC_BUFFERSIZE);
        try {
            io.copy(source, dest);
        } catch (InterruptedException ex) {
            log.debug("get interrupted", ex);
        }
    }

    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source, String transactionID)
        throws IncorrectContentChecksumException, IncorrectContentLengthException, 
            ReadException, WriteException,
            StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "artifact", newArtifact);
        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "source", source);

        Path txnTarget;
        Long expectedLength = null;
        MessageDigestAPI txnDigest = null;
        String checksumAlg = DEFAULT_CHECKSUM_ALGORITHM;
        if (newArtifact.contentChecksum != null) {
            checksumAlg = newArtifact.contentChecksum.getScheme(); // TODO: try sha1 in here
        }
        try {
            if (transactionID != null) {
                // validate
                UUID.fromString(transactionID);
                txnTarget = txnPath.resolve(transactionID);
                if (!Files.exists(txnTarget)) {
                    throw new IllegalArgumentException("unknown transaction: " + transactionID);
                }
                String dstate = getFileAttribute(txnTarget, CUR_DIGEST_ATTR);
                if (dstate != null) {
                    txnDigest = MessageDigestAPI.getDigest(dstate);
                }
                if (txnDigest == null) {
                    throw new RuntimeException("BUG: failed to restore digest in transaction " + transactionID);
                }
                String auri = getFileAttribute(txnTarget, ARTIFACTID_ATTR);
                if (auri == null) {
                    throw new RuntimeException("BUG: failed to restore uri attribute in transaction " + transactionID);
                }
                if (!newArtifact.getArtifactURI().toASCIIString().equals(auri)) {
                    throw new IllegalArgumentException("incorrect Artifact.uri in transaction: " + transactionID
                        + " expected: " + auri);
                }
                String alen = getFileAttribute(txnTarget, EXP_LENGTH_ATTR);
                if (alen != null) {
                    try {
                        expectedLength = Long.valueOf(alen);
                    } catch (NumberFormatException ex) {
                        throw new RuntimeException("BUG: failed to restore expected content length attribute in transaction " + transactionID);
                    }
                }
            } else {
                String tmp = UUID.randomUUID().toString();
                txnTarget = txnPath.resolve(tmp);
                txnDigest = MessageDigestAPI.getInstance(checksumAlg);
                if (Files.exists(txnTarget)) {
                    // unlikely: duplicate UUID in the txnpath directory?
                    throw new RuntimeException("BUG: txnTarget already exists: " + txnTarget);
                }
                expectedLength = newArtifact.contentLength;
            }
        } catch (IOException ex) {
            throw new StorageEngageException("failed to read file attributes", ex);
        } catch (InvalidPathException e) {
            throw new RuntimeException("BUG: invalid path: " + txnPath, e);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("failed to create MessageDigestAPI: " + checksumAlg, ex);
        }
        log.debug("transaction: " + txnTarget + " transactionID: " + transactionID);
        
        
        Throwable throwable = null;
        URI checksum = null;
        
        String prevDigestState = null;
        Long prevLength = 0L;
        
        try {
            prevDigestState = MessageDigestAPI.getEncodedState(txnDigest);
            if (transactionID != null) {
                prevLength = Files.size(txnTarget);
            }
            
            OpenOption opt = StandardOpenOption.CREATE_NEW;
            if (transactionID != null) {
                opt = StandardOpenOption.APPEND;
            }
            MessageDigestAPI md = txnDigest;
            org.opencadc.inventory.storage.DigestOutputStream out = new org.opencadc.inventory.storage.DigestOutputStream(Files.newOutputStream(txnTarget, StandardOpenOption.WRITE, opt), txnDigest);
            MultiBufferIO io = new MultiBufferIO();
            if (transactionID != null) {
                try {
                    log.debug("append starting at offset " + prevLength);
                    io.copy(source, out);
                    out.flush();
                    md = out.getMessageDigest();
                } catch (ReadException ex) {
                    // rollback to prevLength
                    RandomAccessFile raf = new RandomAccessFile(txnTarget.toFile(), "rws");
                    log.debug("rollback from " + raf.length() + " to " + prevLength + " after " + ex);
                    raf.setLength(prevLength);
                    md = MessageDigestAPI.getDigest(prevDigestState);
                    long len = Files.size(txnTarget);
                    log.debug("auto-revert transaction " + transactionID + " to length " + len + " after failed input: " + ex);
                    throw ex;
                } catch (WriteException ex) {
                    log.debug("auto-abort transaction " + transactionID + " after failed write to back end: ", ex);
                    abortTransaction(transactionID);
                    throw ex;
                }
            } else {
                io.copy(source, out);
                out.flush();
                md = out.getMessageDigest();
            }

            String curDigestState = MessageDigestAPI.getEncodedState(md);
            Long curLength = Files.size(txnTarget);
            
            String csVal = HexUtil.toHex(md.digest());
            checksum = URI.create(checksumAlg.toLowerCase() + ":" + csVal);
            log.debug("current checksum: " + checksum);
            log.debug("current file size: " + curLength);
            
            if (transactionID != null && (expectedLength == null || curLength < expectedLength)) {
                // incomplete: no further content checks
                log.debug("incomplete put in transaction: " + transactionID + " - not verifying checksum");
            } else {
                boolean checksumProvided = newArtifact.contentChecksum != null;
                if (checksumProvided) {
                    if (!newArtifact.contentChecksum.equals(checksum)) {
                        throw new IncorrectContentChecksumException(newArtifact.contentChecksum + " != " + checksum);
                    }
                }
                if (expectedLength != null && !expectedLength.equals(curLength)) {
                    if (checksumProvided) {
                        // likely bug in the client, throw a 400 instead
                        throw new IncorrectContentLengthException(expectedLength + " != " + curLength
                            + " but checksum was correct! client BUG?");
                    }
                    throw new IncorrectContentLengthException(expectedLength + " != " + curLength);
                }
            }

            // Set file attributes that must be recovered in iterator
            setFileAttribute(txnTarget, CHECKSUM_ATTR, checksum.toASCIIString());
            setFileAttribute(txnTarget, ARTIFACTID_ATTR, newArtifact.getArtifactURI().toASCIIString());
            
            StorageLocation storageLocation = createStorageLocation(newArtifact.getArtifactURI(), txnTarget);
            
            if (transactionID != null) {
                log.debug("transaction uncommitted: " + transactionID + " " + storageLocation);
                // transaction will continue
                setFileAttribute(txnTarget, CUR_DIGEST_ATTR, curDigestState);
                setFileAttribute(txnTarget, PREV_DIGEST_ATTR, prevDigestState);
                setFileAttribute(txnTarget, PREV_LENGTH_ATTR, prevLength.toString());
                if (curLength == 0L) {
                    return new StorageMetadata(storageLocation);
                }
                // current state
                return new StorageMetadata(storageLocation, newArtifact.getArtifactURI(),
                        checksum, curLength, new Date(Files.getLastModifiedTime(txnTarget).toMillis()));
            }

            // create this before committing the file so constraints applied
            StorageMetadata metadata = new StorageMetadata(storageLocation, newArtifact.getArtifactURI(),
                    checksum, curLength, new Date(Files.getLastModifiedTime(txnTarget).toMillis()));
            
            StorageMetadata ret = commit(metadata, txnTarget);
            txnTarget = null;
            return ret;
            
        } catch (ReadException | WriteException | IllegalArgumentException
            | IncorrectContentChecksumException | IncorrectContentLengthException e) {
            // pass through
            throw e;
        } catch (Throwable t) {
            throwable = t;
            log.error("put error", t);
            if (throwable instanceof IOException) {
                throw new StorageEngageException("put error", throwable);
            }
            // TODO: identify throwables that are transient
            throw new RuntimeException("Unexpected error", throwable);
        } finally {
            // txnTarget file still exists and not in a transaction: something went wrong
            if (txnTarget != null && transactionID == null) {
                try {
                    log.debug("Deleting transaction file.");
                    Files.delete(txnTarget);
                } catch (IOException e) {
                    log.error("Failed to delete transaction file", e);
                }
            }
        }
    }
 
@Override
    public void delete(StorageLocation storageLocation)
        throws ResourceNotFoundException, IOException, StorageEngageException, TransientException {
        delete(storageLocation, false);
    }
    
    @Override
    public void delete(StorageLocation storageLocation, boolean includeRecoverable)
        throws ResourceNotFoundException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "storageLocation", storageLocation);
        Path path = storageLocationToPath(storageLocation);
        if (!Files.exists(path)) {
            throw new ResourceNotFoundException("not found: " + storageLocation);
        }
        String uriAttr = null;
        boolean deletePreserved = false;
        try {
            deletePreserved = "true".equals(getFileAttribute(path, DELETED_PRESERVED));
            uriAttr = getFileAttribute(path, ARTIFACTID_ATTR);
        } catch (IOException ex) {
            throw new StorageEngageException("failed to read attributes for stored file: " + storageLocation, ex);
        }
        
        if (deletePreserved && !includeRecoverable) {
            log.debug("skip " + DELETED_PRESERVED + ": " + storageLocation);
            throw new ResourceNotFoundException("not found: " + storageLocation);
        }
        
        log.debug("delete: " + storageLocation + " aka " + uriAttr);
        if (uriAttr != null) {
            try {
                URI uri = new URI(uriAttr);
                log.debug("recoverable: " + recoverableNamespaces.size() + " purge: " + purgeNamespaces.size());
                Namespace purge = getFirstMatch(uri, purgeNamespaces);
                Namespace recoverable = getFirstMatch(uri, recoverableNamespaces);
               
                if (purge != null) {
                    log.debug("delete/purge: " + storageLocation 
                                + " aka " + uri + " matched " + purge.getNamespace());
                    // fall through to delete
                } else if (recoverable != null) {
                    log.debug("delete/recoverable: " + storageLocation 
                                + " aka " + uri + " matched " + recoverable.getNamespace());
                    // avoid poking fs timestamp unecessarily
                    if (!deletePreserved) {
                        try {
                            setFileAttribute(path, DELETED_PRESERVED, "true");
                        } catch (IOException ex) {
                            throw new StorageEngageException("failed to set attribute for stored file: " + storageLocation, ex);
                        }
                    }
                    return; // don't delete
                } // else: normal fall through to delete
            } catch (URISyntaxException ex) {
                if (!recoverableNamespaces.isEmpty()) {
                    log.error("found invalid " + ARTIFACTID_ATTR + "=" + uriAttr + " on " 
                            + storageLocation + " -- cannot make recoverable");
                }
            }
        } // else: no uriAttr aka incomplete put so delete
        
        log.debug("delete/actual: " + storageLocation + " aka " + uriAttr);
        try {
            Files.delete(path);
        } catch (IOException ex) {
            throw new StorageEngageException("failed to delete stored file: " + storageLocation, ex);
        }
    }
    
    private Namespace getFirstMatch(URI uri, List<Namespace> namespaces) {
        for (Namespace ns : namespaces) {
            log.debug("check: " + ns.getNamespace() + " vs " + uri);
            if (ns.matches(uri)) {
                return ns;
            }
        }
        return null;
    }

    @Override
    public PutTransaction startTransaction(URI artifactURI, Long contentLength) 
            throws StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "artifactURI", artifactURI);
        try {
            String transactionID = UUID.randomUUID().toString();
            Path txnFile = txnPath.resolve(transactionID);
            OutputStream  ostream = Files.newOutputStream(txnFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            ostream.close();
            
            // TODO: accept non-default checksum algorithm for txn?
            MessageDigestAPI md = MessageDigestAPI.getInstance(DEFAULT_CHECKSUM_ALGORITHM);
            String digestState = MessageDigestAPI.getEncodedState(md);
            String md5Val = HexUtil.toHex(md.digest());
            URI checksum = URI.create(DEFAULT_CHECKSUM_ALGORITHM + ":" + md5Val);
            setFileAttribute(txnFile, CUR_DIGEST_ATTR, digestState);
            setFileAttribute(txnFile, CHECKSUM_ATTR, checksum.toASCIIString());
            setFileAttribute(txnFile, ARTIFACTID_ATTR, artifactURI.toASCIIString());
            if (contentLength != null) {
                setFileAttribute(txnFile, EXP_LENGTH_ATTR, contentLength.toString());
            }
            log.debug("startTransaction: " + transactionID);
            return new PutTransaction(transactionID, PT_MIN_BYTES, PT_MAX_BYTES);
        } catch (IOException ex) {
            throw new StorageEngageException("failed to create transaction", ex);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("BUG", ex);
        }
    }
    
    @Override
    public PutTransaction revertTransaction(String transactionID) 
            throws IllegalArgumentException, StorageEngageException, TransientException, UnsupportedOperationException {
        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "transactionID", transactionID);
        try {
            // validate
            UUID.fromString(transactionID);
            Path txnFile = txnPath.resolve(transactionID);
            if (Files.exists(txnFile)) {
                String prevDigestState = getFileAttribute(txnFile, PREV_DIGEST_ATTR);
                String sprevLen = getFileAttribute(txnFile, PREV_LENGTH_ATTR);
                if (prevDigestState == null || sprevLen == null) {
                    throw new IllegalArgumentException("transaction not revertable: " + transactionID);
                }
                Long prevLength = Long.parseLong(sprevLen);
                
                // revert
                RandomAccessFile raf = new RandomAccessFile(txnFile.toFile(), "rws");
                log.debug("revertTransaction: " + transactionID + " from " + raf.length() + " to " + prevLength);
                raf.setLength(prevLength);
                String curDigestState = prevDigestState;
                long len = Files.size(txnFile);
                setFileAttribute(txnFile, CUR_DIGEST_ATTR, curDigestState);
                setFileAttribute(txnFile, PREV_DIGEST_ATTR, null);
                setFileAttribute(txnFile, PREV_LENGTH_ATTR, null);
                
                MessageDigestAPI md = MessageDigestAPI.getDigest(curDigestState);
                String md5Val = HexUtil.toHex(md.digest());
                URI checksum = URI.create(md.getAlgorithmName() + ":" + md5Val);
                setFileAttribute(txnFile, CHECKSUM_ATTR, checksum.toASCIIString());
                
                StorageMetadata ret = createStorageMetadata(txnPath, txnFile, false);
                // txnPath does not have bucket dirs
                //ret.getStorageLocation().storageBucket = InventoryUtil.computeBucket(ret.getStorageLocation().getStorageID(), bucketLength);
                PutTransaction pt = new PutTransaction(transactionID, PT_MIN_BYTES, PT_MAX_BYTES);
                pt.storageMetadata = ret;
                return pt;
            }
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("BUG: failed to restore digest", ex);
        } catch (InvalidPathException e) {
            throw new RuntimeException("BUG: invalid path: " + txnPath, e);
        } catch (IOException ex) {
            throw new StorageEngageException("failed to revert transaction", ex);
        }
        throw new IllegalArgumentException("unknown transaction: " + transactionID);
    }
    
    @Override
    public void abortTransaction(String transactionID) 
            throws IllegalArgumentException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "transactionID", transactionID);
        try {
            Path txnTarget = txnPath.resolve(transactionID);
            if (Files.exists(txnPath)) {
                Files.delete(txnTarget);
            } else {
                throw new IllegalArgumentException("unknown transaction: " + transactionID);
            }
        } catch (IOException ex) {
            throw new StorageEngageException("failed to create transaction", ex);
        }
    }
    
    @Override
    public PutTransaction getTransactionStatus(String transactionID)
        throws IllegalArgumentException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "transactionID", transactionID);
        try {
            // validate
            UUID.fromString(transactionID);
            Path txnFile = txnPath.resolve(transactionID);
            if (Files.exists(txnFile)) {
                StorageMetadata ret = createStorageMetadata(txnPath, txnFile, false);
                // txnPath does not have bucket dirs
                //ret.getStorageLocation().storageBucket = InventoryUtil.computeBucket(ret.getStorageLocation().getStorageID(), bucketLength);
                PutTransaction pt = new PutTransaction(transactionID, PT_MIN_BYTES, PT_MAX_BYTES);
                pt.storageMetadata = ret;
                return pt;
            }
        } catch (InvalidPathException e) {
            throw new RuntimeException("BUG: invalid path: " + txnPath, e);
        }
        throw new IllegalArgumentException("unknown transaction: " + transactionID);
    }
    
    @Override
    public StorageMetadata commitTransaction(String transactionID)
        throws IllegalArgumentException, StorageEngageException, TransientException {
        PutTransaction pt = getTransactionStatus(transactionID);
        Path txnTarget = txnPath.resolve(transactionID); // again
        
        try {
            setFileAttribute(txnTarget, CUR_DIGEST_ATTR, null);
            setFileAttribute(txnTarget, PREV_DIGEST_ATTR, null);
            setFileAttribute(txnTarget, PREV_LENGTH_ATTR, null);
            setFileAttribute(txnTarget, EXP_LENGTH_ATTR, null);
        } catch (IOException ex) {
            throw new StorageEngageException("failed to commit (clear transaction attributes)", ex);
        }
        return commit(pt.storageMetadata, txnTarget);
    }
    
    protected StorageMetadata commit(StorageMetadata sm, Path txnTarget) throws StorageEngageException {
        try {
            Path contentTarget = storageLocationToPath(sm.getStorageLocation());
            // make sure parent (bucket) directories exist
            Path parent = contentTarget.getParent();
            if (!Files.exists(parent)) {
                Files.createDirectories(parent);
            }

            if (Files.exists(contentTarget)) {
                // since filename is a UUID this is fatal
                throw new RuntimeException("BUG: UUID collision on commit: " + sm.getStorageLocation());
            }

            // atomic move into content directory
            final Path result = Files.move(txnTarget, contentTarget, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            log.debug("committed: " + result);
            
            // defensive check in case commit tweaked lastModified timestamp slightly:
            Date d2 = new Date(Files.getLastModifiedTime(contentTarget).toMillis());
            long delta = d2.getTime() - sm.getContentLastModified().getTime();
            if (delta != 0L) {
                log.debug("commit-induced delta lastModified: " + delta + " - recreate StorageMetadata from path");
                sm = createStorageMetadata(contentPath, contentTarget, false);
            }

            return sm;
        } catch (IOException ex) {
            throw new StorageEngageException("failed to commit (atomic move)", ex);
        }
    }
    
    protected abstract StorageLocation createStorageLocation(URI artifactURI, Path txnTarget);

    protected abstract Path storageLocationToPath(StorageLocation loc);
    
    protected abstract StorageMetadata createStorageMetadata(Path base, Path p, boolean includeRecoverable);
}

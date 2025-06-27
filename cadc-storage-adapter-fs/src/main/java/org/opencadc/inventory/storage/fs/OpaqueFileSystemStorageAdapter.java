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
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;

import ca.nrc.cadc.util.StringUtil;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserDefinedFileAttributeView;
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
import org.opencadc.inventory.storage.BucketType;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.DigestOutputStream;
import org.opencadc.inventory.storage.MessageDigestAPI;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.util.fs.XAttrCommandExecutor;

/**
 * An implementation of the StorageAdapter interface on a file system.
 * This implementation creates an opaque file system structure where
 * storageBucket(s) form a directory tree of hex characters and files are 
 * stored at the bottom level  with random (UUID) file names.
 * 
 * @author pdowler
 *
 */
public class OpaqueFileSystemStorageAdapter extends AbstractStorageAdapter implements StorageAdapter {
    private static final Logger log = Logger.getLogger(OpaqueFileSystemStorageAdapter.class);
    
    public static final String CONFIG_FILE = "cadc-storage-adapter-fs.properties";
    public static final String CONFIG_PROPERTY_ROOT = OpaqueFileSystemStorageAdapter.class.getPackage().getName() + ".baseDir";
    public static final String CONFIG_PROPERTY_BUCKET_LENGTH = OpaqueFileSystemStorageAdapter.class.getName() + ".bucketLength";
    // undocumented config option in case java user defined attrs don't work as expected
    public static final String CONFIG_XATTR_EXEC =  OpaqueFileSystemStorageAdapter.class.getName() + ".xattrAlwaysExec";
    public static final int MAX_BUCKET_LENGTH = 7;
            
    
    
    private static final String TXN_FOLDER = "transaction";
    private static final String CONTENT_FOLDER = "content";

    
    private static final int CIRC_BUFFERS = 3;
    private static final int CIRC_BUFFERSIZE = 64 * 1024;

    private static final String DELETED_PRESERVED = "deleted-preserved";
    
    static boolean XATTR_EXEC;
    
    private final int bucketLength;
    
    private final List<Namespace> recoverableNamespaces = new ArrayList<>();
    private final List<Namespace> purgeNamespaces = new ArrayList<>();

    public OpaqueFileSystemStorageAdapter() throws InvalidConfigException {
        PropertiesReader pr = new PropertiesReader(CONFIG_FILE);
        MultiValuedProperties props = pr.getAllProperties();
        
        String rootVal = null;
        
        // get the configured root directory
        rootVal = props.getFirstPropertyValue(CONFIG_PROPERTY_ROOT);
        InventoryUtil.assertNotNull(OpaqueFileSystemStorageAdapter.class, CONFIG_PROPERTY_ROOT, rootVal);
        log.debug("root: " + rootVal);
        if (rootVal == null) {
            throw new InvalidConfigException("failed to load " + CONFIG_PROPERTY_ROOT
                + " from " + CONFIG_FILE);
        }
        
        String length = props.getFirstPropertyValue(CONFIG_PROPERTY_BUCKET_LENGTH);
        InventoryUtil.assertNotNull(OpaqueFileSystemStorageAdapter.class, CONFIG_PROPERTY_BUCKET_LENGTH, length);
        int bucketLen;
        try {
            bucketLen = Integer.parseInt(length);
        } catch (NumberFormatException ex) {
            throw new InvalidConfigException("invalid integer value: " + CONFIG_PROPERTY_BUCKET_LENGTH + " = " + length);
        }
        if (bucketLen < 0 || bucketLen > MAX_BUCKET_LENGTH) {
            throw new InvalidConfigException(CONFIG_PROPERTY_BUCKET_LENGTH + " must be in [1," + MAX_BUCKET_LENGTH + "], found " + bucketLen);
        }
        this.bucketLength = bucketLen;
        
        String exec = props.getFirstPropertyValue(CONFIG_XATTR_EXEC);
        if (exec != null) {
            XATTR_EXEC = "true".equals(exec);
        }
        
        FileSystem fs = FileSystems.getDefault();
        Path root = fs.getPath(rootVal);
        this.contentPath = root.resolve(CONTENT_FOLDER);
        this.txnPath = root.resolve(TXN_FOLDER);

        init(root);
    }

    // for test code
    public OpaqueFileSystemStorageAdapter(File rootDirectory, int bucketLen) 
            throws InvalidConfigException {

        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "rootDirectory", rootDirectory);

        if (bucketLen < 0 || bucketLen > MAX_BUCKET_LENGTH) {
            throw new InvalidConfigException(CONFIG_PROPERTY_BUCKET_LENGTH + " must be in [1," + MAX_BUCKET_LENGTH + "], found " + bucketLen);
        }
        this.bucketLength = bucketLen;
        
        FileSystem fs = FileSystems.getDefault();
        Path root = fs.getPath(rootDirectory.getAbsolutePath());
        this.contentPath = root.resolve(CONTENT_FOLDER);
        this.txnPath = root.resolve(TXN_FOLDER);
        
        init(root);
    }
    
    private void init(Path root) throws InvalidConfigException {
        try {
            if (!Files.isDirectory(root)) {
                throw new InvalidConfigException("root must be a directory");
            }
            if (!Files.isReadable(root) || (!Files.isWritable(root))) {
                throw new InvalidConfigException("read-write permission required on root");
            }
            
            if (!Files.exists(contentPath)) {
                Files.createDirectories(contentPath);
                log.debug("created content dir: " + contentPath);
            }
            if (!Files.isReadable(contentPath) || (!Files.isWritable(contentPath))) {
                throw new InvalidConfigException("read-write permission required on content directory");
            }
            log.debug("validated content dir: " + contentPath);

            if (!Files.exists(txnPath)) {
                Files.createDirectories(txnPath);
                log.debug("created txn dir: " + txnPath);
            }
            if (!Files.isReadable(txnPath) || (!Files.isWritable(txnPath))) {
                throw new InvalidConfigException("read-write permission required on transaction directory");
            }
            log.debug("validated txn dir: " + txnPath);

        } catch (InvalidPathException e) {
            throw new InvalidConfigException("Invalid root directory: " + root, e);
        } catch (IOException io) {
            throw new InvalidConfigException(("Could not create content or transaction directory"), io);
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
    public BucketType getBucketType() {
        return BucketType.HEX;
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
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "artifact", newArtifact);
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "source", source);

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
            DigestOutputStream out = new DigestOutputStream(Files.newOutputStream(txnTarget, StandardOpenOption.WRITE, opt), txnDigest);
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
            
            StorageLocation storageLocation = pathToStorageLocation(txnTarget);
            
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
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "storageLocation", storageLocation);
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
    public void recover(StorageLocation storageLocation, Date contentLastModified) 
            throws ResourceNotFoundException, IOException, InterruptedException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "storageLocation", storageLocation);
        Path path = storageLocationToPath(storageLocation);
        if (!Files.exists(path)) {
            throw new ResourceNotFoundException("not found: " + storageLocation);
        }
        try {
            String delAttr = getFileAttribute(path, DELETED_PRESERVED);
            if ("true".equals(delAttr)) {
                String uriAttr = getFileAttribute(path, ARTIFACTID_ATTR);
                log.debug("recover: " + storageLocation + " aka " + uriAttr);
                setFileAttribute(path, DELETED_PRESERVED, null);
                if (contentLastModified != null) {
                    FileTime t = FileTime.fromMillis(contentLastModified.getTime());
                    Files.setLastModifiedTime(path, t);
                }
            }
        } catch (IOException ex) {
            throw new StorageEngageException("failed to read attributes for stored file: " + storageLocation, ex);
        }
        // silently do nothing??
    }
    
    
    @Override
    public Iterator<StorageMetadata> iterator()
        throws StorageEngageException, TransientException {
        
        return iterator(null, false);
    }
    
    @Override
    public Iterator<StorageMetadata> iterator(String storageBucketPrefix)
        throws StorageEngageException, TransientException {
        return iterator(storageBucketPrefix, false);
    }

    @Override
    public Iterator<StorageMetadata> iterator(String storageBucketPrefix, boolean includeRecoverable) throws StorageEngageException, TransientException {
        return new OpaqueIterator(contentPath, storageBucketPrefix, includeRecoverable);
    }

    // create from tmpfile in the txnPath to re-use UUID
    StorageLocation pathToStorageLocation(Path tmpfile) {
        // re-use the UUID from the tmpfile
        String sid = tmpfile.getFileName().toString();
        URI storageID = URI.create("uuid:" + sid);
        String storageBucket = InventoryUtil.computeBucket(storageID, bucketLength);
        StorageLocation loc = new StorageLocation(storageID);
        loc.storageBucket = storageBucket;
        log.debug("created: " + loc);
        return loc;
    }
    
    // generate destination path with bucket under contentPath
    @Override
    protected Path storageLocationToPath(StorageLocation storageLocation) {
        StringBuilder path = new StringBuilder();
        String bucket = storageLocation.storageBucket;
        log.debug("bucket: " + bucket);
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "storageLocation.bucket", bucket);
        for (char c : bucket.toCharArray()) {
            path.append(c).append(File.separator);
        }
        path.append(storageLocation.getStorageID().getSchemeSpecificPart());
        log.debug("Resolving path in content : " + path.toString());
        Path ret = contentPath.resolve(path.toString());
        return ret;
    }

    /*
    // TODO: these methods would be used for a readable directory structure impl
    // split scheme+path components into storageBucket, filename into storageID
    StorageLocation createReadableStorageLocation(URI artifactURI) {
        StringBuilder path = new StringBuilder();
        path.append(artifactURI.getScheme()).append(File.separator);
        String ssp = artifactURI.getSchemeSpecificPart();
        int i = ssp.lastIndexOf("/");
        path.append(ssp.substring(0, i));
        String storageBucket = path.toString();
        URI storageID = URI.create("name:" + ssp.substring(i));
        StorageLocation loc = new StorageLocation(storageID);
        loc.storageBucket = storageBucket;
        log.debug("created: " + loc);
        return loc;
    }
    
    Path createReadableStorageLocationPath(StorageLocation storageLocation) {
        StringBuilder path = new StringBuilder();
        path.append(storageLocation.storageBucket).append(File.separator);
        path.append(storageLocation.getStorageID().getSchemeSpecificPart());
        log.debug("Resolving path in content : " + path.toString());
        Path ret = contentPath.resolve(path.toString());
        return ret;
    }
    */
    
    protected StorageMetadata createStorageMetadata(Path base, Path p, boolean includeRecoverable) {
        return createStorageMetadataImpl(base, p, includeRecoverable);
    }

    // static impl more easily re-used by OpaqueIterator 
    static StorageMetadata createStorageMetadataImpl(Path base, Path p, boolean includeRecoverable) {
        Path rel = base.relativize(p);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rel.getNameCount() - 1; i++) {
            sb.append(rel.getName(i));
        }
        String storageBucket = sb.toString();
        URI storageID = URI.create("uuid:" + rel.getFileName());
        
        // take care: if base is txnPath, then storageBucket is empty string
        
        log.debug("createStorageMetadata: " + storageBucket + "," + storageID);
        try {
            StorageLocation sloc = new StorageLocation(storageID);
            sloc.storageBucket = storageBucket;
            try {
                String csAttr = getFileAttribute(p, OpaqueFileSystemStorageAdapter.CHECKSUM_ATTR);
                String aidAttr = getFileAttribute(p, OpaqueFileSystemStorageAdapter.ARTIFACTID_ATTR);
                if (csAttr == null || aidAttr == null) {
                    // this could happen if some were to copy files or the whole filesystem to
                    // a new storage area and not preserve xattrs (eg rsync without -X/--xattrs)
                    
                    // for now, just warn and skip so the operator has a chance to correct
                    log.warn("SKIP invalid: " + sloc + " : missing required attribute(s)"
                        + " " + OpaqueFileSystemStorageAdapter.CHECKSUM_ATTR + "=" + csAttr
                        + " " + OpaqueFileSystemStorageAdapter.ARTIFACTID_ATTR + "=" + aidAttr);
                    return null;
                }
                URI contentChecksum = new URI(csAttr);
                long contentLength = Files.size(p);
                URI artifactURI = new URI(aidAttr);
                
                String delAttr = getFileAttribute(p, DELETED_PRESERVED);
                if ("true".equals(delAttr) && !includeRecoverable) {
                    log.debug("skip " + DELETED_PRESERVED + ": " + sloc + " aka " + artifactURI);
                    return null;
                }
                StorageMetadata ret = new StorageMetadata(sloc, artifactURI, contentChecksum, contentLength, 
                        new Date(Files.getLastModifiedTime(p).toMillis()));
                ret.deleteRecoverable = "true".equals(delAttr);
                log.debug("createStorageMetadata: " + ret + " " + ret.deleteRecoverable);
                return ret;
            } catch (FileSystemException | IllegalArgumentException | URISyntaxException ex) {
                log.debug("invalid stored object", ex);
                return new StorageMetadata(sloc); // missing attrs: invalid stored object
            }
        } catch (IOException ex) {
            throw new RuntimeException("failed to recreate StorageMetadata: " + storageBucket + "," + storageID, ex);
        }
    }
}

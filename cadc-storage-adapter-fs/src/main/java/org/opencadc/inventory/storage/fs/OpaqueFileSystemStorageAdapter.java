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

package org.opencadc.inventory.storage.fs;

import ca.nrc.cadc.io.MultiBufferIO;
import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.IncorrectContentChecksumException;
import ca.nrc.cadc.net.IncorrectContentLengthException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
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
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.DigestOutputStream;
import org.opencadc.inventory.storage.InvalidConfigException;
import org.opencadc.inventory.storage.MessageDigestAPI;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * An implementation of the StorageAdapter interface on a file system.
 * This implementation creates an opaque file system structure where
 * storageBucket(s) form a directory tree of hex characters and files are 
 * stored at the bottom level  with random (UUID) file names.
 * 
 * @author pdowler
 *
 */
public class OpaqueFileSystemStorageAdapter implements StorageAdapter {
    private static final Logger log = Logger.getLogger(OpaqueFileSystemStorageAdapter.class);
    
    public static final String CONFIG_FILE = "cadc-storage-adapter-fs.properties";
    public static final String CONFIG_PROPERTY_ROOT = OpaqueFileSystemStorageAdapter.class.getPackage().getName() + ".baseDir";
    public static final String CONFIG_PROPERTY_BUCKET_LENGTH = OpaqueFileSystemStorageAdapter.class.getName() + ".bucketLength";

    public static final int MAX_BUCKET_LENGTH = 7;
            
    static final String ARTIFACTID_ATTR = "artifactID";
    static final String CHECKSUM_ATTR = "contentChecksum";
    static final String EXP_LENGTH_ATTR = "contentLength";
    
    private static final Long PT_MIN_BYTES = 1L;
    private static final Long PT_MAX_BYTES = null;
    
    private static final String TXN_FOLDER = "transaction";
    private static final String CONTENT_FOLDER = "content";

    private static final String DEFAULT_CHECKSUM_ALGORITHM = "MD5";
    private static final int CIRC_BUFFERS = 3;
    private static final int CIRC_BUFFERSIZE = 64 * 1024;

    private static final String CUR_DIGEST_ATTR = "curDigest";
    private static final String PREV_DIGEST_ATTR = "prevDigest";
    private static final String PREV_LENGTH_ATTR = "prevLength";
    
    final Path txnPath;
    final Path contentPath;
    private final int bucketLength;

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
        
        FileSystem fs = FileSystems.getDefault();
        Path root = fs.getPath(rootVal);
        this.contentPath = root.resolve(CONTENT_FOLDER);
        this.txnPath = root.resolve(TXN_FOLDER);

        init(root);
    }

    public OpaqueFileSystemStorageAdapter(File rootDirectory, int bucketLen) throws InvalidConfigException {

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
    public void get(StorageLocation storageLocation, OutputStream dest)
        throws ResourceNotFoundException, ReadException, WriteException, WriteException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(OpaqueFileSystemStorageAdapter.class, "storageLocation", storageLocation);
        InventoryUtil.assertNotNull(OpaqueFileSystemStorageAdapter.class, "dest", dest);
        log.debug("get: " + storageLocation);

        Path path = storageLocationToPath(storageLocation);
        if (!Files.exists(path)) {
            throw new ResourceNotFoundException("not found: " + storageLocation);
        }
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("not a file: " + storageLocation);
        }
        InputStream source = null;
        try {
            source = Files.newInputStream(path, StandardOpenOption.READ);
        } catch (IOException e) {
            throw new StorageEngageException("failed to create input stream for stored file: " + storageLocation, e);
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
        } catch (IOException e) {
            throw new StorageEngageException("failed to create input stream for stored file: " + storageLocation, e);
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
            } else {
                String tmp = UUID.randomUUID().toString();
                txnTarget = txnPath.resolve(tmp);
                txnDigest = MessageDigestAPI.getInstance(checksumAlg);
                if (Files.exists(txnTarget)) {
                    // unlikely: duplicate UUID in the txnpath directory?
                    throw new RuntimeException("BUG: txnTarget already exists: " + txnTarget);
                }
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
            
            if (transactionID != null && (newArtifact.contentLength == null || curLength < newArtifact.contentLength)) {
                // incomplete: no further content checks
                log.debug("incomplete put in transaction: " + transactionID + " - not verifying checksum");
            } else {
                boolean checksumProvided = newArtifact.contentChecksum != null;
                if (checksumProvided) {
                    if (!newArtifact.contentChecksum.equals(checksum)) {
                        throw new IncorrectContentChecksumException(newArtifact.contentChecksum + " != " + checksum);
                    }
                }
                if (newArtifact.contentLength != null && !newArtifact.contentLength.equals(curLength)) {
                    if (checksumProvided) {
                        // likely bug in the client, throw a 400 instead
                        throw new IncorrectContentLengthException(newArtifact.contentLength + " != " + curLength
                            + " but checksum was correct! client BUG?");
                    }
                    throw new IncorrectContentLengthException(newArtifact.contentLength + " != " + curLength);
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
                return new StorageMetadata(storageLocation, checksum, curLength,
                        new Date(Files.getLastModifiedTime(txnTarget).toMillis()));
            }

            // create this before committing the file so constraints applied
            StorageMetadata metadata = new StorageMetadata(storageLocation, checksum, curLength,
                    new Date(Files.getLastModifiedTime(txnTarget).toMillis()));
            metadata.artifactURI = newArtifact.getArtifactURI();
            
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
    public PutTransaction startTransaction(URI artifactURI, Long contentLength) 
            throws StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "artifactURI", artifactURI);
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
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "transactionID", transactionID);
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
                
                StorageMetadata ret = createStorageMetadata(txnPath, txnFile);
                // txnPath does not have bucket dirs
                ret.getStorageLocation().storageBucket = InventoryUtil.computeBucket(ret.getStorageLocation().getStorageID(), bucketLength);
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
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "transactionID", transactionID);
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
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "transactionID", transactionID);
        try {
            // validate
            UUID.fromString(transactionID);
            Path txnFile = txnPath.resolve(transactionID);
            if (Files.exists(txnFile)) {
                StorageMetadata ret = createStorageMetadata(txnPath, txnFile);
                // txnPath does not have bucket dirs
                ret.getStorageLocation().storageBucket = InventoryUtil.computeBucket(ret.getStorageLocation().getStorageID(), bucketLength);
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
    
    private StorageMetadata commit(StorageMetadata sm, Path txnTarget) throws StorageEngageException {
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

            // atomic copy into content directory
            final Path result = Files.move(txnTarget, contentTarget, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            log.debug("committed: " + result);
            
            // defensive check in case commit tweaked lastModified timestamp slightly:
            Date d2 = new Date(Files.getLastModifiedTime(contentTarget).toMillis());
            long delta = d2.getTime() - sm.getContentLastModified().getTime();
            if (delta != 0L) {
                log.debug("commit-induced delta lastModified: " + delta + " - recreate StorageMetadata from path");
                sm = createStorageMetadata(contentPath, contentTarget);
            }

            return sm;
        } catch (IOException ex) {
            throw new StorageEngageException("failed to commit (atomic move)", ex);
        }
    }
    
    @Override
    public void delete(StorageLocation storageLocation)
        throws ResourceNotFoundException, IOException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "storageLocation", storageLocation);
        Path path = storageLocationToPath(storageLocation);
        Files.delete(path);
    }
    
    @Override
    public Iterator<StorageMetadata> iterator()
        throws StorageEngageException, TransientException {
        
        return new OpaqueIterator(contentPath, null);
    }
    
    @Override
    public Iterator<StorageMetadata> iterator(String storageBucket)
        throws StorageEngageException, TransientException {
        return new OpaqueIterator(contentPath, storageBucket);
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
    Path storageLocationToPath(StorageLocation storageLocation) {
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
    
    // also used by OpaqueIterator 
    static StorageMetadata createStorageMetadata(Path base, Path p) {
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
                URI contentChecksum = new URI(csAttr);
                long contentLength = Files.size(p);
                StorageMetadata ret = new StorageMetadata(sloc, contentChecksum, contentLength, 
                        new Date(Files.getLastModifiedTime(p).toMillis()));
                ret.artifactURI = new URI(aidAttr);
                return ret;
            } catch (FileSystemException | IllegalArgumentException | URISyntaxException ex) {
                return new StorageMetadata(sloc); // missing attrs: invalid stored object
            }
        } catch (IOException ex) {
            throw new RuntimeException("failed to recreate StorageMetadata: " + storageBucket + "," + storageID, ex);
        }
    }
    
    public static void setFileAttribute(Path path, String attributeKey, String attributeValue) throws IOException {
        log.debug("setFileAttribute: " + path);
        UserDefinedFileAttributeView udv = Files.getFileAttributeView(path,
            UserDefinedFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (attributeValue != null) {
            attributeValue = attributeValue.trim();
            log.debug("attribute: " + attributeKey + " = " + attributeValue);
            ByteBuffer buf = ByteBuffer.wrap(attributeValue.getBytes(Charset.forName("UTF-8")));
            udv.write(attributeKey, buf);
        } else {
            try {
                log.debug("attribute: " + attributeKey + " (delete)");
                udv.delete(attributeKey);
            } catch (FileSystemException ex) {
                log.debug("assume no such attr: " + ex);
            }
        }
    }
    
    static String getFileAttribute(Path path, String attributeName) throws IOException {
        try {
            UserDefinedFileAttributeView udv = Files.getFileAttributeView(path,
                UserDefinedFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);

            int sz = udv.size(attributeName);
            ByteBuffer buf = ByteBuffer.allocate(2 * sz);
            udv.read(attributeName, buf);
            return new String(buf.array(), Charset.forName("UTF-8")).trim();
        } catch (FileSystemException ex) {
            log.debug("assume no such attr: " + ex);
            return null;
        }
    }
}

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
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.policy.InventoryIsAlwaysRight;
import org.opencadc.inventory.storage.policy.StorageValidationPolicy;

/**
 * An implementation of the storage adapter interface on a file system.
 * 
 * @author majorb
 */
public class FileSystemStorageAdapter implements StorageAdapter {
    
    private static final Logger log = Logger.getLogger(FileSystemStorageAdapter.class);
    
    public static final String CONFIG_FILE = "cadc-storage-adapter-fs.properties";
    public static final String CONFIG_PROPERTY_ROOT = "root";
    public static final String CONFIG_PROPERTY_BUCKETMODE = "bucketMode";
    public static final String CONFIG_PROPERTY_BUCKETDEPTH = "bucketLength";
    private static final String TXN_FOLDER = "transaction";
    static final String CHECKSUM_ATTRIBUTE_NAME = "contentChecksum";
    static final String CONTENT_FOLDER = "content";

    static final String MD5_CHECKSUM_SCHEME = "md5";
    
    private final FileSystem fs;
    final Path txnPath;
    final Path contentPath;
    
    /**
     * Construct a FileSystemStorageAdapter with the config stored in the
     * well-known properties file with well-known properties.
     */
    public FileSystemStorageAdapter() {
        PropertiesReader pr = new PropertiesReader(CONFIG_FILE);
        MultiValuedProperties mvp = pr.getAllProperties();
        String rootVal = null;

        // get the configured root directory
        rootVal = mvp.getFirstPropertyValue(CONFIG_PROPERTY_ROOT);
        log.debug("root: " + rootVal);
        if (rootVal == null) {
            throw new IllegalStateException("failed to load " + CONFIG_PROPERTY_ROOT
                + " from " + CONFIG_FILE);
        }
        
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "rootDirectory", rootVal);
        this.fs = FileSystems.getDefault();

        Path root = this.fs.getPath(rootVal);
        this.contentPath = root.resolve(CONTENT_FOLDER);
        this.txnPath = root.resolve(TXN_FOLDER);

        init(root);
    }

    public FileSystemStorageAdapter(File rootDirectory) {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "rootDirectory", rootDirectory);
        this.fs = FileSystems.getDefault();

        Path root = fs.getPath(rootDirectory.getAbsolutePath());
        this.contentPath = root.resolve(CONTENT_FOLDER);
        this.txnPath = root.resolve(TXN_FOLDER);

        init(root);
    }

    private void init(Path root) {
        try {
            if (!Files.isDirectory(root)) {
                throw new IllegalArgumentException("root must be a directory");
            }
            if (!Files.isReadable(root) || (!Files.isWritable(root))) {
                throw new IllegalArgumentException("read-write permission required on root");
            }

            // Ensure  root/CONTENT_FOLDER and TXN_FOLDER exist and have correct permissions
            // Set Path elements for transaction and content directories
            if (!Files.exists(contentPath)) {
                Files.createDirectories(contentPath);
                log.debug("created content dir: " + contentPath);
            }
            if (!Files.isReadable(contentPath) || (!Files.isWritable(contentPath))) {
                throw new IllegalArgumentException("read-write permission required on content directory");
            }
            log.debug("validated content dir: " + contentPath);

            if (!Files.exists(txnPath)) {
                Files.createDirectories(txnPath);
                log.debug("created txn dir: " + txnPath);
            }
            if (!Files.isReadable(txnPath) || (!Files.isWritable(txnPath))) {
                throw new IllegalArgumentException("read-write permission required on transaction directory");
            }
            log.debug("validated txn dir: " + txnPath);

        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Invalid root directory: " + root, e);
        } catch (IOException io) {
            throw new IllegalArgumentException(("Could not create content or transaction directory"), io);
        }
    }

    @Override
    public StorageValidationPolicy getValidationPolicy() {
        return new InventoryIsAlwaysRight();
    }
    
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "storageLocation", storageLocation);
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "dest", dest);
        log.debug("get: " + storageLocation);

        Path path = createStorageLocationPath(storageLocation);
        if (!Files.exists(path)) {
            throw new ResourceNotFoundException("not found: " + storageLocation.getStorageID());
        }
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("not found: " + storageLocation.getStorageID());
        }
        InputStream source = null;
        try {
            source = Files.newInputStream(path, StandardOpenOption.READ);
        } catch (IOException e) {
            throw new StorageEngageException("failed to create input stream to file system", e);
        }

        MultiBufferIO io = new MultiBufferIO();
        try {
            io.copy(source, dest);
        } catch (InterruptedException ex) {
            log.debug("get interrupted", ex);
        }
    }

    @Override
    public void get(StorageLocation storageLocation, OutputStream dest, ByteRange byteRange) 
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "storageLocation", storageLocation);
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "dest", dest);
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "byteRange", byteRange);
        log.debug("get: " + storageLocation + " " + byteRange);

        Path path = createStorageLocationPath(storageLocation);
        if (!Files.exists(path)) {
            throw new ResourceNotFoundException("not found: " + storageLocation.getStorageID());
        }
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("not found: " + storageLocation.getStorageID());
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
        
        MultiBufferIO io = new MultiBufferIO();
        try {
            io.copy(source, dest);
        } catch (InterruptedException ex) {
            log.debug("get interrupted", ex);
        }
    }
    
    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source, String transactionID)
        throws IncorrectContentChecksumException, IncorrectContentLengthException, ReadException, WriteException,
            StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "artifact", newArtifact);
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "source", source);

        if (transactionID != null) {
            throw new UnsupportedOperationException("put with transaction");
        }
        
        Path txnTarget = null;
        Path contentTarget = null;
        StorageLocation storageLocation = null;
        URI artifactURI = newArtifact.getArtifactURI();
        log.debug("put: artifactURI: " + artifactURI.toString());
        
        try {
            // Make storage location using artifactURI
            storageLocation = this.createStorageLocation(artifactURI);

            // add UUID to txnPath to make it unique
            txnTarget = txnPath.resolve(UUID.randomUUID().toString());
            log.debug("resolved txnTarget file: " + txnTarget + " based on " + txnPath);

            if (Files.exists(txnTarget)) {
                // This is an error as the name in the transaction directory should be unique
                log.debug("file/directory exists");
                throw new IllegalArgumentException(txnPath + " already exists.");
            }
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Illegal path: " + txnPath, e);
        }

        Throwable throwable = null;
        URI checksum = null;
        Long length = null;
        
        try {
            OutputStream out = Files.newOutputStream(txnTarget, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            MessageDigest digest = MessageDigest.getInstance("MD5");
            DigestOutputStream digestOut = new DigestOutputStream(out, digest);
            MultiBufferIO io = new MultiBufferIO();
            io.copy(source, digestOut);
            digestOut.flush();

            byte[] md5sum = digest.digest();
            String md5Val = HexUtil.toHex(md5sum);
            checksum = URI.create(MD5_CHECKSUM_SCHEME + ":" + md5Val);
            log.debug("calculated md5sum: " + checksum);
            length = Files.size(txnTarget);
            log.debug("calculated file size: " + length);
            
            boolean checksumProvided = newArtifact.contentChecksum != null && newArtifact.contentChecksum.getScheme().equals(MD5_CHECKSUM_SCHEME);
            // checksum comparison
            if (checksumProvided) {
                String expectedMD5 = newArtifact.contentChecksum.getSchemeSpecificPart();
                String actualMD5 = checksum.getSchemeSpecificPart();
                if (!expectedMD5.equals(actualMD5)) {
                    throw new IncorrectContentChecksumException(
                        "expected md5 checksum [" + expectedMD5 + "] "
                        + "but calculated [" + actualMD5 + "]");
                }
            } else {
                log.debug("Uncomparable or no contentChecksum provided.");
            }
            
            // content length comparison
            if (newArtifact.contentLength != null) {
                Long expectedLength = newArtifact.contentLength;
                if (!expectedLength.equals(length)) {
                    if (checksumProvided) {
                        // likely bug in the client, throw a 400 instead
                        throw new IllegalArgumentException("correct md5 checksum ["
                            + newArtifact.contentChecksum + "] but incorrect length ["
                            + expectedLength + "]");
                    }
                    throw new IncorrectContentLengthException(
                        "expected contentLength [" + expectedLength + "] "
                        + "but calculated [" + length + "]");
                }
            } else {
                log.debug("No contentLength provided.");
            }

            // TODO: move such methods to utility class
            OpaqueFileSystemStorageAdapter.setFileAttribute(txnTarget, CHECKSUM_ATTRIBUTE_NAME, checksum.toString());

            try {
                contentTarget = this.createStorageLocationPath(storageLocation);

                if (Files.exists(contentTarget)) {
                    // is an overwrite
                    log.debug("file/directory exists");
                    if (!Files.isRegularFile(contentTarget)) {
                        throw new IllegalArgumentException(contentTarget + " is not a file.");
                    }
                } else if (!Files.exists(contentTarget.getParent())) {
                    // is a new file
                    Files.createDirectories(contentTarget.getParent());
                }

            } catch (InvalidPathException e) {
                throw new IllegalArgumentException("Illegal path: " + contentTarget, e);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create content file: " + contentTarget, e);
            }

            // create this before committing the file so constraints applied
            StorageMetadata test = new StorageMetadata(storageLocation, artifactURI, checksum, length, new Date());
                
            // to atomic copy into content directory
            Path result = Files.move(txnTarget, contentTarget, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            log.debug("moved file to : " + contentTarget);
            txnTarget = null;

            StorageMetadata metadata = new StorageMetadata(storageLocation, artifactURI, 
                    checksum, length, new Date(Files.getLastModifiedTime(result).toMillis()));
            return metadata;
            
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
            throw new IllegalStateException("Unexpected error", throwable);
        } finally {
            // if the txnPath file still exists, then something went wrong.
            // Attempt to clear up the transaction file.
            // Otherwise put succeeded.
            if (txnTarget != null) {
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
        throws ResourceNotFoundException, IOException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "storageLocation", storageLocation);
        Path path = createStorageLocationPath(storageLocation);
        if (!Files.exists(path)) {
            throw new ResourceNotFoundException("not found: " + storageLocation);
        }
        Files.delete(path);
    }

    @Override
    public PutTransaction startTransaction(URI uri, Long contentLength) 
        throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PutTransaction revertTransaction(String transactionID) 
        throws IllegalArgumentException, StorageEngageException, TransientException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public StorageMetadata commitTransaction(String string) 
        throws IllegalArgumentException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abortTransaction(String string) 
        throws IllegalArgumentException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PutTransaction getTransactionStatus(String string) 
        throws IllegalArgumentException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Iterator<StorageMetadata> iterator()
        throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException("sorted iteration not supported");
    }
    
    @Override
    public Iterator<StorageMetadata> iterator(String storageBucket)
        throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException("sorted iteration not supported");
    }

    @Override
    public Iterator<StorageMetadata> iterator(String storageBucketPrefix, boolean includeRecoverable) throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<PutTransaction> transactionIterator() throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }
    
    private StorageLocation createStorageLocation(URI artifactURI) {
        URI storageID = artifactURI;
        String storageBucket = null;
        
        String ssp = artifactURI.getSchemeSpecificPart();
        String sspPath = ssp.substring(0, ssp.lastIndexOf("/"));
        storageBucket = artifactURI.getScheme() + ":" + sspPath;
           
        log.debug("new storage location " + storageID + " with bucket: " + storageBucket);
        StorageLocation loc = new StorageLocation(storageID);
        loc.storageBucket = storageBucket;
        return loc;
    }
    
    private Path createStorageLocationPath(StorageLocation storageLocation) {
        URI storageID = storageLocation.getStorageID();
        StringBuilder path = new StringBuilder();

        path.append(storageID.toString());
        
        log.debug("Resolving path in content : " + path.toString());
        Path ret = contentPath.resolve(path.toString());
        return ret;
    }

}

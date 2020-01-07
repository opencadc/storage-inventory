/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2019.                            (c) 2019.
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

import ca.nrc.cadc.net.IncorrectContentChecksumException;
import ca.nrc.cadc.net.IncorrectContentLengthException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.PropertiesReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.ReadException;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.ThreadedIO;
import org.opencadc.inventory.storage.WriteException;

/**
 * An implementation of the storage adapter interface on a file system.
 * This adapter can work in two bucket modes, specified in the BucketMode
 * enumeration:  URI_BUCKET_BASED and URI_BASED.
 * In URI_BUCKET_BASED mode, files are organized by their artifact uriBucket and
 * filenames and paths have no relation to the artifact URI.  The contents
 * of the file system will not be recognizable without the inventory database
 * to provide the mapping.  Subsets of the bucket can be used to change the
 * scope of the tree seen in unsortedIterator.  Artifacts are decoupled from the
 * files in this mode so external artifact URIs may change without consequence.
 * In URI_BASED mode, files are organized by their artifact URI (path and filename).
 * The file system resembles the path and file hierarchy of the artifact URIs it holds.
 * In this mode, the storage location bucket is the path of the scheme-specific-part
 * of the artifact URI.  Subsets (that match a directory) of the storage buckets can
 * be used when calling unsortedIterator.  The items in the iterator in
 * this mode will contain the corresponding artifactURI.  It is not expected
 * that external artifact URIs are changed when this adapter is used.  If they do
 * they will become inconsistent with the items reported by this iterator.
 * In both modes, a null bucket parameter to unsortedIterator will result in the
 * iteration of all files in the file system root.
 * 
 * @author majorb
 *
 */
public class FileSystemStorageAdapter implements StorageAdapter {
    
    private static final Logger log = Logger.getLogger(FileSystemStorageAdapter.class);
    
    public static final String CONFIG_FILE = "cadc-storage-adapter-fs.properties";
    public static final String CONFIG_PROPERTY_ROOT = "root";
    public static final String CONFIG_PROPERTY_BUCKETMODE = "bucketMode";
    public static final String CONFIG_PROPERTY_BUCKETDEPTH = "bucketLength";
    
    static final String MD5_CHECKSUM_SCHEME = "md5";
    static final int MAX_BUCKET_LENGTH = 5;
    static final int DEFAULT_BUCKET_LENGTH = 2;
    
    private FileSystem fs;
    private Path root;
    private BucketMode bucketMode;
    private int bucketLength = DEFAULT_BUCKET_LENGTH;
    
    public static enum BucketMode {
        URI,       // use the URI of the artifact for bucketing
        // This mode is functional except that the bucket sizes exceed
        // the database column length when used with minoc.  Work on this
        // mode is suspended until a use case arises.
        URIBUCKET; // use calculated 5 character uriBucket of the artifact URI for bucketing
        // This mode has been tested with minoc
    }
    
    /**
     * Construct a FileSystemStorageAdapter with the config stored in the
     * well-known properties file with well-known properties.
     */
    public FileSystemStorageAdapter() {
        PropertiesReader pr = new PropertiesReader(CONFIG_FILE);
        String rootVal = null;
        BucketMode bucketMode = null;
        
        // get the configured root directory
        rootVal = pr.getFirstPropertyValue(CONFIG_PROPERTY_ROOT);
        log.debug("root: " + rootVal);
        if (rootVal == null) {
            throw new IllegalStateException("failed to load " + CONFIG_PROPERTY_ROOT
                + " from " + CONFIG_FILE);
        }
        
        // get the configured bucket mode
        try {
            String mode = pr.getFirstPropertyValue(CONFIG_PROPERTY_BUCKETMODE);
            log.debug("bucketMode: " + mode);
            bucketMode = BucketMode.valueOf(mode);
        } catch (Throwable t) {
            throw new IllegalStateException("failed to load " + CONFIG_PROPERTY_BUCKETMODE
                + " from " + CONFIG_FILE + ": " + t.getMessage(), t);
        }
        
        // in uriBucket mode get the bucket depth
        if (bucketMode.equals(BucketMode.URIBUCKET)) {
            try {
                String length = pr.getFirstPropertyValue(CONFIG_PROPERTY_BUCKETDEPTH);
                log.debug("bucketDepth: " + length);
                if (length != null) {
                    bucketLength = Integer.parseInt(length);
                    if (bucketLength < 0 || bucketLength > MAX_BUCKET_LENGTH) {
                        throw new IllegalStateException("Bucket length of " + bucketLength
                            + " not in allowed range of 0-" + MAX_BUCKET_LENGTH);
                    }
                }
            } catch (Throwable t) {
                throw new IllegalStateException("failed to load " + CONFIG_PROPERTY_BUCKETMODE
                    + " from " + CONFIG_FILE + ": " + t.getMessage(), t);
            }
        }
        init(rootVal, bucketMode);
    }
    
    /**
     * Construct a FileSystemStorageAdapter with the config specified
     * in the arguments.
     * 
     * @param rootDirectory The root directory of the local file system.
     * @param bucketMode The mode in which to organize files
     */
    public FileSystemStorageAdapter(String rootDirectory, BucketMode bucketMode) {
        init(rootDirectory, bucketMode);
    }
    
    private void init(String rootDirectory, BucketMode bucketMode) {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "rootDirectory", rootDirectory);
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "bucketMode", bucketMode);
        this.fs = FileSystems.getDefault();
        try {
            root = fs.getPath(rootDirectory);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Invalid rootdirectory: " + rootDirectory, e);
        }
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("root must be a directory");
        }
        if (!Files.isReadable(root) || (!Files.isWritable(root))) {
            throw new IllegalArgumentException("read-write permission required on root");
        }
        this.bucketMode = bucketMode;
    }
    
    /**
     * Get from storage the artifact identified by storageLocation.
     * 
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest The destination stream.
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If the client failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public void get(StorageLocation storageLocation, OutputStream dest)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "storageLocation", storageLocation);
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "dest", dest);
        log.debug("get storageID: " + storageLocation.getStorageID());
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
        ThreadedIO io = new ThreadedIO();
        io.ioLoop(dest, source);
    }
    
    /**
     * Get from storage the artifact identified by storageLocation.
     * 
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest The destination stream.
     * @param cutouts Cutouts to be applied to the artifact
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If the client failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public void get(StorageLocation storageLocation, OutputStream dest, Set<String> cutouts)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException("cutouts not supported");
    }
    
    /**
     * Write an artifact to storage.
     * The value of storageBucket in the returned StorageMetadata and StorageLocation can be used to
     * retrieve batches of artifacts in some of the iterator signatures defined in this interface.
     * Batches of artifacts can be listed by bucket in two of the iterator methods in this interface.
     * If storageBucket is null then the caller will not be able perform bucket-based batch
     * validation through the iterator methods.
     * 
     * @param newArtifact The holds information about the incoming artifact.  If the contentChecksum
     *     and contentLength are set, they will be used to validate the bytes received.
     * @param source The stream from which to read.
     * @return The storage metadata.
     * 
     * @throws IncorrectContentChecksumException If the calculated checksum does not the expected checksum.
     * @throws IncorrectContentLengthException If the calculated length does not the expected length.
     * @throws ReadException If the client failed to stream.
     * @throws WriteException If the storage system failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred.
     */
    public StorageMetadata put(NewArtifact newArtifact, InputStream source)
        throws IncorrectContentChecksumException, IncorrectContentLengthException, ReadException, WriteException,
            StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "artifact", newArtifact);
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "source", source);
        
        Path path = null;
        Path existing = null;
        StorageLocation storageLocation = null;
        URI artifactURI = newArtifact.getArtifactURI();
        
        try {
            
            storageLocation = this.createStorageLocation(artifactURI);
            path = this.createStorageLocationPath(storageLocation);
            
            if (Files.exists(path)) {
                log.debug("file/directory exists");
                if (!Files.isRegularFile(path)) {
                    throw new IllegalArgumentException(path + " is not a file.");
                }
                // hold onto the existing file in case of error
                Path target = path.getParent().resolve(UUID.randomUUID().toString());
                existing = Files.move(path, target, StandardCopyOption.ATOMIC_MOVE);
                log.debug("moved file to be replaced to: " + target);
            } else if (!Files.exists(path.getParent())) {
                Files.createDirectories(path.getParent());
            }
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Illegal path: " + path, e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create file: " + path, e);
        }

        Throwable throwable = null;
        URI checksum = null;
        Long length = null;
        
        try {
            OutputStream out = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            MessageDigest digest = MessageDigest.getInstance("MD5");
            DigestOutputStream digestOut = new DigestOutputStream(out, digest);
            ThreadedIO threadedIO = new ThreadedIO();
            threadedIO.ioLoop(digestOut, source);
            digestOut.flush();
            byte[] md5sum = digest.digest();
            String md5Val = HexUtil.toHex(md5sum);
            checksum = URI.create(MD5_CHECKSUM_SCHEME + ":" + md5Val);
            length = Files.size(path);
            log.debug("calculated md5sum: " + checksum);
            log.debug("calculated file size: " + length);
            
            IncorrectContentChecksumException checksumException = null;
            boolean checksumProvided = newArtifact.contentChecksum != null && newArtifact.contentChecksum.getScheme().equals(MD5_CHECKSUM_SCHEME);
            // checksum comparison
            if (checksumProvided) {
                String expectedMD5 = newArtifact.contentChecksum.getSchemeSpecificPart();
                String actualMD5 = checksum.getSchemeSpecificPart();
                if (!expectedMD5.equals(actualMD5)) {
                    checksumException = new IncorrectContentChecksumException(
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
                    if (checksumException == null && checksumProvided) {
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
            if (checksumException != null) {
                throw checksumException;
            }
            
            StorageMetadata metadata = new StorageMetadata(storageLocation, checksum, length);
            metadata.artifactURI = artifactURI;
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
            if (existing != null) {
                if (throwable != null) {
                    try {
                        log.debug("Restoring original file.");
                        Files.move(existing, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                    } catch (IOException e) {
                        log.error("Failed to restore original file", e);
                    }
                } else {
                    try {
                        log.debug("Deleting original file.");
                        Files.delete(existing);
                    } catch (IOException e) {
                        log.error("Failed to delete original file", e);
                    }
                }
            }
        }
    }
        
    /**
     * Delete from storage the artifact identified by storageLocation.
     * @param storageLocation Identifies the artifact to delete.
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException If an unrecoverable error occurred.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public void delete(StorageLocation storageLocation)
        throws ResourceNotFoundException, IOException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "storageLocation", storageLocation);
        Path path = createStorageLocationPath(storageLocation);
        Files.delete(path);
    }
    
    /**
     * Iterator of items ordered by their storageIDs.
     * @return An iterator over an ordered list of items in storage.
     * 
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If the client failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> iterator()
        throws ReadException, WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException("sorted iteration not supported");
    }
    
    /**
     * Iterator of items ordered by their storageIDs in the given bucket.
     * @param storageBucket Only iterate over items in this bucket.
     * @return An iterator over an ordered list of items in this storage bucket.
     * 
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If the client failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> iterator(String storageBucket)
        throws ReadException, WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException("sorted iteration not supported");
    }
    
    /**
     * An unordered iterator of items in the given bucket.
     * @param storageBucket Only iterate over items in this bucket.
     * @return An iterator over an ordered list of items in this storage bucket.
     * 
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If the client failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> unsortedIterator(String storageBucket)
        throws ReadException, WriteException, StorageEngageException, TransientException {
        StringBuilder path = new StringBuilder();
        int bucketDepth = 0;
        String fixedParentDir = null;
        switch (bucketMode) {
            case URI:
                if (storageBucket != null && storageBucket.length() > 0) {
                    try {
                        URI test = new URI(storageBucket + "/file");
                        InventoryUtil.validateArtifactURI(FileSystemStorageAdapter.class, test);
                    } catch (URISyntaxException | IllegalArgumentException e) {
                        throw new IllegalArgumentException("bucket must be in the form 'scheme:path'");
                    }
                    path.append(storageBucket);
                }
                if (path.length() > 0) {
                    fixedParentDir = path.toString();
                }
                break;
            case URIBUCKET:
                if (storageBucket != null) {
                    if (storageBucket.length() > bucketLength) {
                        throw new IllegalArgumentException("bucket must be a maximum of " + bucketLength + " characters");
                    }
                    for (char c : storageBucket.toCharArray()) {
                        path.append(c).append(File.separator);
                    }
                    bucketDepth = bucketLength - storageBucket.length();
                } else {
                    bucketDepth = bucketLength;
                }
                break;
            default:
                throw new IllegalStateException("unsupported bucket mode");
        }
        try {
            log.debug("resolving path: " + path.toString());
            Path bucketPath = root.resolve(path.toString());
            log.debug("bucketPath: " + bucketPath);
            log.debug("exists: " + Files.exists(bucketPath));
            log.debug("isDir: " + Files.isDirectory(bucketPath));
            if (!Files.exists(bucketPath) || !Files.isDirectory(bucketPath)) {
                throw new IllegalArgumentException("Invalid bucket: " + storageBucket);
            }
            Iterator<StorageMetadata> iterator = null;
            try {
                iterator = new FileSystemIterator(bucketPath, bucketDepth, fixedParentDir);
            } catch (IOException e) {
                throw new StorageEngageException("failed to obtain iterator", e);
            }
            return iterator;
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Invalid bucket: " + storageBucket);
        }   
    }
    
    private StorageLocation createStorageLocation(URI artifactURI) {
        URI storageID = artifactURI;
        String storageBucket = null;
        switch (bucketMode) {
            case URI:
                String ssp = artifactURI.getSchemeSpecificPart();
                String sspPath = ssp.substring(0, ssp.lastIndexOf("/"));
                storageBucket = artifactURI.getScheme() + ":" + sspPath;
                break;
            case URIBUCKET:
                storageBucket = InventoryUtil.computeBucket(artifactURI, bucketLength);
                break;
            default:
                throw new IllegalStateException("unsupported bucket mode");
        }
        log.debug("new storage location " + storageID + " with bucket: " + storageBucket);
        StorageLocation loc = new StorageLocation(storageID);
        loc.storageBucket = storageBucket;
        return loc;
    }
    
    private Path createStorageLocationPath(StorageLocation storageLocation) {
        URI storageID = storageLocation.getStorageID();
        StringBuilder path = new StringBuilder();
        switch (bucketMode) {
            case URI:
                path.append(storageID.toString());
                break;
            case URIBUCKET:
                String bucket = storageLocation.storageBucket;
                log.debug("bucket: " + bucket);
                InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "storageLocation.bucket", bucket);
                for (char c : bucket.toCharArray()) {
                    path.append(c).append(File.separator);
                }
                path.append(storageLocation.getStorageID().toString());
                break;
            default:
                throw new IllegalStateException("unsupported bucket mode");
        }
        
        log.debug("Resolving path: " + path.toString());
        Path ret = root.resolve(path.toString());
        return ret;
    }
    
}

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

import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.HexUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * An implementation of the storage adapter interface on a file system.
 * This adapter can work in two bucket modes, specified in the BucketMode
 * enumeration:  URI_BUCKET_BASED and URI_BASED.
 * In URI_BUCKET_BASED mode, files are organized by their artifact uriBucket and
 * filenames and paths have no relation to the artifact URI.  The contents
 * of the file system will not be recognizable without the inventory database
 * to provide the mapping.  Subsets of the bucket can be used to change the
 * scope of the tree seen in unsortedIterator.  The items in the iterator in
 * this mode will not contain corresponding artifactURIs.  Artifacts are
 * decoupled from the files in this mode so external artifact URIs may change
 * without consequence.
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
    static final String STORAGE_URI_SCHEME = "fs";
    static final String MD5_CHECKSUM_SCHEME = "md5";
    static final int BUCKET_LENGTH = 5;
    
    private FileSystem fs;
    private Path root;
    private BucketMode bucketMode;
    
    public static enum BucketMode {
        URI_BASED,        // use the URI of the artifact for bucketing
        URI_BUCKET_BASED  // use uriBucket of the artifact for bucketing
    }
    
    /**
     * Construct a FileSystemStorageAdapter.
     * 
     * @param rootDirectory The root directory of the local file system.
     * @param bucketMode The mode in which to organize files
     */
    public FileSystemStorageAdapter(String rootDirectory, BucketMode bucketMode) {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "rootDirectory", rootDirectory);
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "bucketMode", bucketMode);
        this.fs = FileSystems.getDefault();
        try {
            root = fs.getPath(rootDirectory);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Invalid rootdirectory: " + rootDirectory, e);
        }
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("rootDirectory must be a directory");
        }
        if (!Files.isReadable(root) || (!Files.isWritable(root))) {
            throw new IllegalArgumentException("read-write permission required on rootDirectory");
        }
        this.bucketMode = bucketMode;
    }

    /**
     * Get from storage the artifact identified by storageLocation.
     * 
     * @param storageLocation The artifact location identifier.
     * @param wrapper An input stream wrapper to receive the bytes.
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public void get(StorageLocation storageLocation, InputStreamWrapper wrapper) throws ResourceNotFoundException, IOException, TransientException {
        log.debug("get storageID: " + storageLocation.getStorageID());
        Path path = createStorageLocationPath(storageLocation);
        InputStream in = Files.newInputStream(path, StandardOpenOption.READ);
        wrapper.read(in);
    }
    
    /**
     * Get from storage the artifact identified by storageLocation.
     * 
     * @param storageLocation The artifact location identifier.
     * @param wrapper An input stream wrapper to receive the bytes.
     * @param cutouts Cutouts to be applied to the artifact
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public void get(StorageLocation storageLocation, InputStreamWrapper wrapper, Set<String> cutouts)
        throws ResourceNotFoundException, IOException, TransientException {
        throw new UnsupportedOperationException("cutouts not supported");
    }
    
    /**
     * Write an artifact to storage.
     * 
     * @param newArtifact The artifact URI and metadata.
     * @param wrapper The wrapper for data of the artifact.
     * @return The storage metadata.
     * 
     * @throws StreamCorruptedException If the calculated checksum does not the expected checksum.
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred.
     */
    public StorageMetadata put(NewArtifact newArtifact, OutputStreamWrapper wrapper) throws StreamCorruptedException, IOException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "artifact", newArtifact);
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "wrapper", wrapper);
        
        Path path = null;
        Path existing = null;
        URI artifactURI = newArtifact.getArtifactURI();
        String uriBucket = InventoryUtil.computeBucket(artifactURI, BUCKET_LENGTH);
        
        try {
            
            path = createArtifactPath(artifactURI, uriBucket);
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
            wrapper.write(digestOut);
            digestOut.flush();
            byte[] md5sum = digest.digest();
            String md5Val = HexUtil.toHex(md5sum);
            checksum = URI.create(MD5_CHECKSUM_SCHEME + ":" + md5Val);
            length = Files.size(path);
            log.debug("calculated md5sum: " + checksum);
            log.debug("calculated file size: " + length);
        } catch (Throwable t) {
            throwable = t;
            log.error("error writing file", t);
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
            
            if (throwable != null) {
                if (throwable instanceof IOException) {
                    throw (IOException) throwable;
                }
                // TODO: identify throwables that are transient
                throw new IllegalStateException("Unexpected error", throwable);
            }
        }
        
        // checksum comparison
        if (newArtifact.contentChecksum != null && newArtifact.contentChecksum.getScheme().equals(MD5_CHECKSUM_SCHEME)) {
            String expectedMD5 = newArtifact.contentChecksum.getSchemeSpecificPart();
            String actualMD5 = checksum.getSchemeSpecificPart();
            if (!expectedMD5.equals(actualMD5)) {
                throw new StreamCorruptedException(
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
                throw new StreamCorruptedException(
                    "expected contentLength [" + expectedLength + "] "
                    + "but calculated [" + length + "]");
            }
        } else {
            log.debug("No contentLength provided.");
        }
        
        URI storageID = null;
        String storageBucket = null;
        switch (bucketMode) {
            case URI_BASED:
                StringBuilder storageIDString = new StringBuilder(STORAGE_URI_SCHEME);
                storageIDString.append(":");
                storageIDString.append(artifactURI.getScheme());
                storageIDString.append(":").append(File.separator);
                storageIDString.append(artifactURI.getSchemeSpecificPart());
                storageID = URI.create(storageIDString.toString());
                String ssp = artifactURI.getSchemeSpecificPart();
                String sspPath = ssp.substring(0, ssp.lastIndexOf("/"));
                storageBucket = artifactURI.getScheme() + ":" + sspPath;
                break;
            case URI_BUCKET_BASED:
                storageID = URI.create(STORAGE_URI_SCHEME + ":" + path.getFileName().toString());
                storageBucket = uriBucket;
                break;
            default:
                throw new IllegalStateException("unsupported bucket mode");
        }
        log.debug("new storage location " + storageID + " with bucket: " + storageBucket);
        StorageLocation loc = new StorageLocation(storageID);
        loc.storageBucket = storageBucket;
        StorageMetadata metadata = new StorageMetadata(loc, checksum, length);
        metadata.artifactURI = artifactURI;
        return metadata;
    }
        
    /**
     * Delete from storage the artifact identified by storageID.
     * @param storageLocation Identifies the artifact to delete.
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public void delete(StorageLocation storageLocation) throws ResourceNotFoundException, IOException, TransientException {
        InventoryUtil.assertNotNull(FileSystemStorageAdapter.class, "storageLocation", storageLocation);
        Path path = createStorageLocationPath(storageLocation);
        Files.delete(path);
    }
    
    /**
     * Iterator of items.  This implementation does not order the files
     * by storageID as per the requirement of the interface.
     * 
     * @return An iterator over an ordered list of items in storage.
     * 
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> iterator() throws IOException, TransientException {
        throw new UnsupportedOperationException("sorted iteration not supported");
    }
    
    /**
     * Iterator of itmes ordered by their storageIDs.
     * @param bucket Only iterate over items in this bucket.
     * @return An iterator over an ordered list of items in this storage bucket.
     * 
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> iterator(String bucket) throws IOException, TransientException {
        throw new UnsupportedOperationException("sorted iteration not supported");
    }
    
    /**
     * Iterator of itmes ordered by their storageIDs.
     * @param bucket Only iterate over items in this bucket.  A null or empty bucket means
     *     iterator over everything.
     * @return An iterator over an ordered list of items in this storage bucket.
     * 
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> unsortedIterator(final String bucket) throws IOException, TransientException {
        StringBuilder path = new StringBuilder();
        int bucketDepth = 0;
        String fixedParentDir = null;
        switch (bucketMode) {
            case URI_BASED:
                if (bucket != null && bucket.length() > 0) {
                    try {
                        URI test = new URI(bucket + "/file");
                        InventoryUtil.validateArtifactURI(FileSystemStorageAdapter.class, test);
                    } catch (URISyntaxException | IllegalArgumentException e) {
                        throw new IllegalArgumentException("bucket must be in the form 'scheme:path'");
                    }
                    int colonIndex = bucket.indexOf(":");
                    path.append(bucket.substring(0, colonIndex + 1));
                    path.append(File.separator);
                    path.append(bucket.substring(colonIndex + 1));
                }
                if (path.length() > 0) {
                    fixedParentDir = path.toString();
                }
                break;
            case URI_BUCKET_BASED:
                if (bucket != null) {
                    if (bucket.length() > BUCKET_LENGTH) {
                        throw new IllegalArgumentException("bucket must be a maximum of " + BUCKET_LENGTH + " characters");
                    }
                    for (char c : bucket.toCharArray()) {
                        path.append(c).append(File.separator);
                    }
                    bucketDepth = BUCKET_LENGTH - bucket.length();
                } else {
                    bucketDepth = BUCKET_LENGTH;
                }
                break;
            default:
                throw new IllegalStateException("unsupported bucket mode");
        }
        try {
            log.debug("resolving path: " + path.toString());
            Path bucketPath = root.resolve(path.toString());
            if (!Files.exists(bucketPath) || !Files.isDirectory(bucketPath)) {
                throw new IllegalArgumentException("Invalid bucket: " + bucket);
            }
            Iterator<StorageMetadata> iterator = null;
            switch (bucketMode) {
                case URI_BASED:
                    iterator = new ArtifactIterator(bucketPath, bucketDepth, fixedParentDir);
                    break;
                case URI_BUCKET_BASED:
                    iterator = new FileSystemIterator(bucketPath, bucketDepth, fixedParentDir);
                    break;
                default:
                    throw new IllegalStateException("unsupported bucket mode");
            }
            return iterator;
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Invalid bucket: " + bucket);
        }
    }
    
    private Path createArtifactPath(URI artifactURI, String uriBucket) {
        StringBuilder path = new StringBuilder();
        switch (bucketMode) {
            case URI_BASED:
                path.append(artifactURI.getScheme());
                path.append(":").append(File.separator);
                path.append(artifactURI.getSchemeSpecificPart());
                break;
            case URI_BUCKET_BASED:
                for (char c : uriBucket.toCharArray()) {
                    path.append(c).append(File.separator);
                }
                UUID filename = UUID.randomUUID();
                path.append(filename);
                break;
            default:
                throw new IllegalStateException("unsupported bucket mode");
        }
        return root.resolve(path.toString());
    }
    
    private Path createStorageLocationPath(StorageLocation storageLocation) throws ResourceNotFoundException {
        URI storageID = storageLocation.getStorageID();
        if (!storageID.getScheme().equals(STORAGE_URI_SCHEME)) {
            throw new IllegalArgumentException("Unkown storage id scheme: " + storageID.getScheme());
        }
        StringBuilder path = new StringBuilder();
        switch (bucketMode) {
            case URI_BASED:
                path.append(storageID.getSchemeSpecificPart());
                break;
            case URI_BUCKET_BASED:
                String bucket = storageLocation.storageBucket;
                for (char c : bucket.toCharArray()) {
                    path.append(c).append(File.separator);
                }
                path.append(storageLocation.getStorageID().getSchemeSpecificPart());
                break;
            default:
                throw new IllegalStateException("unsupported bucket mode");
        }
        
        log.debug("Resolving path: " + path.toString());
        Path ret = root.resolve(path.toString());
        if (!Files.exists(ret)) {
            throw new ResourceNotFoundException("not found: " + storageID);
        }
        if (!Files.isRegularFile(ret)) {
            throw new IllegalArgumentException("not found: " + storageID);
        }
        return ret;
    }
    
    public static URI createMD5Checksum(Path path) throws NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        InputStream in = Files.newInputStream(path);
        DigestInputStream dis = new DigestInputStream(in, md);
        
        int bytesRead = dis.read();
        byte[] buf = new byte[512];
        while (bytesRead > 0) {
            bytesRead = dis.read(buf);
        }
        byte[] digest = md.digest();
        String md5String = HexUtil.toHex(digest);
        return URI.create(FileSystemStorageAdapter.MD5_CHECKSUM_SCHEME + ":" + md5String);
    }
    
}

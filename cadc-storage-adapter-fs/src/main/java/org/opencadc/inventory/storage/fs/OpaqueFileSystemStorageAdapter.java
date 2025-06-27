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
    static boolean XATTR_EXEC;
    
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

        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "rootDirectory", rootDirectory);

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
    public BucketType getBucketType() {
        return BucketType.HEX;
    }
    
    @Override
    public void recover(StorageLocation storageLocation, Date contentLastModified) 
            throws ResourceNotFoundException, IOException, InterruptedException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "storageLocation", storageLocation);
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

    
    @Override
    protected StorageLocation createStorageLocation(URI artifactURI, Path tmpfile) {
        // ignore artifactURI
        // create from tmpfile in the txnPath to re-use UUID
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
        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "storageLocation.bucket", bucket);
        for (char c : bucket.toCharArray()) {
            path.append(c).append(File.separator);
        }
        path.append(storageLocation.getStorageID().getSchemeSpecificPart());
        log.debug("Resolving path in content : " + path.toString());
        Path ret = contentPath.resolve(path.toString());
        return ret;
    }

    @Override
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

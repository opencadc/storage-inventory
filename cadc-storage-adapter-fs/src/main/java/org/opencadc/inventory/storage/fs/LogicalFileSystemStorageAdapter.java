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
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * An implementation of the storage adapter interface on a file system. This implementation
 * organises files on disk using the scheme-specific part of the Artifact.uri as the relative
 * path.
 * 
 * @author pdowler
 */
public class LogicalFileSystemStorageAdapter extends AbstractStorageAdapter implements StorageAdapter {
    
    private static final Logger log = Logger.getLogger(LogicalFileSystemStorageAdapter.class);
    
    public static final String CONFIG_FILE = "cadc-storage-adapter-fs.properties";
    public static final String CONFIG_PROPERTY_ROOT = OpaqueFileSystemStorageAdapter.class.getPackage().getName() + ".baseDir";
    
    static final String CHECKSUM_ATTRIBUTE_NAME = "contentChecksum";
    static final String CONTENT_FOLDER = "content";
    private static final String TXN_FOLDER = "transaction";

    static final String MD5_CHECKSUM_SCHEME = "md5";
    
    private final FileSystem fs;
    
    public LogicalFileSystemStorageAdapter() {
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
        
        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "rootDirectory", rootVal);
        this.fs = FileSystems.getDefault();

        Path root = this.fs.getPath(rootVal);
        super.contentPath = root.resolve(CONTENT_FOLDER);
        super.txnPath = root.resolve(TXN_FOLDER);

        init(root);
    }

    public LogicalFileSystemStorageAdapter(File rootDirectory) {
        InventoryUtil.assertNotNull(LogicalFileSystemStorageAdapter.class, "rootDirectory", rootDirectory);
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
    public void setRecoverableNamespaces(List<Namespace> preserved) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPurgeNamespaces(List<Namespace> purged) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketType getBucketType() {
        return BucketType.NONE; // path in storageID
    }
    
    @Override
    public void recover(StorageLocation storageLocation, Date contentLastModified) 
        throws ResourceNotFoundException, IOException, InterruptedException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<StorageMetadata> iterator()
        throws StorageEngageException, TransientException {
        return iterator(null, false);
    }
    
    @Override
    public Iterator<StorageMetadata> iterator(String storageBucket)
        throws StorageEngageException, TransientException {
        return iterator(storageBucket, false);
    }

    @Override
    public Iterator<StorageMetadata> iterator(String storageBucketPrefix, boolean includeRecoverable) throws StorageEngageException, TransientException {
        return new LogicalIterator(contentPath, storageBucketPrefix);
    }

    @Override
    protected StorageMetadata createStorageMetadata(Path base, Path p, boolean includeRecoverable) {
        if (txnPath.equals(base)) {
            log.warn("in-txn: re-use OpaqueFileSystemStorageAdapter.createStorageMetadataImpl");
            return OpaqueFileSystemStorageAdapter.createStorageMetadataImpl(base, p, includeRecoverable);
        }
        // content
        return createStorageMetadataImpl(base, p, includeRecoverable);
    }
    
    // content
    static StorageMetadata createStorageMetadataImpl(Path base, Path p, boolean includeRecoverable) {
        Path rel = base.relativize(p);
        log.warn("base: " + base + " path: " + p + " rel: " + rel);
        
        URI artifactID = URI.create(rel.toString());
        
        StorageLocation storageLocation = createStorageLocationImpl(artifactID);
        try {
            URI checksum = new URI(AbstractStorageAdapter.getFileAttribute(p, LogicalFileSystemStorageAdapter.CHECKSUM_ATTRIBUTE_NAME));
            long length = Files.size(p);
            StorageMetadata meta = new StorageMetadata(storageLocation, artifactID, 
                    checksum, length, new Date(Files.getLastModifiedTime(p).toMillis()));
            return meta;
        } catch (Exception ex) {
            throw new RuntimeException("failed to recreate StorageMetadata: " + rel, ex);
        }
    }
    
    @Override
    protected StorageLocation createStorageLocation(URI artifactURI, Path txnTarget) {
        // ignore txn
        return createStorageLocationImpl(artifactURI);
    }
        
    private static StorageLocation createStorageLocationImpl(URI artifactURI) {
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
    
    @Override
    protected Path storageLocationToPath(StorageLocation storageLocation) {
        URI storageID = storageLocation.getStorageID();
        StringBuilder path = new StringBuilder();

        path.append(storageID.toString());
        
        log.debug("Resolving path in content : " + path.toString());
        Path ret = contentPath.resolve(path.toString());
        return ret;
    }

}

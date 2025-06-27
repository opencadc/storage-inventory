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

import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.MessageDigestAPI;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
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
    
    protected Path txnPath;
    protected Path contentPath;
    
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
    public Iterator<PutTransaction> transactionIterator() throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
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
    
    protected abstract Path storageLocationToPath(StorageLocation loc);
    
    protected abstract StorageMetadata createStorageMetadata(Path base, Path p, boolean includeRecoverable);
}

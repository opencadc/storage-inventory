/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020.                            (c) 2020.
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

package org.opencadc.inventory.storage.swift;

import ca.nrc.cadc.io.ByteCountInputStream;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.MultiBufferIO;
import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.IncorrectContentChecksumException;
import ca.nrc.cadc.net.IncorrectContentLengthException;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.StringUtil;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.exception.CommandException;
import org.javaswift.joss.exception.Md5ChecksumException;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.instructions.UploadInstructions;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.InvalidConfigException;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 *
 * @author pdowler
 */
public class SwiftStorageAdapter  implements StorageAdapter {
    private static final Logger log = Logger.getLogger(SwiftStorageAdapter.class);

    private static final long CEPH_UPLOAD_LIMIT = 5 * 1024L * 1024L * 1024L; // 5 GiB
    private static final String CEPH_UPLOAD_LIMIT_MSG = "5 GiB";
    
    static final String CONFIG_FILENAME = "cadc-storage-adapter-swift.properties";
    
    static final String CONF_ENDPOINT = SwiftStorageAdapter.class.getName() + ".authEndpoint";
    static final String CONF_USER = SwiftStorageAdapter.class.getName() + ".username";
    static final String CONF_KEY = SwiftStorageAdapter.class.getName() + ".key";
    static final String CONF_SBLEN = SwiftStorageAdapter.class.getName() + ".bucketLength";
    static final String CONF_BUCKET = SwiftStorageAdapter.class.getName() + ".bucketName";
    static final String CONF_ENABLE_MULTI = SwiftStorageAdapter.class.getName() + ".multiBucket";

    private static final String KEY_SCHEME = "id";

    private static final String DEFAULT_CHECKSUM_ALGORITHM = "md5";
    
    // ceph or swift does all kinds of mangling of metadata keys (char substitution, truncation, case changes)
    // javaswift lib seems to at least force keys in the Map to lower case to protect against some of it
    private static final String ARTIFACT_ID_ATTR = "org.opencadc.artifactid";
    private static final String CONTENT_CHECKSUM_ATTR = "org.opencadc.contentchecksum";
    private static final String TRANSACTION_ATTR = "org.opencadc.transaction";
    
    private static final String VERSION_ATTR = "org.opencadc.swift.version";
    private static final String BUCKETLENGTH_ATTR = "org.opencadc.swift.storagebucketlength";
    private static final String MULTIBUCKET_ATTR = "org.opencadc.swift.multibucket";
    
    private static final int CIRC_BUFFERS = 3;
    private static final int CIRC_BUFFERSIZE = 64 * 1024;

    // test code checks these
    final int storageBucketLength;
    final String storageBucket;
    final String txnBucket;
    final boolean multiBucket;
    private final Account client;
    private Container txnContainer;
    
    // temporary hack to store intermediate digest state: cannot work across multiple JVMs aka with load balanced deployment
    private static final Map<String,MessageDigest> txnDigestStore = new TreeMap<>();
    
    // ctor for unit tests that do not connect
    SwiftStorageAdapter(String storageBucket, int storageBucketLength, boolean multiBucket) {
        this.storageBucket = storageBucket;
        this.txnBucket = storageBucket + "txn";
        this.storageBucketLength = storageBucketLength;
        this.multiBucket = multiBucket;
        this.client = null;
    }
    
    public SwiftStorageAdapter() throws InvalidConfigException, StorageEngageException {
        this(true, null, null, null);
    }
    
    // ctor for intTest to override configured multiBucket flag
    SwiftStorageAdapter(boolean connect, String storageBucketOverride, Integer storageBucketLengthOverride, Boolean multiBucketOverride) 
            throws InvalidConfigException, StorageEngageException {
        final AccountConfig ac = new AccountConfig();
        try {
            File config = new File(System.getProperty("user.home") + "/config/" + CONFIG_FILENAME);
            Properties props = new Properties();
            props.load(new FileReader(config));
            
            StringBuilder sb = new StringBuilder();
            sb.append("incomplete config: ");
            boolean ok = true;
            
            final String suri = props.getProperty(CONF_ENDPOINT);
            sb.append("\n\t").append(CONF_ENDPOINT).append(": ");
            if (suri == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }

            final String sbl = props.getProperty(CONF_SBLEN);
            sb.append("\n\t").append(CONF_SBLEN).append(": ");
            if (sbl == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }

            final String bn = props.getProperty(CONF_BUCKET);
            sb.append("\n\t").append(CONF_BUCKET).append(": ");
            if (bn == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }
            
            final String emb = props.getProperty(CONF_ENABLE_MULTI);
            sb.append("\n\t").append(CONF_ENABLE_MULTI).append(": ");
            if (emb == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }
            
            final String user = props.getProperty(CONF_USER);
            sb.append("\n\t").append(CONF_USER).append(": ");
            if (user == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }
            
            final String key = props.getProperty(CONF_KEY);
            sb.append("\n\t").append(CONF_KEY).append(": ");
            if (bn == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }
            
            if (!ok) {
                throw new InvalidConfigException(sb.toString());
            }
           
            if (storageBucketLengthOverride != null) {
                this.storageBucketLength = storageBucketLengthOverride; // unbox
            } else {
                this.storageBucketLength = Integer.parseInt(sbl.trim());
            }
            
            if (multiBucketOverride != null) {
                this.multiBucket = multiBucketOverride; // unbox
            } else {
                this.multiBucket = Boolean.parseBoolean(emb);
            }

            if (storageBucketOverride != null) {
                this.txnBucket = storageBucketOverride;
            } else {
                this.txnBucket = bn;
            }
            if (multiBucket) {
                this.storageBucket = txnBucket; // dynamic hex buckets appended
            } else {
                this.storageBucket = txnBucket + "-content";
            }
            
            ac.setAuthenticationMethod(AuthenticationMethod.BASIC);
            ac.setUsername(user);
            ac.setPassword(key);
            ac.setAuthUrl(suri);
            
        } catch (FileNotFoundException ex) {
            throw new InvalidConfigException("missing config", ex);
        } catch (Exception ex) {
            throw new InvalidConfigException("invalid config", ex);
        }
        
        try {
            this.client = new AccountFactory(ac).createAccount();
            checkConnectivity();
            init();
        } catch (InvalidConfigException | StorageEngageException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new StorageEngageException("connectiviy check failed", ex);
        }
    }

    private void checkConnectivity() {
        // check connectivity
        client.getServerTime();
    }
    
    private void init() throws InvalidConfigException, StorageEngageException {
        // base-name bucket to store config attributes, transient content (transactions)
        this.txnContainer = client.getContainer(txnBucket);
        if (!txnContainer.exists()) {
            log.warn("creating: " + txnContainer.getName());
            txnContainer.create();
            log.warn("created: " + txnContainer.getName());
        }
        if (!multiBucket) {
            Container c = client.getContainer(storageBucket);
            if (!c.exists()) {
                log.warn("creating: " + c.getName());
                c.create();
                log.warn("created: " + c.getName());
            }
        }
        
        // check vs config
        Map<String,Object> curmeta = txnContainer.getMetadata();
        log.warn("metadata items: " + curmeta.size());
        for (Map.Entry<String,Object> me : curmeta.entrySet()) {
            log.warn(me.getKey() + " = " + me.getValue());
        }
        
        String version = (String) curmeta.get(VERSION_ATTR);
        if (version != null) {
            String mbstr = (String) curmeta.get(MULTIBUCKET_ATTR);
            String sblstr = (String) curmeta.get(BUCKETLENGTH_ATTR);
            boolean mbok = (mbstr != null && multiBucket == Boolean.parseBoolean(mbstr));
            boolean sblok = (sblstr != null || Integer.parseInt(sblstr) == storageBucketLength);
            if (!mbok || !sblok) {
                throw new InvalidConfigException("found bucket: " + storageBucket + "/" + sblstr + "/" + mbstr
                    + " -- incompatible with config: " + storageBucket + "/" + storageBucketLength + "/" + multiBucket + "]");
            }
            // previous init OK
            log.warn("init looks OK: " + storageBucket + "/" + storageBucketLength + "/" + multiBucket);
            return;
        }

        if (multiBucket) {
            BucketNameGenerator gen = new BucketNameGenerator(storageBucket, storageBucketLength);
            log.warn("config: " + gen.getCount() + " buckets");
            Iterator<String> iter = gen.iterator();
            while (iter.hasNext()) {
                String bucketName = iter.next();
                Container mb = client.getContainer(bucketName);
                if (!mb.exists()) {
                    try {
                        log.info("creating: " + bucketName);
                        mb.create();
                    } catch (CommandException ex) {
                        throw new StorageEngageException("failed to create container: " + bucketName, ex);
                    }
                } else {
                    log.info("exists: " + bucketName);
                }
            }
        }
        
        // init succeeeded: commit
        Map<String,Object> meta = new TreeMap<>();
        meta.put(VERSION_ATTR, "0.5.0");
        meta.put(BUCKETLENGTH_ATTR, Integer.toString(storageBucketLength));
        meta.put(MULTIBUCKET_ATTR, Boolean.toString(multiBucket));
        txnContainer.setMetadata(meta);
        log.info("init complete: " + storageBucket + "/" + storageBucketLength + "/" + multiBucket);
    }
    
    static class InternalBucket {
        String name;
        
        InternalBucket(String name) {
            this.name = name; 
        }

        @Override
        public String toString() {
            return "InternalBucket[" + name + "]";
        }
    }
    
    // internal storage model for multiBucket=true: 
    //      {baseStorageBucket} is created and used to store config and transactions
    //      {baseStorageBucket}-{StorageLocation.storageBucket} are created at init and stroe file objects
    //      {baseStorageBucket}-{StorageLocation.storageBucket}/{StorageLocation.storageID}
    //      storageID is id:{uuid}
    //      storageBucket is {hex}
    
    // internal storage model for multiBucket=false:
    //      {baseStorageBucket} is created and used to store temporary transaction status in objects
    //      {baseStorageBucket}-content stores file objects
    //      {baseStorageBucket}-content/{StorageLocation.storageID}
    //      storageID is id:{hex}:{uuid}
    //      storageBucket is {hex}
    StorageLocation generateStorageLocation() {
        UUID id = UUID.randomUUID();
        return generateStorageLocation(id);
    }
    
    StorageLocation generateStorageLocation(UUID uuid) {
        String id = uuid.toString();
        if (multiBucket) {
            StorageLocation ret = new StorageLocation(URI.create(KEY_SCHEME + ":" + id));
            ret.storageBucket = InventoryUtil.computeBucket(URI.create(id), storageBucketLength);
            return ret;
        }
        // single bucket
        String sb = InventoryUtil.computeBucket(URI.create(id), storageBucketLength);
        StorageLocation ret = new StorageLocation(URI.create(KEY_SCHEME + ":" + sb + ":" + id));
        ret.storageBucket = sb;
        return ret;
    }
    
    InternalBucket toInternalBucket(StorageLocation loc) {
        if (multiBucket) {
            return new InternalBucket(storageBucket + "-" + loc.storageBucket);
        }
        return new InternalBucket(storageBucket);
    }
    
    StorageLocation toExternal(InternalBucket bucket, String key) {
        // validate key
        String[] parts = key.split(":");
        if (multiBucket && ((parts.length != 2 || !KEY_SCHEME.equals(parts[0])))) {
            throw new IllegalStateException("BUG: invalid multi-bucket object key: " + key);
        }
        if (!multiBucket && ((parts.length != 3 || !KEY_SCHEME.equals(parts[0])))) {
            throw new IllegalStateException("BUG: invalid single-bucket object key: " + key);
        }
        
        StorageLocation ret = new StorageLocation(URI.create(key));
        if (multiBucket) {
            ret.storageBucket = toStorageBucket(bucket);
            if (ret.storageBucket == null) {
                throw new IllegalStateException("BUG: invalid multi-bucket storage bucket: " + bucket.name);
            }
        } else {
            ret.storageBucket = parts[1];
        }
        
        return ret;
    }

    // multi-bucket only: 
    //      return StorageLocation.storageBucket or null if the name is not a container name created by this adapter
    String toStorageBucket(InternalBucket bucket) {
        try {
            // validate bucket
            String base = bucket.name.substring(0, storageBucket.length());
            String sb = bucket.name.substring(1 + storageBucket.length());
            if (!storageBucket.equals(base) || sb.length() != storageBucketLength) {
                return null;
            }
            return sb;
        } catch (StringIndexOutOfBoundsException ex) {
            return null;
        }
    }
    
    private StorageMetadata toStorageMetadata(StorageLocation loc, URI md5, long contentLength, 
            final URI artifactURI, Date lastModified) {
        StorageMetadata storageMetadata = new StorageMetadata(loc, md5, contentLength);
        storageMetadata.artifactURI = artifactURI;
        storageMetadata.contentLastModified = lastModified;
        return storageMetadata;
    }
    
    // get the swift container that would include the specified location
    private Container getContainerImpl(StorageLocation loc, boolean createIfNotExists) throws ResourceNotFoundException {
        InternalBucket bucket = toInternalBucket(loc);
        Container sub = client.getContainer(bucket.name);
        if (!sub.exists()) {
            if (createIfNotExists) {
                sub.create();
            } else {
                throw new ResourceNotFoundException("not found: " + loc);
            }
        }
        return sub;
    }
    
    private StoredObject getStoredObject(StorageLocation loc, boolean inTxn) throws ResourceNotFoundException {
        Container sub = getContainerImpl(loc, false);
        String key = loc.getStorageID().toASCIIString();
        StoredObject obj = sub.getObject(key);
        if (!obj.exists()) {
            throw new ResourceNotFoundException("not found: " + loc);
        }
        boolean objInTxn = isInTransaction(obj);
        if (!inTxn && !objInTxn) {
            return obj;
        }
        if (inTxn && objInTxn) {
            return obj;
        }
        
        log.warn("skip object in transaction: " + obj.getName());
        throw new ResourceNotFoundException("not found: " + loc);
    }

    /**
     * Get from storage the artifact identified by storageLocation.
     *
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest The destination stream.
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If writing failed.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     */
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest)
            throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException {
        log.debug("get: " + storageLocation);
        
        StoredObject obj = getStoredObject(storageLocation, false);
        
        try (final InputStream source = obj.downloadObjectAsInputStream()) {
            MultiBufferIO io = new MultiBufferIO(CIRC_BUFFERS, CIRC_BUFFERSIZE);
            io.copy(source, dest);
        } catch (InterruptedException ex) {
            log.debug("get interrupted", ex);
        } catch (ReadException | WriteException e) {
            // Handle before the IOException below so it's not wrapped into that catch.
            throw e;
        } catch (IOException ex) {
            throw new ReadException("close stream failure", ex);
        }
    }

    @Override
    public void get(StorageLocation storageLocation, OutputStream dest, SortedSet<ByteRange> byteRanges) 
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        log.debug("get: " + storageLocation);
        
        StoredObject obj = getStoredObject(storageLocation, false);
        
        DownloadInstructions di = new DownloadInstructions();
        if (!byteRanges.isEmpty()) {
            Iterator<ByteRange> iter = byteRanges.iterator();
            ByteRange br = iter.next();
            if (iter.hasNext()) {
                throw new UnsupportedOperationException("multiple byte ranges not supported");
            }
            
            long endPos = br.getOffset() + br.getLength() - 1L; // RFC7233 range is inclusive
            //di.setRange(new MidPartRange(br.getOffset(), endPos)); // published javaswift 0.10.4 constructor: MidPartRange(int, int)
            di.setRange(new JossRangeWorkaround(br.getOffset(), endPos));
        }
        try (final InputStream source = obj.downloadObjectAsInputStream(di)) {
            MultiBufferIO io = new MultiBufferIO(CIRC_BUFFERS, CIRC_BUFFERSIZE);
            io.copy(source, dest);
        } catch (InterruptedException ex) {
            log.debug("get interrupted", ex);
        } catch (ReadException | WriteException e) {
            // Handle before the IOException below so it's not wrapped into that catch.
            throw e;
        } catch (IOException ex) {
            throw new ReadException("close stream failure", ex);
        }
    }

    /**
     * Write an artifact to storage.
     * The value of storageBucket in the returned StorageMetadata and StorageLocation can be used to
     * retrieve batches of artifacts in some of the iterator signatures defined in this interface.
     * Batches of artifacts can be listed by bucket in two of the iterator methods in this interface.
     * If storageBucket is null then the caller will not be able perform bucket-based batch
     * validation through the iterator methods.
     *
     * @param newArtifact known information about the incoming artifact
     * @param source stream from which to read
     * @param transactionID null for auto-commit, "true" to start a transaction, or existing transactionID
     * @return storage metadata after write
     * @throws ca.nrc.cadc.io.ByteLimitExceededException if content length exceeds limit
     * @throws IncorrectContentChecksumException checksum of the data stream did not match the value in newArtifact
     * @throws IncorrectContentLengthException number bytes read did not match the value in newArtifact
     * @throws ReadException If the client failed to read the stream.
     * @throws WriteException If the storage system failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws ca.nrc.cadc.net.TransientException
     */
    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source, String transactionID)
            throws ByteLimitExceededException, 
            IncorrectContentChecksumException, IncorrectContentLengthException, 
            ReadException, WriteException,
            StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "artifact", newArtifact);
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "source", source);
        
        final StorageLocation storageLocation;
        MessageDigest txnDigest;
        String checksumAlg = DEFAULT_CHECKSUM_ALGORITHM;
        if (newArtifact.contentChecksum != null) {
            checksumAlg = newArtifact.contentChecksum.getScheme(); // TODO: try sha1 in here
        }
        try {
            if (transactionID != null) {
                // validate
                storageLocation = getTransactionStorageLocation(transactionID);
                txnDigest = txnDigestStore.get(transactionID);
                if (txnDigest == null) {
                    throw new IllegalArgumentException("unknown transaction: " + transactionID);
                }
                // TODO check that artifact URI matches stored attr
            } else {
                storageLocation = generateStorageLocation();
                txnDigest = MessageDigest.getInstance(checksumAlg);
            }

        } catch (ResourceNotFoundException ex) {
            // from getTransactionStatus
            throw new IllegalArgumentException(ex.getMessage());
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("failed to create MessageDigest: " + DEFAULT_CHECKSUM_ALGORITHM);
        }
        log.warn("transaction: " + storageLocation + " transactionID: " + transactionID);
        
        if (newArtifact.contentLength != null && newArtifact.contentLength > CEPH_UPLOAD_LIMIT) {
            throw new ByteLimitExceededException("put exceeds size limit (" + CEPH_UPLOAD_LIMIT_MSG + ") for simple stream", CEPH_UPLOAD_LIMIT);
        }

        try {
            TrapFailInputStream trap = new TrapFailInputStream(source);
            ByteCountInputStream bcis = new ByteCountInputStream(trap);
            DigestInputStream dis = new DigestInputStream(bcis, txnDigest);
            
            Container sub = getContainerImpl(storageLocation, true);
            StoredObject obj = sub.getObject(storageLocation.getStorageID().toASCIIString());
            UploadInstructions up = new UploadInstructions(dis);
            //if (newArtifact.contentChecksum != null && "MD5".equalsIgnoreCase(newArtifact.contentChecksum.getScheme())) {
            //    up.setMd5(newArtifact.contentChecksum.getSchemeSpecificPart());
            //}

            if (transactionID != null) {
                try {
                    obj.uploadObject(up);
                //} catch (Md5ChecksumException ex) {
                    //swift detected
                //    throw new PreconditionFailedException("checksum mismatch: " + newArtifact.contentChecksum + " did not match content");
                } catch (CommandException ex) {
                    if (bcis.getByteCount() >= CEPH_UPLOAD_LIMIT && hasWriteFailSocketException(ex)) {
                        abortTransaction(transactionID);
                        throw new ByteLimitExceededException("put exceeds size limit (" + CEPH_UPLOAD_LIMIT_MSG + ") for simple stream", CEPH_UPLOAD_LIMIT);
                    }
                    if (trap.fail != null) {
                        log.warn("abort transactionID " + transactionID + " after failed input: " + trap.fail);
                        abortTransaction(transactionID);
                        throw new ReadException("read from input stream failed", trap.fail);
                    }
                    log.warn("abort transactionID " + transactionID + " after failed write to back end: ", ex);
                    abortTransaction(transactionID);
                    throw new WriteException("internal failure: " + ex);
                }
            } else {
                try {
                    obj.uploadObject(up);
                } catch (Md5ChecksumException ex) {
                    //swift detected
                    throw new PreconditionFailedException("checksum mismatch: " + newArtifact.contentChecksum + " did not match content");
                } catch (CommandException ex) {
                    if (bcis.getByteCount() >= CEPH_UPLOAD_LIMIT && hasWriteFailSocketException(ex)) {
                        throw new ByteLimitExceededException("put exceeds size limit (" + CEPH_UPLOAD_LIMIT_MSG + ") for simple stream", CEPH_UPLOAD_LIMIT);
                    }
                    if (trap.fail != null) {
                        throw new ReadException("read from input stream failed", trap.fail);
                    }
                    throw new WriteException("internal failure: " + ex);
                }
            }
                    
            final MessageDigest md = dis.getMessageDigest();
            // clone so we can persist the current state for resume
            MessageDigest curMD = (MessageDigest) md.clone();
            String md5Val = HexUtil.toHex(curMD.digest());
            URI checksum = URI.create(checksumAlg + ":" + md5Val);
            log.warn("current checksum: " + checksum);
            Long length = obj.getContentLength();
            log.warn("current file size: " + length);
            
            if (transactionID != null && (newArtifact.contentLength == null || length < newArtifact.contentLength)) {
                // incomplete: no further content checks
                log.debug("incomplete put in transaction: " + transactionID + " - not verifying checksum");
            } else {
                if (newArtifact.contentChecksum != null && !checksum.equals(newArtifact.contentChecksum)) {
                    obj.delete();
                    throw new PreconditionFailedException("checksum mismatch: " + newArtifact.contentChecksum + " != " + checksum);
                }

                if (newArtifact.contentLength != null && !length.equals(newArtifact.contentLength)) {
                    obj.delete();
                    throw new PreconditionFailedException("length mismatch: " + newArtifact.contentLength + " != " + length);
                }
            }

            if (transactionID != null) {
                log.warn("transaction uncommitted: " + transactionID + " " + storageLocation);
                // transaction will continue
                obj.setAndSaveMetadata(TRANSACTION_ATTR, "true");
                
                StoredObject txn = txnContainer.getObject("txn:" + transactionID);
                txn.setAndDoNotSaveMetadata(ARTIFACT_ID_ATTR, newArtifact.getArtifactURI().toASCIIString());
                txn.setAndDoNotSaveMetadata(CONTENT_CHECKSUM_ATTR, checksum.toASCIIString());
                txn.saveMetadata();
                
                txnDigestStore.put(transactionID, md);
                
                if (length == 0L) {
                    return new StorageMetadata(storageLocation);
                }
                return new StorageMetadata(storageLocation, checksum, length);
            }
            
            // create this before committing the file so constraints applied
            StorageMetadata metadata = new StorageMetadata(storageLocation, checksum, length);
            metadata.artifactURI = newArtifact.getArtifactURI();
            
            StorageMetadata ret = commit(metadata, obj);
            return ret;
        } catch (ResourceNotFoundException ex) {
            throw new RuntimeException("failed to find/create child container: " + storageLocation.storageBucket, ex);
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException("BUG: MessageDigest is not cloneable", ex);
        } catch (CommandException ex) {
            throw new StorageEngageException("internal failure: " + ex);
        }
    }
    
    // TODO: in order to resume a failed upload, we probably have to not throw the IOException so joss/swift/ceph don't
    // invalidate/delete the partial object... TBD
    private static class TrapFailInputStream extends InputStream {
        IOException fail;
        final InputStream istream;

        public TrapFailInputStream(InputStream istream) {
            this.istream = istream;
        }
        
        @Override
        public int read() throws IOException {
            try {
                return istream.read();
            } catch (IOException ex) {
                this.fail = ex;
                throw ex;
            }
        }

        @Override
        public int read(byte[] bytes) throws IOException {
            try {
                return istream.read(bytes);
            } catch (IOException ex) {
                this.fail = ex;
                throw ex;
            }
        }
        
        @Override
        public int read(byte[] bytes, int off, int len) throws IOException {
            try {
                return istream.read(bytes, off, len);
            } catch (IOException ex) {
                this.fail = ex;
                throw ex;
            }
        }
    }

    @Override
    public String startTransaction(URI artifactURI) throws StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "artifactURI", artifactURI);
        try {
            final String transactionID = UUID.randomUUID().toString();
            
            // init digest
            MessageDigest md = MessageDigest.getInstance("MD5");
            MessageDigest curMD = (MessageDigest) md.clone();
            byte[] dig = curMD.digest();
            String md5Val = HexUtil.toHex(dig);
            URI checksum = URI.create("md5:" + md5Val);
            
            // create object to track txn status
            StoredObject txn = txnContainer.getObject("txn:" + transactionID);
            // need some bytes in getTransactionStatus
            // TODO: store serialized MessageDigest object to support continue in put
            txn.uploadObject(dig);

            txn.setAndDoNotSaveMetadata(ARTIFACT_ID_ATTR, artifactURI.toASCIIString());
            txn.setAndDoNotSaveMetadata(CONTENT_CHECKSUM_ATTR, checksum.toASCIIString());
            txn.saveMetadata();

            // TODO: serialise the md object and store it in the txn status object
            txnDigestStore.put(transactionID, md);
            
            return transactionID;
        } catch (CloneNotSupportedException | NoSuchAlgorithmException ex) {
            throw new RuntimeException("BUG", ex);
        } catch (Exception ex) {
            throw new StorageEngageException("failed to create transaction", ex);
        } 
    }

    @Override
    public StorageMetadata commitTransaction(String transactionID) throws ResourceNotFoundException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "transactionID", transactionID);
        try {
            StorageMetadata metadata = getTransactionStatus(transactionID);
            log.warn("txn status: " + metadata.getStorageLocation());
            
            StoredObject txn = txnContainer.getObject("txn:" + transactionID);
            if (!txn.exists()) {
                throw new IllegalArgumentException("unknown transaction: " + transactionID);
            }
            
            StoredObject obj = getStoredObject(metadata.getStorageLocation(), true);

            log.warn("delete txn: " + txn.getName());
            txn.delete();
            
            StorageMetadata ret = commit(metadata, obj);
            txnDigestStore.remove(transactionID);
            return ret;
        } catch (CommandException ex) {
            throw new StorageEngageException("failed to commit transaction: " + transactionID, ex);
        }
    }
    
    private StorageMetadata commit(StorageMetadata sm, StoredObject obj) throws StorageEngageException {
        try {
            obj.setAndDoNotSaveMetadata(ARTIFACT_ID_ATTR, sm.artifactURI.toASCIIString());
            obj.setAndDoNotSaveMetadata(CONTENT_CHECKSUM_ATTR, sm.getContentChecksum().toASCIIString());
            obj.removeAndDoNotSaveMetadata(TRANSACTION_ATTR);
            obj.saveMetadata();
            sm.contentLastModified = obj.getLastModifiedAsDate(); // after assign attrs
            return sm;
        } catch (CommandException ex) {
            throw new StorageEngageException("failed to persist attributes: " + sm.getStorageLocation(), ex);
        }
    }

    @Override
    public void abortTransaction(String transactionID) throws ResourceNotFoundException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "transactionID", transactionID);
        // validate
        UUID uuid = UUID.fromString(transactionID);
        
        try {
            StorageMetadata metadata = getTransactionStatus(transactionID);
            log.warn("txn status: " + metadata.getStorageLocation());
                
            StoredObject txn = txnContainer.getObject("txn:" + transactionID);
            if (!txn.exists()) {
                throw new ResourceNotFoundException("unknown transaction: " + transactionID);
            }
            log.warn("delete txn: " + txn.getName());
            txn.delete();
            try {
                
                StoredObject obj = getStoredObject(metadata.getStorageLocation(), true);
                log.warn("delete obj: " + obj.getName());
                obj.delete();
            } catch (ResourceNotFoundException ex) {
                log.warn("abortTransaction: " + transactionID + " - file object not found", ex);
            }
        } catch (CommandException ex) {
            throw new StorageEngageException("failed to abort transaction: " + transactionID, ex);
        }
    }

    @Override
    public StorageMetadata getTransactionStatus(String transactionID) throws ResourceNotFoundException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "transactionID", transactionID);
        UUID uuid = UUID.fromString(transactionID);
        try {
            StoredObject txn = txnContainer.getObject("txn:" + transactionID);
            
            // temp: need to get contentLength from the real object
            StorageLocation loc = generateStorageLocation(uuid);
            Container sub = getContainerImpl(loc, true);
            StoredObject obj = sub.getObject(loc.getStorageID().toASCIIString());
            long contentLength = 0L;
            if (obj.exists()) {
                contentLength = obj.getContentLength();
            }
            
            if (txn.exists()) {
                Map<String,Object> meta = txn.getMetadata();
                String scs = (String) meta.get(CONTENT_CHECKSUM_ATTR);
                String auri = (String) meta.get(ARTIFACT_ID_ATTR);

                URI md5 = new URI(scs);
                URI artifactURI = new URI(auri);
                // location of the actual file
                
                return toStorageMetadata(loc, md5, contentLength, artifactURI, txn.getLastModifiedAsDate());
            }
        } catch (IllegalArgumentException | IllegalStateException | URISyntaxException ex) {
            throw new RuntimeException("BUG: invalid object: " + transactionID, ex);
        }
        
        throw new ResourceNotFoundException("unknown transaction: " + transactionID);
    }
    
    private boolean isInTransaction(StoredObject obj) {
        String val = (String) obj.getMetadata(TRANSACTION_ATTR);
        return ("true".equals(val));
    }
    
    private StorageLocation getTransactionStorageLocation(String transactionID) throws ResourceNotFoundException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "transactionID", transactionID);
        UUID uuid = UUID.fromString(transactionID);
        StoredObject txn = txnContainer.getObject("txn:" + transactionID);
        if (txn.exists()) {
            return generateStorageLocation(uuid);
        }
        throw new ResourceNotFoundException("unknown transaction: " + transactionID);
    }
    
    // main use: iterator
    private StorageMetadata objectToStorageMetadata(InternalBucket bucket, StoredObject obj) {
        final StorageLocation loc = toExternal(bucket, obj.getName());
        try {
            Map<String,Object> meta = obj.getMetadata();
            String scs = (String) meta.get(CONTENT_CHECKSUM_ATTR);
            String auri = (String) meta.get(ARTIFACT_ID_ATTR);

            if (scs == null || auri == null) {
                // put failed to set metadata after writing the file: invalid
                return new StorageMetadata(loc);
            }

            URI md5 = new URI(scs);
            URI artifactURI = new URI(auri);

            return toStorageMetadata(loc, md5, obj.getContentLength(), artifactURI, obj.getLastModifiedAsDate());
        } catch (IllegalArgumentException | IllegalStateException | URISyntaxException ex) {
            return new StorageMetadata(loc);
        }
    }

    // ceph+swift API: when writing past limit the stream gets closed 
    private boolean  hasWriteFailSocketException(Exception ex) {
        Throwable cause = ex.getCause();
        while (cause != null) {
            if (cause instanceof SocketException && "broken pipe (write failed)".equalsIgnoreCase(cause.getMessage())) {
                return  true;
            }
            cause = cause.getCause();
        }
        return false;
    }
        
    // used by intTest
    boolean exists(StorageLocation storageLocation) throws StorageEngageException {
        log.debug("exists: " + storageLocation);
        try {
            Container sub = getContainerImpl(storageLocation, false);
            String key = storageLocation.getStorageID().toASCIIString();
            StoredObject obj = sub.getObject(key);
            return obj.exists();
        } catch (ResourceNotFoundException ex) {
            return false; // no container
        } catch (Exception ex) {
            throw new StorageEngageException("failed to delete: " + storageLocation, ex);
        }
    }
    
    /**
     * Delete from storage the artifact identified by storageLocation.
     *
     * @param storageLocation Identifies the artifact to delete.
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException If an unrecoverable error occurred.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred.
     */
    @Override
    public void delete(StorageLocation storageLocation)
            throws ResourceNotFoundException, IOException, StorageEngageException, TransientException {
        log.debug("delete: " + storageLocation);
        String key = storageLocation.getStorageID().toASCIIString();
        
        // invalid object found by StorageMetadataIterator
        if (key.startsWith("invalid:")) {
            key = key.replace("invalid:", "");
        }
        try {
            Container sub = getContainerImpl(storageLocation, false);
            StoredObject obj = sub.getObject(key);
            if (obj.exists()) {
                obj.delete();
                return;
            }
        } catch (ResourceNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new StorageEngageException("failed to delete: " + storageLocation, ex);
        }
        throw new ResourceNotFoundException("not found: " + storageLocation);
    }

    @Override
    public Iterator<StorageMetadata> iterator() throws StorageEngageException, TransientException {
        return iterator(null);
    }

    @Override
    public Iterator<StorageMetadata> iterator(String bucketPrefix) throws StorageEngageException, TransientException {
        client.resetContainerCache();
        if (multiBucket) {
            return new MultiBucketStorageIterator(bucketPrefix);
        }
        return new SingleBucketStorageIterator(bucketPrefix);
    }

    // for intTest cleanup
    Iterator<Container> bucketIterator() {
        return new BucketIterator(null);
    }
    
    void deleteBucket(Container c) throws Exception {
        c.delete();
    }
    
    // iterator over all dynamically generated storage buckets
    private class BucketIterator implements Iterator<Container> {
        private static final int BATCH_SIZE = 1024;
        
        String bucketPrefix;
        String queryPrefix;
        Iterator<Container> iter;
        private boolean done = false;
        
        BucketIterator(String bucketPrefix) {
            this.queryPrefix = storageBucket + "-";
            this.bucketPrefix = queryPrefix;
            if (bucketPrefix != null) {
                this.bucketPrefix = queryPrefix + bucketPrefix;
            }
        }
        
        @Override
        public boolean hasNext() {
            if (done) {
                return false;
            }
            
            // laxy init
            if (iter == null) {
                // WARNING: With CEPH Object Store and Swift API:
                // pagesize limit is applied before filtering with the query prefix so if there are enough
                // non-matching containers before the prefix the result will be empty
                
                List<Container> keep = new ArrayList<>();
                boolean doList = true;
                String nextMarkeyKey = null;
                while (doList) {
                    Collection<Container> list = client.list(queryPrefix, nextMarkeyKey, BATCH_SIZE);
                    log.debug("BucketIterator from=" + nextMarkeyKey + " -> size=" + list.size());
                    doList = !list.isEmpty();
                    for (Container c : list) {
                        if (c.getName().startsWith(bucketPrefix)) {
                            keep.add(c);
                        }
                        nextMarkeyKey = c.getName();
                    }
                }
                log.debug("BucketIterator bucketprefix=" + bucketPrefix + " keep=" + keep.size());
                if (!keep.isEmpty()) {
                    this.iter = keep.iterator();
                } else {
                    done = true;
                }
            }
            
            if (iter == null) {
                return false;
            }

            return iter.hasNext();
        }

        @Override
        public Container next() {
            Container c = iter.next();
            return c;
        }
    }
    
    private class MultiBucketStorageIterator implements Iterator<StorageMetadata> {
        private static final int BATCH_SIZE = 1024;

        private Iterator<Container> bucketIterator;
        private Container currentBucket;
        private InternalBucket icurBucket;
        
        private Iterator<StoredObject> objectIterator;
        private StorageMetadata nextItem;
        private String nextMarkerKey;

        public MultiBucketStorageIterator(String bucketPrefix) {
            this.bucketIterator = new BucketIterator(bucketPrefix);
            advance();
        }
        
        @Override
        public boolean hasNext() {
            return (nextItem != null);
        }

        @Override
        public StorageMetadata next() {
            if (nextItem == null) {
                throw new NoSuchElementException();
            }
            
            StorageMetadata ret = nextItem;
            advance();
            return ret;
        }
        
        private void advance() {
            this.nextItem = null;
            while (true) {
                // next bucket
                if (currentBucket == null && bucketIterator.hasNext()) {
                    currentBucket = bucketIterator.next();
                    icurBucket = new InternalBucket(currentBucket.getName());
                    nextMarkerKey = null;
                }
                if (currentBucket == null) {
                    log.debug("MultiBucketStorageIterator: DONE");
                    return;
                }
                
                // next batch in current bucket
                if (objectIterator == null || !objectIterator.hasNext()) {
                    Collection<StoredObject> list = currentBucket.list(null, nextMarkerKey, BATCH_SIZE);
                    log.debug("SingleBucketStorageIterator bucket=" + currentBucket.getName()
                            + " size=" + list.size() + " from=" + nextMarkerKey);
                    if (!list.isEmpty()) {
                        objectIterator = list.iterator();
                    } else {
                        log.debug("MultiBucketStorageIterator: " + currentBucket.getName() + " DONE");
                        currentBucket = null;
                        icurBucket = null;
                        objectIterator = null; // bucket done
                    }
                }
                
                // next object in current batch
                if (objectIterator != null) {
                    while (objectIterator.hasNext()) {
                        StoredObject obj = objectIterator.next();
                        this.nextMarkerKey = obj.getName();
                        if (isInTransaction(obj)) {
                            log.debug("MultiBucketStorageIterator.advance: skip " + obj.getName());
                        } else {
                            log.debug("MultiBucketStorageIterator.advance: next " + obj.getName() + " len=" + obj.getContentLength());
                            this.nextItem = objectToStorageMetadata(icurBucket, obj);
                            return; // nextItem staged
                        }
                    }
                }
            }
        }
    }
    
    private class SingleBucketStorageIterator implements Iterator<StorageMetadata> {

        private static final int BATCH_SIZE = 1024;
        private Iterator<StoredObject> objectIterator;
        private final String bucketPrefix;
        private final Container swiftContainer;
        
        private StorageMetadata nextItem;
        private String nextMarkerKey; // name of last item returned by next()
        private boolean done = false;

        public SingleBucketStorageIterator(String bucketPrefix) {
            if (bucketPrefix != null) {
                this.bucketPrefix = KEY_SCHEME + ":" + bucketPrefix;
            } else {
                this.bucketPrefix = null;
            }
            this.swiftContainer = client.getContainer(storageBucket);
            advance();
        }
        
        @Override
        public boolean hasNext() {
            return (nextItem != null);
        }

        @Override
        public StorageMetadata next() {
            if (nextItem == null) {
                throw new NoSuchElementException();
            }
            
            StorageMetadata ret = nextItem;
            advance();
            return ret;
        }
        
        private void advance() {
            this.nextItem = null;
            while (true) {
                if (objectIterator == null || !objectIterator.hasNext()) {
                    Collection<StoredObject> list = swiftContainer.list(bucketPrefix, nextMarkerKey, BATCH_SIZE);
                    log.debug("SingleBucketStorageIterator bucketPrefix=" + bucketPrefix + " size=" + list.size() + " from=" + nextMarkerKey);
                    if (!list.isEmpty()) {
                        objectIterator = list.iterator();
                    } else {
                        log.debug("SingleBucketStorageIterator: DONE");
                        objectIterator = null;
                        return; // nextItem == null aka done
                    }
                }
                if (objectIterator != null) {
                    while (objectIterator.hasNext()) {
                        StoredObject obj = objectIterator.next();
                        this.nextMarkerKey = obj.getName();
                        if (isInTransaction(obj)) {
                            log.debug("SingleBucketStorageIterator.advance: skip " + obj.getName());
                        } else {
                            log.debug("SingleBucketStorageIterator.advance: next " + obj.getName() + " len=" + obj.getContentLength());
                            this.nextItem = objectToStorageMetadata(null, obj);
                            return; // nextItem staged
                        }
                    }
                }
            }
        }
    }
}

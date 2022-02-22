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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.exception.CommandException;
import org.javaswift.joss.exception.Md5ChecksumException;
import org.javaswift.joss.headers.object.ObjectManifest;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.instructions.UploadInstructions;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.DigestInputStream;
import org.opencadc.inventory.storage.InvalidConfigException;
import org.opencadc.inventory.storage.MessageDigestAPI;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * StorageAdapter implementation using the SWIFT API to store files in an
 * object store. This has been developed and tested using a CEPH Object Store
 * back end; correct operation may depend on behavior of CEPH (including  
 * configuration and deployment details).
 * 
 * @author pdowler
 */
public class SwiftStorageAdapter  implements StorageAdapter {
    private static final Logger log = Logger.getLogger(SwiftStorageAdapter.class);

    static final long GIGABYTE = 1024L * 1024L * 1024L;
    private static final long CEPH_OBJECT_SIZE_LIMIT = 5 * GIGABYTE;
    private static final String CEPH_OBJECT_SIZE_LIMIT_MSG = "5 GiB";
    
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
    
    // config atributes stored on main bucket
    private static final String VERSION_ATTR = "org.opencadc.swift.version";
    private static final String BUCKETLENGTH_ATTR = "org.opencadc.swift.storagebucketlength";
    private static final String MULTIBUCKET_ATTR = "org.opencadc.swift.multibucket";
    
    // permanent object atributes
    private static final String ARTIFACT_ID_ATTR = "org.opencadc.artifactid";
    private static final String CONTENT_CHECKSUM_ATTR = "org.opencadc.contentchecksum";
    private static final String DLO_ATTR = "org.opencadc.swift.dlo";
    
    // temporary object attributes
    private static final String TRANSACTION_ATTR = "org.opencadc.swift.txn";
    private static final String PT_MIN_ATTR = TRANSACTION_ATTR + ".minlen";
    private static final String PT_MAX_ATTR = TRANSACTION_ATTR + ".maxlen";
    private static final String CUR_DIGEST_ATTR = TRANSACTION_ATTR + ".curdigest";
    private static final String PREV_DIGEST_ATTR = TRANSACTION_ATTR + ".prevdigest";
    private static final String TOTAL_LENGTH_ATTR = TRANSACTION_ATTR + ".totallen";
    
    private static final int CIRC_BUFFERS = 3;
    private static final int CIRC_BUFFERSIZE = 64 * 1024;

    // test code checks these
    final int storageBucketLength;
    final String storageBucket;
    final String txnBucket;
    final boolean multiBucket;
    long segmentMaxBytes = CEPH_OBJECT_SIZE_LIMIT;
    long segmentMinBytes = segmentMaxBytes; // default: all files smaller than CEPH limit are simple objects
    
    private final Account client;
    private Container txnContainer;
    
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
            log.info("txnBucket: " + txnBucket + " storageBucket: " + storageBucket);
            
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
            long t1;
            
            log.debug("creating client/authenticate...");
            t1 = System.currentTimeMillis();
            ac.setAllowContainerCaching(true);
            ac.setAllowCaching(true);
            ac.setAllowSynchronizeWithServer(false);
            ac.setAllowReauthenticate(true);
            this.client = new AccountFactory(ac).createAccount();
            final long authTime = System.currentTimeMillis() - t1;
            log.debug("creating client/authenticate: " + authTime + " client: " + client.getClass().getName());
            
            // base-name bucket to store transient content and config attributes
            log.debug("get base storageBucket...");
            t1 = System.currentTimeMillis();
            this.txnContainer = client.getContainer(txnBucket);
            final long bucketTime = System.currentTimeMillis() - t1;
            log.debug("get base storageBucket: " + bucketTime);
            
            log.info("SwiftStorageAdapter.INIT authTime=" + authTime + " bucketTime=" + bucketTime);
            init(txnContainer);
            
        } catch (InvalidConfigException | StorageEngageException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new StorageEngageException("connectiviy check failed", ex);
        }
    }

    
    private void init(Container c) throws InvalidConfigException, StorageEngageException {
        if (!c.exists()) {
            log.info("creating: " + c.getName());
            c.create();
        }
        
        // check vs config
        Map<String,Object> curmeta = c.getMetadata();
        log.debug("metadata items: " + curmeta.size());
        for (Map.Entry<String,Object> me : curmeta.entrySet()) {
            log.debug(me.getKey() + " = " + me.getValue());
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
            log.debug("init looks OK: " + storageBucket + "/" + storageBucketLength + "/" + multiBucket);
            return;
        }

        if (multiBucket) {
            BucketNameGenerator gen = new BucketNameGenerator(storageBucket, storageBucketLength);
            log.info("config: " + gen.getCount() + " buckets");
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
        } else {
            Container mb = client.getContainer(storageBucket);
            if (!mb.exists()) {
                try {
                    log.info("creating: " + storageBucket);
                    mb.create();
                } catch (CommandException ex) {
                    throw new StorageEngageException("failed to create container: " + storageBucket, ex);
                }
            } else {
                log.info("exists: " + storageBucket);
            }
        }
        
        // init succeeeded: commit
        Map<String,Object> meta = new TreeMap<>();
        meta.put(VERSION_ATTR, "0.5.0");
        meta.put(BUCKETLENGTH_ATTR, Integer.toString(storageBucketLength));
        meta.put(MULTIBUCKET_ATTR, Boolean.toString(multiBucket));
        c.setMetadata(meta);
        log.info("bucket init complete: " + storageBucket + "/" + storageBucketLength + "/" + multiBucket);
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
    //      {baseStorageBucket}-{StorageLocation.storageBucket} are created at init and stores file objects
    //      {baseStorageBucket}-{StorageLocation.storageBucket}/{StorageLocation.storageID} is a simple object or manifest of segmented object
    //      {baseStorageBucket}-{StorageLocation.storageBucket}/{StorageLocation.storageID}:p:{sequence} is a part N of a segmented object, N in [1,M]
    //      storageID is id:{uuid}
    //      storageBucket is {hex}
    //      sequence is padded for correct alphabetic ordering required for dynamic large objects
    //               0001 to 9999 segments: 9999 * 5GiB segments = ~50PiB object
    
    // internal storage model for multiBucket=false:
    //      {baseStorageBucket} is created and used to store temporary transaction status in objects
    //      {baseStorageBucket}-content stores file objects
    //      {baseStorageBucket}-content/{StorageLocation.storageID}
    //      storageID is id:{hex}:{uuid}
    //      storageBucket is {hex}
    StorageLocation generateStorageLocation() {
        UUID id = UUID.randomUUID();
        return toStorageLocation(id);
    }
    
    StorageLocation toStorageLocation(UUID uuid) {
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
        StorageMetadata storageMetadata = new StorageMetadata(loc, md5, contentLength, lastModified);
        storageMetadata.artifactURI = artifactURI;
        return storageMetadata;
    }
    
    // get the swift container that would include the specified location
    private Container getContainerImpl(StorageLocation loc) {
        InternalBucket bucket = toInternalBucket(loc);
        Container sub = client.getContainer(bucket.name);
        if (!sub.exists()) {
            throw new RuntimeException("BUG: container not found: " + bucket.name);
        }
        return sub;
    }
    
    private StoredObject getStoredObject(StorageLocation loc, boolean inTxn) throws ResourceNotFoundException {
        Container sub = getContainerImpl(loc);
        String key = loc.getStorageID().toASCIIString();
        StoredObject obj = sub.getObject(key);
        if (!obj.exists()) {
            throw new ResourceNotFoundException("not found: " + loc);
        }
        boolean objInTxn = !isIteratorVisible(obj);
        if (!inTxn && !objInTxn) {
            return obj;
        }
        if (inTxn && objInTxn) {
            return obj;
        }
        
        log.debug("skip object in transaction: " + obj.getName());
        throw new ResourceNotFoundException("not found: " + loc);
    }

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
    public void get(StorageLocation storageLocation, OutputStream dest, ByteRange byteRange) 
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        log.debug("get: " + storageLocation);
        
        StoredObject obj = getStoredObject(storageLocation, false);
        
        DownloadInstructions di = new DownloadInstructions();
        if (byteRange != null) {
            long endPos = byteRange.getOffset() + byteRange.getLength() - 1L; // RFC7233 range is inclusive
            //di.setRange(new MidPartRange(br.getOffset(), endPos)); // published javaswift 0.10.4 constructor: MidPartRange(int, int)
            di.setRange(new JossRangeWorkaround(byteRange.getOffset(), endPos));
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

    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source, String transactionID)
            throws ByteLimitExceededException, 
            IncorrectContentChecksumException, IncorrectContentLengthException, 
            ReadException, WriteException,
            StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "artifact", newArtifact);
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "source", source);
        
        StorageLocation writeDataLocation;
        MessageDigestAPI txnDigest = null;
        String checksumAlg = DEFAULT_CHECKSUM_ALGORITHM;
        if (newArtifact.contentChecksum != null) {
            checksumAlg = newArtifact.contentChecksum.getScheme(); // TODO: try sha1 in here
        }
        SwiftPutTransaction txn = null;
        try {
            if (transactionID != null) {
                txn = getTransactionStatusImpl(transactionID);
                // enforce segment size limits
                if (newArtifact.contentLength == null) {
                    throw new IllegalArgumentException("invalid put: must specify content-length of segment");
                }
                log.warn("enforce segment size limit: " + newArtifact.contentLength + " vs "
                        + "[" + txn.getMinSegmentSize() + "," + txn.getMaxSegmentSize() + "]");
                
                if (txn.getMaxSegmentSize() < newArtifact.contentLength) {
                    throw new IllegalArgumentException("invalid put: segment too large - expected content-length in "
                            + "[" + txn.getMinSegmentSize() + "," + txn.getMaxSegmentSize() + "]");
                }
                
                Long curLength = 0L; 
                if (txn.storageMetadata != null) {
                    curLength = txn.storageMetadata.getContentLength();
                }
                long elen = curLength + newArtifact.contentLength;
                if (txn.totalLength != null) {
                    if (elen == txn.totalLength) {
                        log.warn("transaction " + transactionID + ": last segment, allowing content-length=" + newArtifact.contentLength);
                    } else {
                        if (newArtifact.contentLength < txn.getMinSegmentSize()) {
                            throw new IllegalArgumentException("invalid put: segment too small - expected content-length in "
                                    + "[" + txn.getMinSegmentSize() + "," + txn.getMaxSegmentSize() + "]");
                        }
                        
                    }
                } // else: currently don't allow segmented put of unknown length
                
                if (txn.dynamicLargeObject) {
                    // allow segment put to proceed
                    writeDataLocation = getNextTransactionStorageLocation(txn.getID());
                } else {
                    if (txn.storageMetadata != null) {
                        throw new IllegalArgumentException("data already written: contentLength=" + txn.storageMetadata.getContentLength());
                    }
                    // allow single-segment put to proceed
                    UUID id = UUID.fromString(transactionID);
                    writeDataLocation = toStorageLocation(id);
                }
                String dstate = (String) txn.txnObject.getMetadata(CUR_DIGEST_ATTR);
                if (dstate != null) {
                    txnDigest = MessageDigestAPI.getDigest(dstate);
                }
                if (txnDigest == null) {
                    throw new RuntimeException("BUG: failed to restore digest in transaction " + transactionID);
                }
                String auri = (String) txn.txnObject.getMetadata(ARTIFACT_ID_ATTR);
                if (auri == null) {
                    throw new RuntimeException("BUG: failed to restore uri attribute in transaction " + transactionID);
                }
                if (!newArtifact.getArtifactURI().toASCIIString().equals(auri)) {
                    throw new IllegalArgumentException("incorrect Artifact.uri in transaction: " + transactionID
                        + " expected: " + auri);
                }
            } else {
                writeDataLocation = generateStorageLocation();
                txnDigest = MessageDigestAPI.getInstance(checksumAlg);
            }

        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("failed to create MessageDigestAPI: " + checksumAlg, ex);
        }
        log.debug("write location: " + writeDataLocation + " transaction: " + transactionID);
        
        if (newArtifact.contentLength != null && newArtifact.contentLength > CEPH_OBJECT_SIZE_LIMIT) {
            throw new ByteLimitExceededException("put exceeds size limit (" + CEPH_OBJECT_SIZE_LIMIT_MSG + ") for simple stream", CEPH_OBJECT_SIZE_LIMIT);
        }

        String prevDigestState = null;
        Long prevLength = 0L;
        
        try {
            prevDigestState = MessageDigestAPI.getEncodedState(txnDigest);
            
            TrapFailInputStream trap = new TrapFailInputStream(source);
            ByteCountInputStream bcis = new ByteCountInputStream(trap);
            MessageDigestAPI md = txnDigest;
            DigestInputStream dis = new DigestInputStream(bcis, txnDigest);
            
            Container sub = getContainerImpl(writeDataLocation);
            StoredObject obj = sub.getObject(writeDataLocation.getStorageID().toASCIIString());
            StoredObject metaObj = obj; // simple
            UploadInstructions up = new UploadInstructions(dis);

            if (txn != null) {
                if (txn.dynamicLargeObject) {
                    metaObj = txn.metaObject; // manifest created in startTransaction
                }
                try {
                    if (txn.metaObject != null) {
                        prevLength = metaObj.getContentLength();
                    }
                    obj.uploadObject(up);
                    md = dis.getMessageDigest();
                    log.debug("upload: " + writeDataLocation + " OK");
                } catch (CommandException ex) {
                    if (bcis.getByteCount() >= CEPH_OBJECT_SIZE_LIMIT && hasWriteFailSocketException(ex)) {
                        abortTransaction(transactionID);
                        throw new ByteLimitExceededException("put exceeds size limit (" + CEPH_OBJECT_SIZE_LIMIT_MSG + ")", 
                                CEPH_OBJECT_SIZE_LIMIT);
                    }
                    if (trap.fail != null) { // read exception
                        // rollback to prev state
                        long len = metaObj.getContentLength();
                        
                        if (obj.exists()) {
                            log.debug("revert from " + len + " to " + prevLength + " after " + trap.fail);
                            obj.delete();
                            len = metaObj.getContentLength();
                            
                        } else {
                            log.debug("revert to " + len + "==" + prevLength + " not necessary after " + trap.fail);
                        }
                        md = MessageDigestAPI.getDigest(prevDigestState);
                        log.debug("proceeding with transaction " + transactionID + " at offset " + len + " after failed input: " + ex);
                    } else {
                        // write exception
                        log.debug("auto-abort transactionID " + transactionID + " after failed write to back end: " + ex);
                        abortTransaction(transactionID);
                        throw new WriteException("internal failure: " + ex);
                    }
                }
            } else {
                try {
                    if (newArtifact.contentChecksum != null && "MD5".equalsIgnoreCase(newArtifact.contentChecksum.getScheme())) {
                        up.setMd5(newArtifact.contentChecksum.getSchemeSpecificPart());
                    }
                    obj.uploadObject(up);
                    md = dis.getMessageDigest();
                } catch (Md5ChecksumException ex) {
                    //swift detected
                    throw new PreconditionFailedException("checksum mismatch: " + newArtifact.contentChecksum + " did not match content");
                } catch (CommandException ex) {
                    if (bcis.getByteCount() >= CEPH_OBJECT_SIZE_LIMIT && hasWriteFailSocketException(ex)) {
                        throw new ByteLimitExceededException("put exceeds size limit (" + CEPH_OBJECT_SIZE_LIMIT_MSG + ")", 
                                CEPH_OBJECT_SIZE_LIMIT);
                    }
                    if (trap.fail != null) {
                        throw new ReadException("read from input stream failed", trap.fail);
                    }
                    throw new WriteException("internal failure: " + ex);
                }
            }
                    
            // clone so we can persist the current state for resume
            String curDigestState = MessageDigestAPI.getEncodedState(md);
            Long curLength = metaObj.getContentLength();
            
            String csVal = HexUtil.toHex(md.digest());
            URI checksum = URI.create(checksumAlg.toLowerCase() + ":" + csVal);
            log.debug("current checksum: " + checksum);
            log.debug("current file size: " + curLength);
            
            if (txn != null) {
                if (txn.totalLength != null && curLength < txn.totalLength) {
                    // incomplete: no further content checks
                    // TODO: could check that curLength increased by exactly newArtifact.contentLength
                    log.debug("incomplete put in transaction: " + txn.getID() + " - not verifying checksum or length");
                } else {
                    // complete: do content checks
                    if (newArtifact.contentChecksum != null && !checksum.equals(newArtifact.contentChecksum)) {
                        // auto revert?
                        throw new PreconditionFailedException("checksum mismatch: " + newArtifact.contentChecksum + " != " + checksum);
                    }
                    if (txn.totalLength != null && !curLength.equals(txn.totalLength)) {
                        // auto revert?
                        throw new PreconditionFailedException("length mismatch: " + txn.totalLength + " != " + curLength);
                    }
                }
            } else {
                // no txn: single put
                if (newArtifact.contentChecksum != null && !checksum.equals(newArtifact.contentChecksum)) {
                    obj.delete();
                    throw new PreconditionFailedException("checksum mismatch: " + newArtifact.contentChecksum + " != " + checksum);
                }

                if (newArtifact.contentLength != null && !curLength.equals(newArtifact.contentLength)) {
                    obj.delete();
                    throw new PreconditionFailedException("length mismatch: " + newArtifact.contentLength + " != " + curLength);
                }
            }

            if (txn != null) {
                log.debug("transaction uncommitted: " + txn + " " + writeDataLocation);
                
                if (!txn.dynamicLargeObject) {
                    obj.setAndSaveMetadata(TRANSACTION_ATTR, "true");
                }
                
                txn.txnObject.setAndDoNotSaveMetadata(CUR_DIGEST_ATTR, curDigestState);
                txn.txnObject.setAndDoNotSaveMetadata(PREV_DIGEST_ATTR, prevDigestState);
                txn.txnObject.setAndDoNotSaveMetadata(ARTIFACT_ID_ATTR, newArtifact.getArtifactURI().toASCIIString());
                txn.txnObject.setAndDoNotSaveMetadata(CONTENT_CHECKSUM_ATTR, checksum.toASCIIString());
                txn.txnObject.saveMetadata();
                
                PutTransaction t = getTransactionStatus(transactionID);
                return t.storageMetadata;
            }

            // create this before committing the file so constraints applied
            StorageMetadata metadata = new StorageMetadata(writeDataLocation, checksum, curLength, obj.getLastModifiedAsDate());
            metadata.artifactURI = newArtifact.getArtifactURI();
            // contentLastModified assigned in commit
            StorageMetadata ret = commit(metadata, obj);
            return ret;
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("failed to create MessageDigestAPI: " + checksumAlg);
        } catch (CommandException ex) {
            throw new StorageEngageException("internal failure: " + ex, ex);
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

    // min segment size that does not increase number of segments
    long calcMinSegmentSize(long contentLength) {
        long num = (long) Math.ceil(((double)contentLength) / ((double) segmentMaxBytes));
        long minSeg = (long) Math.ceil(((double)contentLength) / ((double) num));
        return minSeg;
    }
    
    @Override
    public PutTransaction startTransaction(URI artifactURI, Long contentLength) 
            throws StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "artifactURI", artifactURI);
        try {
            UUID id = UUID.randomUUID();
            final String transactionID = id.toString();
            
            // calculate segmentation
            SwiftPutTransaction ret;
            if (contentLength == null) {
                // stream of unknown length: default to simple object
                ret = new SwiftPutTransaction(transactionID, segmentMaxBytes, segmentMaxBytes);
                
                // stream of unknown length: assume dynamic large object
                //ret.dynamicLargeObject = true;
            } else if (contentLength <= segmentMinBytes) {
                // normal object
                ret = new SwiftPutTransaction(transactionID, segmentMaxBytes, segmentMaxBytes);
            } else {
                // dynamic large object
                long minSeg = segmentMinBytes;
                if (segmentMinBytes == segmentMaxBytes) {
                    minSeg = calcMinSegmentSize(contentLength);
                }
                ret = new SwiftPutTransaction(transactionID, minSeg, segmentMaxBytes);
                ret.dynamicLargeObject = true;
            }
            ret.totalLength = contentLength;
            
            // TODO: accept non-default checksum algorithm for txn?
            MessageDigestAPI md = MessageDigestAPI.getInstance(DEFAULT_CHECKSUM_ALGORITHM);
            String digestState = MessageDigestAPI.getEncodedState(md);
            byte[] dig = md.digest();
            String csVal = HexUtil.toHex(dig);
            URI checksum = URI.create("md5:" + csVal);
            
            // create object to track txn status
            StoredObject txn = txnContainer.getObject("txn:" + transactionID);
            // need some bytes in getTransactionStatus object
            
            txn.uploadObject(dig);
            txn.setAndDoNotSaveMetadata(CUR_DIGEST_ATTR, digestState);
            txn.setAndDoNotSaveMetadata(ARTIFACT_ID_ATTR, artifactURI.toASCIIString());
            txn.setAndDoNotSaveMetadata(CONTENT_CHECKSUM_ATTR, checksum.toASCIIString());
            txn.setAndDoNotSaveMetadata(DLO_ATTR, ret.dynamicLargeObject);
            if (ret.getMinSegmentSize() != null) {
                txn.setAndDoNotSaveMetadata(PT_MIN_ATTR, ret.getMinSegmentSize().toString());
            }
            if (ret.getMaxSegmentSize() != null) {
                txn.setAndDoNotSaveMetadata(PT_MAX_ATTR, ret.getMaxSegmentSize().toString());
            }
            if (ret.totalLength != null) {
                txn.setAndDoNotSaveMetadata(TOTAL_LENGTH_ATTR, ret.totalLength.toString());
            }
            
            txn.saveMetadata();
            ret.txnObject = txn;

            if (ret.dynamicLargeObject) {
                // create large object manifest; each write (append) is a new object with this prefix
                StorageLocation mloc = toStorageLocation(id);
                log.debug("create manifest: " + mloc);
                Container sub = getContainerImpl(mloc);
                StoredObject mo = sub.getObject(mloc.getStorageID().toASCIIString());
                ObjectManifest manifest = new ObjectManifest(sub.getName() + "/" + mloc.getStorageID().toASCIIString());
                UploadInstructions up = new UploadInstructions(new byte[0]);
                up.setObjectManifest(manifest);
                mo.uploadObject(up);
                mo.setAndDoNotSaveMetadata(DLO_ATTR, ret.dynamicLargeObject);
                mo.setAndDoNotSaveMetadata(TRANSACTION_ATTR, true); // hide until commit
                mo.saveMetadata();
                ret.metaObject = mo;
            }
            log.debug("startTransaction: contentLength=" + contentLength + " " + ret);
            return ret;
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("BUG", ex);
        } catch (Exception ex) {
            throw new StorageEngageException("failed to create transaction", ex);
        } 
    }

    @Override
    public PutTransaction revertTransaction(String transactionID) 
            throws IllegalArgumentException, StorageEngageException, TransientException, UnsupportedOperationException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "transactionID", transactionID);
        UUID uuid = UUID.fromString(transactionID);
        try {
            log.debug("getTransactionStatus: " + transactionID);
            StoredObject txn = txnContainer.getObject("txn:" + transactionID);
            if (txn.exists()) {
                Map<String,Object> meta = txn.getMetadata();
                
                String prevDigestState = (String) meta.get(PREV_DIGEST_ATTR);
                if (prevDigestState == null) {
                    throw new IllegalArgumentException("transaction not revertable: " + transactionID);
                }

                // revert: remove the last part
                // manifest already exists
                UUID id = UUID.fromString(transactionID);
                StorageLocation manifest = toStorageLocation(id);
                Container sub = getContainerImpl(manifest);
                String prefix = manifest.getStorageID().toASCIIString() + ":p:";
                Iterator<StoredObject> parts = new LargeObjectPartIterator(sub, prefix);
                StoredObject last = null;
                while (parts.hasNext()) {
                    last = parts.next();
                }
                log.debug("delete part: " + last.getName());
                last.delete();
                
                String curDigestState = prevDigestState;
                MessageDigestAPI md = MessageDigestAPI.getDigest(curDigestState);
                String md5Val = HexUtil.toHex(md.digest());
                URI checksum = URI.create(md.getAlgorithmName() + ":" + md5Val);
                
                txn.setAndDoNotSaveMetadata(CUR_DIGEST_ATTR, curDigestState);
                txn.setAndDoNotSaveMetadata(CONTENT_CHECKSUM_ATTR, checksum.toASCIIString());
                txn.removeAndDoNotSaveMetadata(PREV_DIGEST_ATTR);
                txn.saveMetadata();
                
                return getTransactionStatusImpl(transactionID);
            }
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("BUG: failed to restore digest", ex);
        } catch (IllegalArgumentException | IllegalStateException ex) {
            throw new RuntimeException("BUG: invalid object: " + transactionID, ex);
        }
        
        throw new IllegalArgumentException("unknown transaction: " + transactionID);
    }
    
    @Override
    public StorageMetadata commitTransaction(String transactionID) 
            throws IllegalArgumentException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "transactionID", transactionID);
        try {
            SwiftPutTransaction pt = getTransactionStatusImpl(transactionID);
            
            StorageMetadata metadata = pt.storageMetadata;
            log.debug("txn status: " + pt + " " + metadata.getStorageLocation());
            log.debug("delete txn: " + pt.txnObject.getName());
            pt.txnObject.delete();
            StorageMetadata ret = commit(metadata, pt.metaObject);
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
            
            // force getting object last modified from server for consistency with iterator
            obj.reload();
            Date finalLastModified = obj.getLastModifiedAsDate();
            if (finalLastModified.equals(sm.getContentLastModified())) {
                log.debug("commit: lastModified **not changed** by attribute mod");
                return sm;
            }
            log.debug("commit: lastModified **changed** by attribute mod");
            StorageMetadata ret = new StorageMetadata(sm.getStorageLocation(), sm.getContentChecksum(), sm.getContentLength(), finalLastModified);
            ret.artifactURI = sm.artifactURI;
            return ret;
        } catch (CommandException ex) {
            throw new StorageEngageException("failed to persist attributes: " + sm.getStorageLocation(), ex);
        }
    }

    @Override
    public void abortTransaction(String transactionID) throws IllegalArgumentException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "transactionID", transactionID);
        // validate
        UUID uuid = UUID.fromString(transactionID);
        
        try {
            SwiftPutTransaction pt = getTransactionStatusImpl(transactionID);
            log.debug("delete txn: " + pt);
            pt.txnObject.delete();
            pt.txnObject = null;
            
            if (pt.metaObject != null) { // simple or manifest
                if (pt.dynamicLargeObject && pt.storageMetadata != null) {
                    Container sub = getContainerImpl(pt.storageMetadata.getStorageLocation());
                    String prefix = pt.metaObject.getName() + ":p:";
                    LargeObjectPartIterator iter = new LargeObjectPartIterator(sub, prefix);
                    while (iter.hasNext()) {
                        StoredObject part = iter.next();
                        log.debug("delete part: " + part.getName());
                        part.delete();
                    }
                }
                log.debug("delete obj: " + pt.metaObject.getName());
                pt.metaObject.delete();
                pt.metaObject = null;
            }
        } catch (CommandException ex) {
            throw new StorageEngageException("failed to abort transaction: " + transactionID, ex);
        }
    }

    @Override
    public PutTransaction getTransactionStatus(String transactionID) throws IllegalArgumentException, StorageEngageException, TransientException {
        return getTransactionStatusImpl(transactionID);
    }
    
    // enable internal calls to avoid cast
    private SwiftPutTransaction getTransactionStatusImpl(String transactionID) throws IllegalArgumentException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "transactionID", transactionID);
        UUID uuid = UUID.fromString(transactionID);
        try {
            log.debug("getTransactionStatus: " + transactionID);
            StoredObject txn = txnContainer.getObject("txn:" + transactionID);
            if (txn.exists()) {
                Map<String,Object> meta = txn.getMetadata();
                String smin = (String) meta.get(PT_MIN_ATTR);
                String smax = (String) meta.get(PT_MAX_ATTR);
                Long ptMin = null;
                if (smin != null) { 
                    ptMin = Long.parseLong(smin);
                }
                Long ptMax = null;
                if (smax != null) {
                    ptMax = Long.parseLong(smax);
                }
                
                final SwiftPutTransaction ret = new SwiftPutTransaction(transactionID, ptMin, ptMax);
                
                String sdlo = (String) meta.get(DLO_ATTR);
                ret.dynamicLargeObject = "true".equals(sdlo);
                
                String stot = (String) meta.get(TOTAL_LENGTH_ATTR);
                if (stot != null) {
                    ret.totalLength = Long.parseLong(stot);
                }
                ret.txnObject = txn;

                String scs = (String) meta.get(CONTENT_CHECKSUM_ATTR);
                String auri = (String) meta.get(ARTIFACT_ID_ATTR);
                URI md5 = new URI(scs);
                URI artifactURI = new URI(auri);
                    
                // need to get contentLength from the real object (simple or manifest)
                StorageLocation loc = toStorageLocation(uuid);
                Container sub = getContainerImpl(loc);
                StoredObject obj = sub.getObject(loc.getStorageID().toASCIIString());
                if (obj.exists()) {
                    ret.metaObject = obj;
                }
                log.debug("getTransactionStatus: " + ret);
                
                if (ret.metaObject != null) {
                    if (ret.metaObject.getContentLength() > 0L) {
                        ret.storageMetadata = toStorageMetadata(loc, md5, 
                                ret.metaObject.getContentLength(), 
                                artifactURI, 
                                ret.metaObject.getLastModifiedAsDate());
                    }
                }
                return ret;
            }
        } catch (IllegalArgumentException | IllegalStateException | URISyntaxException ex) {
            throw new RuntimeException("BUG: invalid object: " + transactionID, ex);
        }
        
        throw new IllegalArgumentException("unknown transaction: " + transactionID);
    }
    
    // used by SwiftPutTxnTest.cleanupBefore()
    Iterator<String> listTransactions() {
        return new TxnIterator(txnContainer.list().iterator()); // assume it's small
    }
    
    private class TxnIterator implements Iterator<String> {

        private final Iterator<StoredObject> cso;

        public TxnIterator(Iterator<StoredObject> cso) {
            this.cso = cso;
        }
        
        @Override
        public boolean hasNext() {
            return cso.hasNext();
        }

        @Override
        public String next() {
            StoredObject so = cso.next();
            URI uri = URI.create(so.getName());
            if (!"txn".equals(uri.getScheme())) {
                throw new IllegalStateException("unexpected object in txnContainer: " + so);
            }
            return uri.getSchemeSpecificPart();
        }
        
    }
    
    private boolean isIteratorVisible(StoredObject obj) {
        String val = (String) obj.getMetadata(TRANSACTION_ATTR);
        if ("true".equals(val)) {
            return false;
        }
        String name = obj.getName();
        if (name.contains(":p:")) {
            return false;
        }
        return true;
    }
    
    // get destination to write bytes to
    private StorageLocation getNextTransactionStorageLocation(String transactionID) 
            throws IllegalArgumentException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "transactionID", transactionID);
        
        // manifest already exists
        UUID id = UUID.fromString(transactionID);
        StorageLocation manifest = toStorageLocation(id);
        Container sub = getContainerImpl(manifest);
        String prefix = manifest.getStorageID().toASCIIString() + ":p:";
        Iterator<StoredObject> parts = new LargeObjectPartIterator(sub, prefix);
        String last = null;
        while (parts.hasNext()) {
            StoredObject p = parts.next();
            last = p.getName();
        }
        String objectName = getNextPartName(prefix, last);
        StorageLocation ret = new StorageLocation(URI.create(objectName));
        ret.storageBucket = manifest.storageBucket;
        log.debug("storageLocation for part: " + ret);
        return ret;
        
    }
    
    String getNextPartName(String prefix, String prev) {
        int p = 0;
        if (prev != null) {
            String s = prev.substring(prefix.length());
            p = Integer.parseInt(s);
        }
        p++;
        // %04d pads to 4 digits to max value is 9999
        if (p > 9999) {
            throw new IllegalArgumentException("LIMIT: reached limit of 9999 part names per object");
        }
        return String.format(prefix + "%04d", p);
        
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
            Container sub = getContainerImpl(storageLocation);
            String key = storageLocation.getStorageID().toASCIIString();
            StoredObject obj = sub.getObject(key);
            return obj.exists();
        } catch (Exception ex) {
            throw new StorageEngageException("failed to check exists: " + storageLocation, ex);
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
            Container sub = getContainerImpl(storageLocation);
            StoredObject obj = sub.getObject(key);
            if (obj.exists()) {
                String dloAttr = (String) obj.getMetadata(DLO_ATTR);
                if ("true".equals(dloAttr)) {
                    String prefix = obj.getName() + ":p:";
                    LargeObjectPartIterator iter = new LargeObjectPartIterator(sub, prefix);
                    while (iter.hasNext()) {
                        StoredObject part = iter.next();
                        log.debug("delete part: " + part.getName());
                        part.delete();
                    }
                }
                log.debug("delete object: " + obj.getName());
                obj.delete();
                return;
            }
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
                        if (isIteratorVisible(obj)) {
                            log.debug("MultiBucketStorageIterator.advance: next " + obj.getName() + " len=" + obj.getContentLength());
                            this.nextItem = objectToStorageMetadata(icurBucket, obj);
                            return; // nextItem staged
                        } else {
                            log.debug("MultiBucketStorageIterator.advance: skip " + obj.getName());
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
                        if (isIteratorVisible(obj)) {
                            log.debug("SingleBucketStorageIterator.advance: next " + obj.getName() + " len=" + obj.getContentLength());
                            this.nextItem = objectToStorageMetadata(null, obj);
                            return; // nextItem staged
                        } else {
                            log.debug("SingleBucketStorageIterator.advance: skip " + obj.getName());
                        }
                    }
                }
            }
        }
    }
    
    // iterator over all parts of a dynamic large object
    private class LargeObjectPartIterator implements Iterator<StoredObject> {
        
        private final Container swiftContainer;
        private final String prefix;
        private static final int BATCH_SIZE = 1024;
        private Iterator<StoredObject> objectIterator;
        
        private StoredObject nextItem;
        private String nextMarkerKey; // name of last item returned by next()

        public LargeObjectPartIterator(Container sub, String prefix) {
            this.swiftContainer = sub;
            this.prefix = prefix;
            advance();
        }

        @Override
        public boolean hasNext() {
            return (nextItem != null);
        }

        @Override
        public StoredObject next() {
            if (nextItem == null) {
                throw new NoSuchElementException();
            }
            
            StoredObject ret = nextItem;
            advance();
            return ret;
        }
        
        private void advance() {
            this.nextItem = null;
            while (true) {
                if (objectIterator == null || !objectIterator.hasNext()) {
                    Collection<StoredObject> list = swiftContainer.list(prefix, nextMarkerKey, BATCH_SIZE);
                    log.debug("LargeObjectPartIterator prefix=" + prefix + " size=" + list.size() + " from=" + nextMarkerKey);
                    if (!list.isEmpty()) {
                        objectIterator = list.iterator();
                    } else {
                        log.debug("LargeObjectPartIterator: DONE");
                        objectIterator = null;
                        return; // nextItem == null aka done
                    }
                }
                if (objectIterator != null) {
                    while (objectIterator.hasNext()) {
                        StoredObject obj = objectIterator.next();
                        this.nextMarkerKey = obj.getName();
                        log.debug("LargeObjectPartIterator.advance: next " + obj.getName() + " len=" + obj.getContentLength());
                        this.nextItem = obj;
                        return; // nextItem staged
                    }
                }
            }
        }
    }
}

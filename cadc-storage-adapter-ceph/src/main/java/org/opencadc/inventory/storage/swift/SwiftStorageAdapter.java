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
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.HexUtil;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.client.factory.TempUrlHashPrefixSource;
import org.javaswift.joss.exception.CommandException;
import org.javaswift.joss.exception.Md5ChecksumException;
import org.javaswift.joss.headers.object.range.MidPartRange;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.instructions.UploadInstructions;
import org.javaswift.joss.model.Access;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.StoredObject;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;
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
    
    private static final String CONFIG_FILENAME = "cadc-storage-adapter-ceph.properties";
    
    private static final String CONF_ENDPOINT = SwiftStorageAdapter.class.getName() + ".authEndpoint";
    private static final String CONF_USER = SwiftStorageAdapter.class.getName() + ".username";
    private static final String CONF_KEY = SwiftStorageAdapter.class.getName() + ".key";
    private static final String CONF_SBLEN = SwiftStorageAdapter.class.getName() + ".bucketLength";
    private static final String CONF_BUCKET = SwiftStorageAdapter.class.getName() + ".bucketName";

    static final String KEY_SCHEME = "sb"; // same scheme as the S3StorageAdapterSB for now
    static final int BUFFER_SIZE_BYTES = 8192;
    static final String DEFAULT_CHECKSUM_ALGORITHM = "md5";
    
    // ceph or swift does all kinds of mangling of metadata keys (char substitution, truncation, case changes)
    // javaswift lib seems to at least force keys in the Map to lower case to protect aginst some of it
    static final String ARTIFACT_ID_ATTR = "org.opencadc.artifactid";
    static final String CONTENT_CHECKSUM_ATTR = "org.opencadc.contentchecksum";
    
    private static final int CIRC_BUFFERS = 3;
    private static final int CIRC_BUFFERSIZE = 64 * 1024;

    protected final int storageBucketLength;
    protected final String storageBucket;
    protected final Account client;
    
    private final InternalBucket internalBucket;
    private Container swiftContainer;
    
    // ctor for unit tests that do not connect
    SwiftStorageAdapter(String storageBucket, int storageBucketLength) {
        this.storageBucket = storageBucket;
        this.storageBucketLength = storageBucketLength;
        this.client = null;
        this.internalBucket = new InternalBucket(storageBucket);
    }
    
    public SwiftStorageAdapter() {
        try {
            File config = new File(System.getProperty("user.home") + "/config/" + CONFIG_FILENAME);
            Properties props = new Properties();
            props.load(new FileReader(config));
            
            StringBuilder sb = new StringBuilder();
            sb.append("incomplete config: ");
            boolean ok = true;
            
            final String suri = props.getProperty(CONF_ENDPOINT);
            sb.append("\n\t" + CONF_ENDPOINT + ": ");
            if (suri == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }

            final String sbl = props.getProperty(CONF_SBLEN);
            sb.append("\n\t" + CONF_SBLEN + ": ");
            if (sbl == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }

            final String bn = props.getProperty(CONF_BUCKET);
            sb.append("\n\t" + CONF_BUCKET + ": ");
            if (bn == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }
            
            final String user = props.getProperty(CONF_USER);
            sb.append("\n\t" + CONF_USER + ": ");
            if (user == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }
            
            final String key = props.getProperty(CONF_KEY);
            sb.append("\n\t" + CONF_KEY + ": ");
            if (bn == null) {
                sb.append("MISSING");
                ok = false;
            } else {
                sb.append("OK");
            }
            
            if (!ok) {
                throw new IllegalStateException(sb.toString());
            }
           
            this.storageBucket = bn;
            this.storageBucketLength = Integer.parseInt(sbl);
            AccountConfig ac = new AccountConfig();

            // work around because uvic ceph auth returns wrong storage url
            //URL authURL = new URL(suri);
            //ac.setAccessProvider(new AccessProviderImpl(authURL, user, key));
            //ac.setAuthenticationMethod(AuthenticationMethod.EXTERNAL);
            
            ac.setAuthenticationMethod(AuthenticationMethod.BASIC);
            ac.setUsername(user);
            ac.setPassword(key);
            ac.setAuthUrl(suri);
            //ac.setTenantId("swift");
            
            this.client = new AccountFactory(ac).createAccount();
            
            this.internalBucket = new InternalBucket(storageBucket);
            init();
        } catch (Exception fatal) {
            throw new RuntimeException("invalid config", fatal);
        }
    }

    private static class AccessWorkaroundHack implements Access {

        private String token;
        private String storageURL;
        
        @Override
        public void setPreferredRegion(String string) {
            //ignore
        }

        @Override
        public String getToken() {
            return token;
        }

        @Override
        public String getInternalURL() {
            return null;
        }

        @Override
        public String getPublicURL() {
            return storageURL;
        }

        @Override
        public boolean isTenantSupplied() {
            return true; // via {username}:{tenant}
        }

        @Override
        public String getTempUrlPrefix(TempUrlHashPrefixSource tuhps) {
            return null;
        }
        
    }
    
    private void init() {
        this.swiftContainer = client.getContainer(internalBucket.name);
        if (swiftContainer.exists()) {
            log.debug("container: " + swiftContainer.getName() + " [exists]");
            return;
        }
        swiftContainer.create();
        log.debug("container: " + swiftContainer.getName() + " [created]");
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
    
    // internal format for object identifier is sb:{hex}:{uuid} and we re-use hex as the storageBucket
    StorageLocation generateStorageLocation() {
        String id = UUID.randomUUID().toString();
        String pre = InventoryUtil.computeBucket(URI.create(id), storageBucketLength);
        StorageLocation ret = new StorageLocation(URI.create(KEY_SCHEME + ":" + pre + ":" + id));
        ret.storageBucket = pre;
        return ret;
    }
    
    InternalBucket toInternalBucket(StorageLocation loc) {
        return internalBucket;
    }
    
    protected StorageLocation toExternal(InternalBucket bucket, String key) {
        // ignore bucket
        // extract scheme from the key
        String[] parts = key.split(":");
        if (parts.length != 3 || !KEY_SCHEME.equals(parts[0])) {
            throw new IllegalStateException("invalid object key: " + key + " from bucket: " + bucket);
        }
        StorageLocation ret = new StorageLocation(URI.create(key));
        ret.storageBucket = parts[1];
        return ret;
    }
    
    private StorageMetadata toStorageMetadata(StorageLocation loc, URI md5, long contentLength, 
            final URI artifactURI, Date lastModified) {
        StorageMetadata storageMetadata = new StorageMetadata(loc, md5, contentLength);
        storageMetadata.artifactURI = artifactURI;
        storageMetadata.contentLastModified = lastModified;
        return storageMetadata;
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
        //String bucket = toInternalBucket(storageLocation).name;
        String key = storageLocation.getStorageID().toASCIIString();
        StoredObject obj = swiftContainer.getObject(key);
        if (!obj.exists()) {
            throw new ResourceNotFoundException("not found: " + storageLocation);
        }
        
        try (final InputStream source = obj.downloadObjectAsInputStream()) {
            //ThreadedIO tio = new ThreadedIO(BUFFER_SIZE_BYTES, 8);
            //tio.ioLoop(dest, inputStream);
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
        //String bucket = toInternalBucket(storageLocation).name;
        String key = storageLocation.getStorageID().toASCIIString();
        StoredObject obj = swiftContainer.getObject(key);
        if (!obj.exists()) {
            throw new ResourceNotFoundException("not found: " + storageLocation);
        }
        
        DownloadInstructions di = new DownloadInstructions();
        if (!byteRanges.isEmpty()) {
            Iterator<ByteRange> iter = byteRanges.iterator();
            ByteRange br = iter.next();
            if (iter.hasNext()) {
                throw new UnsupportedOperationException("multiple byte ranges not supported");
            }
            
            long endPos = br.getOffset() + br.getLength() - 1L;
            di.setRange(new MidPartRange((int) br.getOffset(), (int) endPos)); // HACK: javaswift API uses int!!!
        }
        try (final InputStream source = obj.downloadObjectAsInputStream(di)) {
            //ThreadedIO tio = new ThreadedIO(BUFFER_SIZE_BYTES, 8);
            //tio.ioLoop(dest, inputStream);
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
     * Get from storage the artifact identified by storageLocation with cutout specifications. Currently needs full
     * implementation.
     *
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest The destination stream.
     * @param cutouts Cutouts to be applied to the artifact
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If writing failed.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     */
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest, Set<String> cutouts)
            throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException {
        throw new UnsupportedOperationException();
    }

    // internal bucket management used for init, dynamic buckets, and intTest cleanup
    void createBucket(InternalBucket bucket) throws ResourceAlreadyExistsException {
        throw new UnsupportedOperationException();
    }
    
    void deleteBucket(InternalBucket bucket) throws ResourceNotFoundException {
        throw new UnsupportedOperationException();
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
     * @return storage metadata after write
     * @throws ca.nrc.cadc.io.ByteLimitExceededException
     *
     * @throws IncorrectContentChecksumException checksum of the data stream did not match the value in newArtifact
     * @throws IncorrectContentLengthException number bytes read did not match the value in newArtifact
     * @throws ReadException If the client failed to read the stream.
     * @throws WriteException If the storage system failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     */
    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source)
            throws ByteLimitExceededException, 
            IncorrectContentChecksumException, IncorrectContentLengthException, 
            ReadException, WriteException,
            StorageEngageException {
        InventoryUtil.assertNotNull(SwiftStorageAdapter.class, "newArtifact", newArtifact);
        log.debug("put: " + newArtifact);
        
        if (newArtifact.contentLength != null && newArtifact.contentLength > CEPH_UPLOAD_LIMIT) {
            throw new ByteLimitExceededException("put exceeds size limit (" + CEPH_UPLOAD_LIMIT_MSG + ") for simple stream", CEPH_UPLOAD_LIMIT);
        }
        
        final StorageLocation loc = generateStorageLocation();

        try {
            String alg = DEFAULT_CHECKSUM_ALGORITHM;
            if (newArtifact.contentChecksum != null) {
                alg = newArtifact.contentChecksum.getScheme(); // TODO: try sha1 in here
            }
            TrapFailInputStream trap = new TrapFailInputStream(source);
            ByteCountInputStream bcis = new ByteCountInputStream(trap);
            DigestInputStream dis = new DigestInputStream(bcis, MessageDigest.getInstance(DEFAULT_CHECKSUM_ALGORITHM));
            

            StoredObject obj = swiftContainer.getObject(loc.getStorageID().toASCIIString());
            UploadInstructions up = new UploadInstructions(dis);
            if (newArtifact.contentChecksum != null && "MD5".equalsIgnoreCase(newArtifact.contentChecksum.getScheme())) {
                up.setMd5(newArtifact.contentChecksum.getSchemeSpecificPart());
            }

            try {
                obj.uploadObject(up);
            } catch (CommandException ex) {
                if (bcis.getByteCount() >= CEPH_UPLOAD_LIMIT && hasWriteFailSocketException(ex)) {
                    throw new ByteLimitExceededException("put exceeds size limit (" + CEPH_UPLOAD_LIMIT_MSG + ") for simple stream", CEPH_UPLOAD_LIMIT);
                }
                if (trap.fail != null) {
                    throw new ReadException("read from input stream failed", trap.fail);
                }
                throw ex;
            }
            
            //String etag = obj.getEtag();
            //String slm = obj.getLastModified();
            //log.debug("after upload: etag=" + etag + " len=" + obj.getContentLength());
                    
            final URI contentChecksum = URI.create(DEFAULT_CHECKSUM_ALGORITHM + ":" + HexUtil.toHex(dis.getMessageDigest().digest()));
            
            if (newArtifact.contentChecksum != null && !contentChecksum.equals(newArtifact.contentChecksum)) {
                obj.delete();
                throw new PreconditionFailedException("checksum mismatch: " + newArtifact.contentChecksum + " != " + contentChecksum);
            }
            final Long contentLength = obj.getContentLength();
            if (newArtifact.contentLength != null && !contentLength.equals(newArtifact.contentLength)) {
                obj.delete();
                throw new PreconditionFailedException("length mismatch: " + newArtifact.contentLength + " != " + contentLength);
            }
           
            final Date lastModified = obj.getLastModifiedAsDate();
            
            Map<String,Object> metadata = new TreeMap<>();
            metadata.put(ARTIFACT_ID_ATTR, newArtifact.getArtifactURI().toASCIIString());
            metadata.put(CONTENT_CHECKSUM_ATTR, contentChecksum.toASCIIString());
            obj.setMetadata(metadata);
            //obj.saveMetadata();
            
            return toStorageMetadata(loc, contentChecksum, contentLength, newArtifact.getArtifactURI(), lastModified);
        } catch (Md5ChecksumException ex) {
            //swift detected
            throw new PreconditionFailedException("checksum mismatch: " + newArtifact.contentChecksum + " did not match content");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("failed to create MessageDigest: " + DEFAULT_CHECKSUM_ALGORITHM);
        }
    }
    
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
    
    // method used in integration tests to verify put
    StorageMetadata getMeta(StorageLocation storageLocation)
        throws ResourceNotFoundException, StorageEngageException {
        log.debug("getMeta: " + storageLocation);
        String key = storageLocation.getStorageID().toASCIIString();
        StoredObject obj = swiftContainer.getObject(key);
        if (!obj.exists()) {
            throw new ResourceNotFoundException("not found: " + storageLocation);
        }
        return objectToStorageMetadata(obj);
    }
    
    // also used by iterator
    private StorageMetadata objectToStorageMetadata(StoredObject obj) {
        final StorageLocation loc = toExternal(internalBucket, obj.getName());
        try {
            Map<String,Object> meta = obj.getMetadata();
            String scs = (String) meta.get(CONTENT_CHECKSUM_ATTR);
            String auri = (String) meta.get(ARTIFACT_ID_ATTR);

            if (scs == null || auri == null) {
                // put failed to set metadata after writing the file: invalid
                log.warn("found invalid: " + loc);
                return new StorageMetadata(new StorageLocation(URI.create("invalid:" + obj.getName())));
            }

            URI md5 = URI.create(scs);
            URI artifactURI = URI.create(auri);

            return toStorageMetadata(loc, md5, obj.getContentLength(), artifactURI, obj.getLastModifiedAsDate());
        } catch (IllegalStateException ex) {
            // this form is sufficient be used for delete
            log.warn("found invalid: " + loc, ex);
            return new StorageMetadata(new StorageLocation(URI.create("invalid:" + obj.getName())));
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
        String key = storageLocation.getStorageID().toASCIIString();
        try {
            StoredObject obj = swiftContainer.getObject(key);
            return obj.exists();
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
            StoredObject obj = swiftContainer.getObject(key);
            if (obj.exists()) {
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
        return new StorageMetadataIterator(bucketPrefix);
    }

    private class StorageMetadataIterator implements Iterator<StorageMetadata> {

        private static final int BATCH_SIZE = 1000;
        private Iterator<DirectoryOrObject> objectIterator;
        private final String bucketPrefix;
        
        private String nextMarkerKey; // name of last item returned by next()
        private boolean done = false;

        public StorageMetadataIterator(String bucketPrefix) {
            if (bucketPrefix != null) {
                this.bucketPrefix = KEY_SCHEME + ":" + bucketPrefix;
            } else {
                this.bucketPrefix = null;
            }
        }
        
        @Override
        public boolean hasNext() {
            if (done) {
                return false;
            }
            
            if (objectIterator == null || !objectIterator.hasNext()) {
                Collection<DirectoryOrObject> list = swiftContainer.listDirectory(bucketPrefix, '/', nextMarkerKey, BATCH_SIZE);
                log.debug("StorageMetadataIterator bucketPrefix: " + bucketPrefix + " size: " + list.size() + " from " + nextMarkerKey);
                if (!list.isEmpty()) {
                    objectIterator = list.iterator();
                } else {
                    objectIterator = null;
                    done = true;
                    log.debug("StorageMetadataIterator: done");
                }
            }

            if (objectIterator == null) {
                return false;
            }

            return objectIterator.hasNext();
        }

        @Override
        public StorageMetadata next() {
            if (objectIterator == null || !objectIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            DirectoryOrObject o = objectIterator.next();
            StoredObject obj = o.getAsObject();
            log.debug("next: name=" + obj.getName() + " len=" + obj.getContentLength());
            this.nextMarkerKey = obj.getName();
            
            return objectToStorageMetadata(obj);
        }
        
    }
}

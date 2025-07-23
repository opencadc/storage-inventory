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

package org.opencadc.inventory.storage.eos;

import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.MultiBufferIO;
import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.IncorrectContentChecksumException;
import ca.nrc.cadc.net.IncorrectContentLengthException;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
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
 * StorageAdapter implementation for the EOS file storage system.
 *
 * @author pdowler
 */
public class EosStorageAdapter implements StorageAdapter {

    private static final Logger log = Logger.getLogger(EosStorageAdapter.class);

    static final String CONFIG_FILE = "cadc-storage-adapter-eos.properties";
    // https://eos-mgm.keel-dev.arbutus.cloud:8443/eos/keel-dev.arbutus.cloud/data/lsst/users/fabio/hello.txt?authz=$EOSAUTHZ

    static final String CONFIG_PROPERTY_MGM_SRV = EosStorageAdapter.class.getName() + ".mgmServer";
    static final String CONFIG_PROPERTY_MGM_PATH = EosStorageAdapter.class.getName() + ".mgmServerPath";
    static final String CONFIG_PROPERTY_MGM_HTTPS_PORT = EosStorageAdapter.class.getName() + ".mgmHttpsPort";
    static final String CONFIG_PROPERTY_TOKEN = EosStorageAdapter.class.getName() + ".authToken";
    static final String CONFIG_PROPERTY_SCHEME = EosStorageAdapter.class.getName() + ".artifactScheme";

    private static final int CIRC_BUFFERS = 3;
    private static final int CIRC_BUFFERSIZE = 64 * 1024;
    
    private final URI mgmServer;
    private final String mgmPath;
    private final URL mgmBaseURL;
    private final String authToken;
    private final String artifactScheme;

    /**
     * Standard constructor for dynamic loading and operational use.
     */
    public EosStorageAdapter() {
        PropertiesReader pr = new PropertiesReader(CONFIG_FILE);
        MultiValuedProperties mvp = pr.getAllProperties();

        String srv = mvp.getFirstPropertyValue(CONFIG_PROPERTY_MGM_SRV);
        if (srv == null) {
            throw new InvalidConfigException("failed to load " + CONFIG_PROPERTY_MGM_SRV
                    + " from " + CONFIG_FILE);
        }

        String path = mvp.getFirstPropertyValue(CONFIG_PROPERTY_MGM_PATH);
        if (path == null) {
            throw new InvalidConfigException("failed to load " + CONFIG_PROPERTY_MGM_PATH
                    + " from " + CONFIG_FILE);
        }

        String port = mvp.getFirstPropertyValue(CONFIG_PROPERTY_MGM_HTTPS_PORT);
        if (port == null) {
            throw new InvalidConfigException("failed to load " + CONFIG_PROPERTY_MGM_HTTPS_PORT
                    + " from " + CONFIG_FILE);
        }

        String tok = mvp.getFirstPropertyValue(CONFIG_PROPERTY_TOKEN);
        if (tok == null) {
            throw new InvalidConfigException("failed to load " + CONFIG_PROPERTY_TOKEN
                    + " from " + CONFIG_FILE);
        }

        String as = mvp.getFirstPropertyValue(CONFIG_PROPERTY_SCHEME);
        if (as == null || as.isEmpty()) {
            throw new InvalidConfigException("failed to load " + CONFIG_PROPERTY_SCHEME
                    + " from " + CONFIG_FILE);
        }

        this.mgmPath = path;
        try {
            this.mgmServer = new URI(srv);
        } catch (URISyntaxException ex) {
            throw new InvalidConfigException("invalid " + CONFIG_PROPERTY_MGM_SRV + ": " + srv, ex);
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("https://").append(mgmServer.getAuthority());
        if (port != null) {
            sb.append(":").append(port);
        }
        sb.append(path);
        String surl = sb.toString();
        try {
            this.mgmBaseURL = new URL(surl);
        } catch (MalformedURLException ex) {
            throw new InvalidConfigException("failed to construct https URL: " + surl, ex);
        }
        
        if (tok.startsWith("zteos64:")) {
            this.authToken = tok;
        } else {
            throw new InvalidConfigException("invalid " + CONFIG_PROPERTY_TOKEN + ": expected zteos64:{encoded}");
        }
        
        this.artifactScheme = as;
        
        log.warn("mgmServer: " + mgmServer);
        log.warn("mgmPath: " + mgmPath);
        log.warn("mgm http: " + mgmBaseURL);
        log.warn("artifact scheme: " + artifactScheme);
        log.warn("authToken: REDACTED");
    }

    @Override
    public BucketType getBucketType() {
        return BucketType.PATH;
    }

    @Override
    public void get(StorageLocation storageLocation, OutputStream dest)
            throws InterruptedException, ResourceNotFoundException,
            ReadException, WriteException, StorageEngageException, TransientException {
        get(storageLocation, dest, null);
    }

    @Override
    public void get(StorageLocation storageLocation, OutputStream dest, ByteRange byteRange)
            throws InterruptedException, ResourceNotFoundException,
            ReadException, WriteException, StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(EosStorageAdapter.class, "storageLocation", storageLocation);
        InventoryUtil.assertNotNull(EosStorageAdapter.class, "dest", dest);
        log.debug("get: " + storageLocation);

        StringBuilder sb = new StringBuilder();
        sb.append(mgmBaseURL);
        sb.append("/").append(storageLocation.storageBucket);
        sb.append("/").append(storageLocation.getStorageID().toASCIIString());
        sb.append("?authz=").append(authToken); // no url-encode required
        String surl = sb.toString();
        log.warn("get: " + surl);
        
        String rangeRequest = null;
        if (byteRange != null) {
            long a = byteRange.getOffset();
            long b = byteRange.getOffset() + byteRange.getLength() - 1;
            rangeRequest = "bytes=" + a + "-" + b;
        }
        InputStream source = null;
        try {
            URL url = new URL(sb.toString());
            HttpGet get = new HttpGet(url, true);
            if (rangeRequest != null) {
                get.setRequestProperty("Range", rangeRequest);
            }
            get.prepare();
            if (get.getResponseCode() == 307) {
                // temporary redirect not supported by HttpGet/HttpTransfer or core java lib
                String location = get.getResponseHeader("Location");
                if (location == null) {
                    throw new StorageEngageException("GET return a 307 with no location header: " + mgmBaseURL);
                }
                url = new URL(location);
                get = new HttpGet(url, true);
                if (rangeRequest != null) {
                    get.setRequestProperty("Range", rangeRequest);
                }
                get.prepare();
            }
            source = get.getInputStream();
        } catch (ByteLimitExceededException | ResourceAlreadyExistsException ignore) {
            log.debug("ignore exception: " + ignore);
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG: generated invalid URL " + surl + "?AUTHZ=REDACTED");
        } catch (IOException ex) {
            throw new StorageEngageException("cannot access " + mgmBaseURL);
        } 

        MultiBufferIO io = new MultiBufferIO(CIRC_BUFFERS, CIRC_BUFFERSIZE);
        try {
            io.copy(source, dest);
        } catch (InterruptedException ex) {
            log.debug("get interrupted", ex);
        }
    }

    @Override
    public Iterator<StorageMetadata> iterator()
            throws StorageEngageException, TransientException {
        EosFind find = new EosFind(mgmServer, mgmPath, authToken, artifactScheme);
        find.start();
        log.warn("iterator.hasNext(): " + find.hasNext());
        return find;
    }

    @Override
    public Iterator<StorageMetadata> iterator(String storageBucketPrefix)
            throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<StorageMetadata> iterator(String storageBucketPrefix, boolean includeRecoverable)
            throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    // read-only implementation -- everything else unsupported

    @Override
    public void setRecoverableNamespaces(List<Namespace> preserved) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Namespace> getRecoverableNamespaces() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPurgeNamespaces(List<Namespace> purged) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Namespace> getPurgeNamespaces() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source, String transactionID) 
            throws ByteLimitExceededException, IllegalArgumentException, InterruptedException,
            IncorrectContentChecksumException, IncorrectContentLengthException, 
            ReadException, WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(StorageLocation storageLocation) 
            throws ResourceNotFoundException, IOException, InterruptedException,
            StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(StorageLocation storageLocation, boolean includeRecoverable) 
            throws ResourceNotFoundException, IOException, InterruptedException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void recover(StorageLocation storageLocation, Date contentLastModified) 
            throws ResourceNotFoundException, IOException, InterruptedException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PutTransaction startTransaction(URI artifactURI, Long contentLength) 
            throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PutTransaction revertTransaction(String transactionID) 
            throws IllegalArgumentException, StorageEngageException, TransientException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageMetadata commitTransaction(String transactionID) 
            throws IllegalArgumentException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abortTransaction(String transactionID) 
            throws IllegalArgumentException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PutTransaction getTransactionStatus(String transactionID) 
            throws IllegalArgumentException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<PutTransaction> transactionIterator() 
            throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }
}

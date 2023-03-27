/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2022=3.                            (c) 2023.
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
 *  $Revision: 4 $
 *
 ************************************************************************
 */

package org.opencadc.inventory.storage.ad;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.MultiBufferIO;
import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.IncorrectContentChecksumException;
import ca.nrc.cadc.net.IncorrectContentLengthException;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.RangeNotSatisfiableException;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferParsingException;
import ca.nrc.cadc.vos.TransferReader;
import ca.nrc.cadc.vos.TransferWriter;
import ca.nrc.cadc.vos.VOS;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.AccessControlException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import javax.security.auth.Subject;
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
import org.opencadc.tap.TapClient;

/**
 * The interface to storage implementations.
 * 
 * @author jeevesh
 *
 */
public class AdStorageAdapter implements StorageAdapter {

    private static final Logger log = Logger.getLogger(AdStorageAdapter.class);
    private static final URI DATA_RESOURCE_ID = URI.create("ivo://cadc.nrc.ca/data");
    private static final String TAP_SERVICE_URI = "ivo://cadc.nrc.ca/ad";

    // cache for repeated ByteRange requests
    private StorageLocation storageLoc;
    private URL storageURL;
    
    /**
     * Construct an AdStorageAdapter with the config stored in the
     * well-known properties file with well-known properties.
     */
    public AdStorageAdapter(){}

    @Override
    public BucketType getBucketType() {
        return BucketType.PLAIN;
    }

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
    public void get(StorageLocation storageLocation, OutputStream dest)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        get(storageLocation, dest, null);
    }

    @Override
    public void get(StorageLocation storageLocation, OutputStream dest, ByteRange byteRange)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        

        try {
            boolean auth = CredUtil.checkCredentials();
            log.debug("authenticated: " + auth);
        } catch (CertificateExpiredException | CertificateNotYetValidException e) {
            throw new AccessControlException("permission denied -- reason: delegated X509 Certificate is invalid");
        }

        try {
            URL sourceURL;
            if (byteRange != null && storageLoc != null && storageLoc.equals(storageLocation)) {
                // use cached value
                sourceURL = storageURL;
                log.debug("cached URL: " + sourceURL);
            } else {
                this.storageLoc = null;
                this.storageURL = null;
                sourceURL = this.toURL(storageLocation.getStorageID());
                log.debug("negotiated URL: " + sourceURL);
                // cache for next request
                this.storageLoc = storageLocation;
                this.storageURL = sourceURL;
            }
        
            boolean followRedirects = true;
            HttpGet get = new HttpGet(sourceURL, followRedirects);
            if (byteRange != null) {
                long end = byteRange.getOffset() + byteRange.getLength() - 1;
                String rval = "bytes=" + byteRange.getOffset() + "-" + end;
                get.setRequestProperty("range", rval);
            }
            get.prepare();
            String hexMD5 = get.getContentMD5();
            if (storageLocation.expectedContentChecksum != null && hexMD5 != null) {
                if (!hexMD5.equals(storageLocation.expectedContentChecksum.getSchemeSpecificPart())) {
                    throw new PreconditionFailedException("file changed in AD and contentChecksum no longer matches");
                }
            }
            MultiBufferIO tio = new MultiBufferIO();
            tio.copy(get.getInputStream(), dest);

        } catch (ByteLimitExceededException | ResourceAlreadyExistsException | RangeNotSatisfiableException unexpected) {
            log.debug("error type: " + unexpected.getClass());
            throw new RuntimeException(unexpected.getMessage());
        } catch (InterruptedException | IOException ie) {
            throw new TransientException(ie.getMessage());
        }
    }
    
    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source, String transactionID)
        throws IncorrectContentChecksumException, IncorrectContentLengthException, ReadException,
            WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException("not supported");
    }
        
    @Override
    public void delete(StorageLocation storageLocation)
        throws ResourceNotFoundException, IOException, InterruptedException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void delete(StorageLocation storageLocation, boolean includeRecoverable) 
        throws ResourceNotFoundException, IOException, InterruptedException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void recover(StorageLocation storageLocation, Date contentLastModified) 
        throws ResourceNotFoundException, IOException, InterruptedException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException("not supported");
    }
    
    @Override
    public PutTransaction startTransaction(URI uri, Long contentLength) throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageMetadata commitTransaction(String string) throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abortTransaction(String string) throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PutTransaction revertTransaction(String transactionID) 
            throws IllegalArgumentException, StorageEngageException, TransientException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public PutTransaction getTransactionStatus(String string) throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Iterator<StorageMetadata> iterator()
        throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException("not supported");
    }
    
    @Override
    public Iterator<StorageMetadata> iterator(String storageBucket)
        throws StorageEngageException, TransientException {
        InventoryUtil.assertNotNull(AdStorageQuery.class, "storageBucket", storageBucket);
        if (!StringUtil.hasLength(storageBucket)) {
            throw new IllegalArgumentException("Archive name must be specified: " + storageBucket);
        }

        log.debug("storage bucket: " + storageBucket);
        TapClient tc = null;
        try {
            tc = new TapClient(URI.create(TAP_SERVICE_URI));
            tc.setConnectionTimeout(12000); // 12 sec
            tc.setReadTimeout(20 * 60000); // 20 min
        } catch (ResourceNotFoundException rnfe) {
            throw new StorageEngageException("Unable to connect to tap client: ", rnfe);
        }
        AdStorageQuery adQuery = new AdStorageQuery(storageBucket);
        Iterator<StorageMetadata> storageMetadataIterator = null;

        try {
            String adql = adQuery.getQuery();
            log.debug("bucket: " + storageBucket + " query: " + adql);
            storageMetadataIterator = tc.query(adql, adQuery.getRowMapper(), true);
        } catch (Exception e) {
            log.error("error executing TapClient query");

            throw new TransientException(e.getMessage());
        }

        // AdStorageIterator ensures duplicates don't get sent out
        return new AdStorageIterator(storageMetadataIterator);
    }

    @Override
    public Iterator<StorageMetadata> iterator(String storageBucket, boolean includeRecoverable) throws StorageEngageException, TransientException {
        if (includeRecoverable) {
            throw new UnsupportedOperationException();
        }
        return iterator(storageBucket);
    }

    @Override
    public Iterator<PutTransaction> transactionIterator() throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }
    
    // negotiate a URL so we can potentially re-use it for multiple ByteRange requests
    private URL toURL(URI uri) throws AccessControlException, NotAuthenticatedException,
            ByteLimitExceededException, ResourceNotFoundException, TransientException {
        try {
            Subject subject = AuthenticationUtil.getCurrentSubject();
            AuthMethod authMethod = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            if (authMethod == null) {
                authMethod = AuthMethod.ANON;
            }
            RegistryClient rc = new RegistryClient();
            Capabilities caps = rc.getCapabilities(DATA_RESOURCE_ID);
            if (caps == null) {
                throw new RuntimeException("OOPS - " + DATA_RESOURCE_ID + " not found");
            }
            
            /*
            Capability negotiate = caps.findCapability(Standards.VOSPACE_SYNC_21);
            if (negotiate == null) {
                throw new RuntimeException("OOPS - " + DATA_RESOURCE_ID + " does not support transfer negotiation");
            }
            Interface ifc = negotiate.findInterface(authMethod);
            if (ifc == null) {
                throw new RuntimeException("OOPS - no interface for auth method " + authMethod);
            }
            URL srcURL = ifc.getAccessURL().getURL();
            */
            
            // caps is for the new read-only data2 service, so we have to fake it a bit
            Capability cap = caps.findCapability(Standards.VOSI_CAPABILITIES);
            Interface ifc = cap.findInterface(AuthMethod.ANON);
            URL furl = ifc.getAccessURL().getURL();
            final URL srcURL = new URL(furl.toExternalForm().replace("/capabilities", "/transfer"));
            
            Transfer request = new Transfer(uri, Direction.pullFromVoSpace);
            request.version = VOS.VOSPACE_21;
            request.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTPS_GET));
            TransferWriter writer = new TransferWriter();
            StringWriter out = new StringWriter();
            writer.write(request, out);
            String req = out.toString();
            log.debug("request:\n" + req);

            FileContent content = new FileContent(req, "text/xml", Charset.forName("UTF-8"));
            HttpPost post = new HttpPost(srcURL, content, true);
            post.prepare();
            
            InputStream istream = post.getInputStream();
            String xml = StringUtil.readFromInputStream(istream, "UTF-8");
            log.debug("response:\n" + xml);

            TransferReader reader = new TransferReader();
            Transfer t = reader.read(xml, null);
            if (t != null && !t.getProtocols().isEmpty()) {
                Protocol p = t.getProtocols().get(0);
                if (p.getEndpoint() != null) {
                    return new URL(p.getEndpoint());
                }
            }
            
            throw new RuntimeException("OOPS - failed to get transfer URL for " + uri);
            
        } catch (MalformedURLException | ResourceAlreadyExistsException ex) {
            throw new RuntimeException("BUG", ex);
        } catch (IOException | TransferParsingException ex) {
            throw new TransientException("failed to negotiate data URL", ex);
        } catch (InterruptedException ex) {
            throw new RuntimeException("interrupted", ex);
        }
    }

}

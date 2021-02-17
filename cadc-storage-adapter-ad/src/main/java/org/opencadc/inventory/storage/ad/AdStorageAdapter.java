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
 *  $Revision: 4 $
 *
 ************************************************************************
 */

package org.opencadc.inventory.storage.ad;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.cred.client.CredUtil;
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
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.util.Iterator;
import java.util.SortedSet;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.NewArtifact;
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

    /**
     * Construct an AdStorageAdapter with the config stored in the
     * well-known properties file with well-known properties.
     */
    public AdStorageAdapter(){}

    /**
     * Get the artifact identified by storageLocation from storage.
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
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {

        URL sourceURL = this.toURL(storageLocation.getStorageID());
        log.debug("sourceURL: " + sourceURL.toString());

        try {
            boolean auth = CredUtil.checkCredentials();
            log.debug("authenticated: " + auth);
        } catch (CertificateExpiredException | CertificateNotYetValidException e) {
            throw new AccessControlException("permission denied -- reason: delegated X509 Certificate is invalid");
        }

        try {
            boolean followRedirects = true;
            HttpGet get = new HttpGet(sourceURL, followRedirects);
            get.prepare();
            MultiBufferIO tio = new MultiBufferIO();
            tio.copy(get.getInputStream(), dest);

        } catch (ByteLimitExceededException | ResourceAlreadyExistsException unexpected) {
            log.debug("error type: " + unexpected.getClass());
            throw new RuntimeException(unexpected.getMessage());
        } catch (InterruptedException | IOException ie) {
            throw new TransientException(ie.getMessage());
        }
    }

    /**
     * Get the artifact identified by storageLocation from storage.
     * 
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest The destination stream.
     * @param byteRanges set of byte ranges to read
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If the client failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    @Override
    public void get(StorageLocation storageLocation, OutputStream dest, SortedSet<ByteRange> byteRanges)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException("not supported");
    }
    
    /**
     * Write not supported.
     * 
     * @param newArtifact The holds information about the incoming artifact.  If the contentChecksum
     *     and contentLength are set, they will be used to validate the bytes received.
     * @param source The stream from which to read.
     * @param transactionID null for auto-commit or existing transactionID 
     * @return The storage metadata.
     * 
     * @throws IncorrectContentChecksumException If the calculated checksum does not the expected
     *     checksum as described in newArtifact.
     * @throws IncorrectContentLengthException If the calculated length does not the expected
     *     length as described in newArtifact.
     * @throws ReadException If the client failed to stream.
     * @throws WriteException If the storage system failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred.
     */
    @Override
    public StorageMetadata put(NewArtifact newArtifact, InputStream source, String transactionID)
        throws IncorrectContentChecksumException, IncorrectContentLengthException, ReadException,
            WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException("not supported");
    }
        
    /**
     * Delete not supported.
     * 
     * @param storageLocation Identifies the artifact to delete.
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException If an unrecoverable error occurred.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    @Override
    public void delete(StorageLocation storageLocation)
        throws ResourceNotFoundException, IOException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public String startTransaction(URI uri) throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageMetadata commitTransaction(String string) throws ResourceNotFoundException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abortTransaction(String string) throws ResourceNotFoundException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageMetadata getTransactionStatus(String string) throws ResourceNotFoundException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Complete iterator not supported.
     * @return An iterator over an ordered list of items in storage.
     * 
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    @Override
    public Iterator<StorageMetadata> iterator()
        throws StorageEngageException, TransientException {
        throw new UnsupportedOperationException("not supported");
    }
    
    /**
     * Iterator of items ordered by their storageIDs in the given bucket.
     * @param storageBucket Only iterate over items in this bucket.
     * @return An iterator over an ordered list of items in this storage bucket.
     * 
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
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
        } catch (ResourceNotFoundException rnfe) {
            throw new StorageEngageException("Unable to connect to tap client: ", rnfe);
        }
        AdStorageQuery adQuery = new AdStorageQuery(storageBucket);
        Iterator<StorageMetadata> storageMetadataIterator = null;

        try {
            storageMetadataIterator = tc.execute(adQuery.getQuery(), adQuery.getRowMapper());
        } catch (Exception e) {
            log.error("error executing TapClient query");

            throw new TransientException(e.getMessage());
        }

        // AdStorageIterator ensures duplicates don't get sent out
        return new AdStorageIterator(storageMetadataIterator);
    }

    private URL toURL(URI uri) {
        try {
            Subject subject = AuthenticationUtil.getCurrentSubject();
            AuthMethod authMethod = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            if (authMethod == null) {
                authMethod = AuthMethod.ANON;
            }
            RegistryClient rc = new RegistryClient();
            Capabilities caps = rc.getCapabilities(DATA_RESOURCE_ID);
            Capability dataCap = caps.findCapability(Standards.DATA_10);
            Interface ifc = dataCap.findInterface(authMethod);
            if (ifc == null) {
                throw new IllegalArgumentException("No interface for auth method " + authMethod);
            }
            String baseDataURL = ifc.getAccessURL().getURL().toString();
            URL url = new URL(baseDataURL + "/" + uri.getSchemeSpecificPart());
            log.debug(uri + " --> " + url);
            return url;
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG", ex);
        } catch (Throwable t) {
            String message = "Failed to convert to data URL";
            throw new RuntimeException(message, t);
        }
    }

}

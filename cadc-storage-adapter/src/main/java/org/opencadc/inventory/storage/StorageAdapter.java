/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2023.                            (c) 2023.
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
 *  with OpenCADC.  if not, see          OpenCADC ; si ce n’est
 *  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
 *                                       <http://www.gnu.org/licenses/>.
 *
 *  $Revision: 4 $
 *
 ************************************************************************
 */

package org.opencadc.inventory.storage;

import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.IncorrectContentChecksumException;
import ca.nrc.cadc.net.IncorrectContentLengthException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;

import java.util.List;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.StorageLocation;

/**
 * The interface to storage implementations. Implementations may throw InvalidConfigException
 * or StorageEngageException from the constructor.
 * 
 * @author majorb
 *
 */
public interface StorageAdapter {

    /**
     * Configure the optional set of namespaces to preserve and allow for later
     * recovery. When objects are deleted from storage with delete(StorageLocation),
     * those with matching artifact uri should be marked as deleted but otherwise
     * preserved for later recovery via recover(StorageLocation storageLocation, 
     * Date contentLastModified).
     * 
     * @param preserved set of namespaces to preserve
     * @throws UnsupportedOperationException if preservation for recovery is not supported
     */
    public void setRecoverableNamespaces(List<Namespace> preserved);
    
    public List<Namespace> getRecoverableNamespaces();
    
    /**
     * Configure the optional set of namespaces to purge from storage. When objects 
     * are deleted from storage with delete(StorageLocation), those with matching
     * artifact uri will be deleted without consulting the normal "recoverable namespaces"
     * list whether or not they were previously deleted (marked) or not.
     * 
     * @param purged set of namespaces to purge
     */
    public void setPurgeNamespaces(List<Namespace> purged);
    
    public List<Namespace> getPurgeNamespaces();
    
    /**
     * Get the bucket type supported by the adapter.
     * 
     * @return a BucketType
     */
    public BucketType getBucketType();
    
    /**
     * Get a stored object identified by storageLocation.
     * 
     * @param storageLocation the storage location containing storageID and storageBucket
     * @param dest the destination stream
     * @throws java.lang.InterruptedException if thread receives an interrupt
     * @throws ResourceNotFoundException if the artifact could not be found
     * @throws ReadException if the storage system failed to stream
     * @throws WriteException if the client failed to stream
     * @throws StorageEngageException if the adapter failed to interact with storage
     * @throws TransientException if an unexpected, temporary exception occurred
     */
    public void get(StorageLocation storageLocation, OutputStream dest)
        throws InterruptedException, ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException;
    
    /**
     * Get part of a stored object identified by storageLocation using a byte range.
     * 
     * @param storageLocation the object to read
     * @param dest the destination stream
     * @param byteRange a single byte range to retrieve
     * 
     * @throws java.lang.InterruptedException if thread receives an interrupt
     * @throws ResourceNotFoundException if the artifact could not be found
     * @throws ReadException if the storage system failed to stream
     * @throws WriteException if the client failed to stream
     * @throws StorageEngageException if the adapter failed to interact with storage
     * @throws TransientException if an unexpected, temporary exception occurred
     */
    public void get(StorageLocation storageLocation, OutputStream dest, ByteRange byteRange)
        throws InterruptedException, ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException;
            
    /**
     * Write an object to storage. The returned storage location will be used for future get and 
     * delete calls. If the storage implementation overwrites a previously used StorageLocation, it must
     * perform an atomic replace and leave the previously stored bytes intact if the put fails. The storage
     * implementation may be designed to always write to a new storage location (e.g. generated unique storageID). 
     * The value of storageBucket in the returned StorageMetadata and StorageLocation can be used to
     * retrieve batches of objects in some of the iterator signatures defined in this interface.
     * Batches of artifacts can be listed by bucket in two of the iterator methods in this interface.
     * If storageBucket is null then the caller will not be able perform bucket-based batch
     * validation through the iterator methods.
     * 
     * @param newArtifact The holds information about the incoming artifact.  if the contentChecksum
     *     and contentLength are set, they will be used to validate the bytes received.
     * @param source The stream from which to read.
     * @param transactionID existing transactionID or null for auto-commit
     * 
     * @return current StorageMetadata
     * 
     * @throws ByteLimitExceededException if content length exceeds internal limit
     * @throws IllegalArgumentException if the newArtifact is invalid or the transaction does not exist
     * @throws IncorrectContentChecksumException if the calculated checksum does not match the expected
     *     checksum as described in newArtifact.
     * @throws IncorrectContentLengthException if the calculated length does not match the expected
     *     length as described in newArtifact.
     * @throws java.lang.InterruptedException if thread receives an interrupt
     * @throws ReadException if the client failed to stream.
     * @throws WriteException if the storage system failed to stream.
     * @throws StorageEngageException if the adapter failed to interact with storage.
     * @throws TransientException if an unexpected, temporary exception occurred.
     */
    public StorageMetadata put(NewArtifact newArtifact, InputStream source, String transactionID)
        throws ByteLimitExceededException, IllegalArgumentException, IncorrectContentChecksumException, IncorrectContentLengthException, 
            InterruptedException, ReadException, WriteException, StorageEngageException, TransientException;
        
    /**
     * Delete the object identified by storageLocation. Actual behaviour is dependent on the
     * configured set of recoverable namespaces and purged namespaces described elsewhere.
     * 
     * @param storageLocation the object to delete
     * 
     * @throws ResourceNotFoundException if the location could not be found or it is already preserved/recoverable
     * @throws IOException if an unrecoverable error occurred.
     * @throws java.lang.InterruptedException if thread receives an interrupt
     * @throws StorageEngageException if the adapter failed to interact with storage.
     * @throws TransientException if an unexpected, temporary exception occurred. 
     */
    public void delete(StorageLocation storageLocation)
        throws ResourceNotFoundException, IOException, InterruptedException, StorageEngageException, TransientException;
    
    /**
     * Delete the object identified by storageLocation. The includeRecoverable flag specifies if
     * recoverable (previously deleted and preserved) locations are considered valid targets to be
     * deleted; a value of true allows for the deletion of previously deleted objects. Actual behaviour 
     * is dependent on the configured set of recoverable and purged namespaces described elsewhere.
     * 
     * @param storageLocation the object to delete
     * @param includeRecoverable true to force deletion of preserved objects, otherwise false
     * 
     * @throws ResourceNotFoundException if the location could not be found or it is preserved/recoverable and includeRecoverable=false
     * @throws IOException if an unrecoverable error occurred.
     * @throws java.lang.InterruptedException if thread receives an interrupt
     * @throws StorageEngageException if the adapter failed to interact with storage.
     * @throws TransientException if an unexpected, temporary exception occurred. 
     */
    public void delete(StorageLocation storageLocation, boolean includeRecoverable)
        throws ResourceNotFoundException, IOException, InterruptedException, StorageEngageException, TransientException;
    
    /**
     * Recover a previously deleted storage location. Implementations should try to reset or store
     * the provided contentLastModified and return that value in future iterations over StorageMetadata
     * objects.
     * 
     * @param storageLocation the storage location to recover
     * @param contentLastModified preferred last modified timestamp
     * @throws ResourceNotFoundException
     * @throws IOException
     * @throws InterruptedException
     * @throws StorageEngageException
     * @throws TransientException 
     */
    public void recover(StorageLocation storageLocation, Date contentLastModified)
        throws ResourceNotFoundException, IOException, InterruptedException, StorageEngageException, TransientException;
    
    /**
     * Start a transaction for a PUT operation.
     * 
     * @param artifactURI artifact identifier
     * @param contentLength complete length of the data
     * @return transaction status
     * 
     * @throws StorageEngageException if the adapter failed to interact with storage
     * @throws TransientException if an unexpected, temporary exception occurred 
     */
    PutTransaction startTransaction(URI artifactURI, Long contentLength) 
        throws StorageEngageException, TransientException;
    
    /**
     * Revert a transaction to a previous state.
     * 
     * @param transactionID existing transaction ID
     * @return final storage metadata
     * 
     * @throws IllegalArgumentException if the specified transaction does not exist
     * @throws StorageEngageException if the adapter failed to interact with storage
     * @throws TransientException if an unexpected, temporary exception occurred 
     * @throws UnsupportedOperationException if the transaction cannot be reverted
     */
    public PutTransaction revertTransaction(String transactionID)
        throws IllegalArgumentException, StorageEngageException, TransientException, UnsupportedOperationException;
    
    /**
     * Commit a transaction.
     * 
     * @param transactionID existing transaction ID
     * @return current transaction status
     * 
     * @throws IllegalArgumentException if the specified transaction does not exist
     * @throws StorageEngageException if the adapter failed to interact with storage
     * @throws TransientException if an unexpected, temporary exception occurred 
     */
    public StorageMetadata commitTransaction(String transactionID)
        throws IllegalArgumentException, StorageEngageException, TransientException;
    
    /**
     * Abort a transaction.
     * 
     * @param transactionID existing transaction ID
     * 
     * @throws IllegalArgumentException if the specified transaction does not exist
     * @throws StorageEngageException if the adapter failed to interact with storage
     * @throws TransientException if an unexpected, temporary exception occurred 
     */
    public void abortTransaction(String transactionID) 
        throws IllegalArgumentException, StorageEngageException, TransientException;
    
    /**
     * Get current transaction status.
     * 
     * @param transactionID existing transaction ID
     * @return transaction status with current storage metadata for bytes received and stored
     * 
     * @throws IllegalArgumentException if the specified transaction does not exist
     * @throws StorageEngageException if the adapter failed to interact with storage
     * @throws TransientException if an unexpected, temporary exception occurred 
     */
    public PutTransaction getTransactionStatus(String transactionID)
        throws IllegalArgumentException, StorageEngageException, TransientException;
            
    /**
     * Iterator of items ordered by StorageLocation.
     * 
     * @return An iterator over an ordered list of items in storage.
     * 
     * @throws StorageEngageException if the adapter failed to interact with storage.
     * @throws TransientException if an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> iterator()
        throws StorageEngageException, TransientException;
    
    /**
     * Iterate over StorageMetadata ordered by their StorageLocation.
     * 
     * @param storageBucketPrefix null, partial, or complete storageBucket string
     * @return iterator over StorageMetadata sorted by StorageLocation
     * 
     * @throws StorageEngageException if the adapter failed to interact with storage.
     * @throws TransientException if an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> iterator(String storageBucketPrefix)
        throws StorageEngageException, TransientException;
    
    /**
     * Iterate over StorageMetadata ordered by their StorageLocation. Some implementations
     * may support recovery of previously deleted objects; these can be included in the 
     * iteration.
     * 
     * @param storageBucketPrefix null, partial, or complete storageBucket string
     * @param includeRecoverable include stored objects that are recoverable (from deleted state)
     * @return iterator over StorageMetadata sorted by StorageLocation
     * 
     * @throws StorageEngageException if the adapter failed to interact with storage
     * @throws TransientException if an unexpected, temporary exception occurred 
     */
    public Iterator<StorageMetadata> iterator(String storageBucketPrefix, boolean includeRecoverable)
        throws StorageEngageException, TransientException;
    
    /**
     * Iterate over current transactions. This method is expected to be used by 
     * a maintenance program like file-validate to cleanup stale transactions.
     * 
     * @return Iterator over current transactions
     * 
     * @throws StorageEngageException if the adapter failed to interact with storage
     * @throws TransientException if an unexpected, temporary exception occurred
     */
    public Iterator<PutTransaction> transactionIterator() 
        throws StorageEngageException, TransientException;
}

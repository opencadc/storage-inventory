/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2019.                            (c) 2019.
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

package org.opencadc.inventory.storage;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.PropertiesReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencadc.inventory.StorageLocation;

/**
 * Provides access to storage.  
 *
 * @author majorb
 *
 */
public class StorageClient implements StorageAdapter {
    
    private static Logger log = Logger.getLogger(StorageClient.class);
    public static String STORAGE_ADPATER_CLASS_PROPERTY = StorageAdapter.class.getName();
    
    private StorageAdapter adapter;

    /**
     * Constructor that loads the StorageAdapter from a system property.
     * system property.
     */
    public StorageClient() {
        String adpaterClassname = System.getProperty(STORAGE_ADPATER_CLASS_PROPERTY);
        this.adapter = load(adpaterClassname);
    }
    
    /**
     * Constructor that takes a pre-loaded storage adapter.
     * @param adapter The adapter to use.
     */
    public StorageClient(StorageAdapter adapter) {
        this.adapter = adapter;
    }
    
    /**
     * Constructor that loads the StorageAdapter from a configuration file.
     * @param configFilename The configuration file.
     */
    public StorageClient(String configFilename) {
        PropertiesReader pr = new PropertiesReader(configFilename);
        String adpaterClassname = pr.getFirstPropertyValue(STORAGE_ADPATER_CLASS_PROPERTY);
        this.adapter = load(adpaterClassname);
    }
    
    /**
     * Load the configured storage adapter or the default one if none
     * are found.
     * 
     * @return The storage adapter to be used by this client.
     */
    private StorageAdapter load(String cname) {
        if (cname == null) {
            throw new IllegalStateException(
                "No storage adapter defined by system property: " + STORAGE_ADPATER_CLASS_PROPERTY);
        }
        try {
            Class c = Class.forName(cname);
            Constructor con = c.getConstructor();
            Object o = con.newInstance();
            StorageAdapter ret = (StorageAdapter) o;
            log.debug("Loaded Storage adapter: " + cname);
            return ret;
        } catch (Throwable t) {
            throw new IllegalStateException("Failed to load storage adapter " + cname, t);
        }
    }
    
    /**
     * Get from storage the artifact identified by storageLocation.
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
    public void get(StorageLocation storageLocation, OutputStream dest)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        adapter.get(storageLocation, dest);
    }
    
    /**
     * Get from storage the artifact identified by storageLocation.
     * 
     * @param storageLocation The storage location containing storageID and storageBucket.
     * @param dest The destination stream.
     * @param cutouts Cutouts to be applied to the artifact
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If the client failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public void get(StorageLocation storageLocation, OutputStream dest, Set<String> cutouts)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        adapter.get(storageLocation, dest, cutouts);
    }
    
    /**
     * Write an artifact to storage.
     * The value of storageBucket in the returned StorageMetadata and StorageLocation can be used to
     * retrieve batches of artifacts in some of the iterator signatures defined in this interface.
     * Batches of artifacts can be listed by bucket in two of the iterator methods in this interface.
     * If storageBucket is null then the caller will not be able perform bucket-based batch
     * validation through the iterator methods.
     * 
     * @param newArtifact The holds information about the incoming artifact.  If the contentChecksum
     *     and contentLength are set, they will be used to validate the bytes received.
     * @param source The stream from which to read.
     * @return The storage metadata.
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws StreamCorruptedException If the calculated checksum does not the expected checksum.
     * @throws ReadException If the client failed to stream.
     * @throws WriteException If the storage system failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred.
     */
    public StorageMetadata put(NewArtifact newArtifact, InputStream source)
        throws ResourceNotFoundException, StreamCorruptedException, ReadException, WriteException,
            StorageEngageException, TransientException {
        return adapter.put(newArtifact, source);
    }
        
    /**
     * Delete from storage the artifact identified by storageLocation.
     * @param storageLocation Identifies the artifact to delete.
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException If an unrecoverable error occurred.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public void delete(StorageLocation storageLocation)
        throws ResourceNotFoundException, IOException, StorageEngageException, TransientException {
        adapter.delete(storageLocation);
    }
    
    /**
     * Iterator of items ordered by their storageIDs.
     * @return An iterator over an ordered list of items in storage.
     * 
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If the client failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> iterator()
        throws ReadException, WriteException, StorageEngageException, TransientException {
        return adapter.iterator();
    }
    
    /**
     * Iterator of items ordered by their storageIDs in the given bucket.
     * @param storageBucket Only iterate over items in this bucket.
     * @return An iterator over an ordered list of items in this storage bucket.
     * 
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If the client failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> iterator(String storageBucket)
        throws ReadException, WriteException, StorageEngageException, TransientException {
        return adapter.iterator(storageBucket);
    }
    
    /**
     * An unordered iterator of items in the given bucket.
     * @param storageBucket Only iterate over items in this bucket.
     * @return An iterator over an ordered list of items in this storage bucket.
     * 
     * @throws ReadException If the storage system failed to stream.
     * @throws WriteException If the client failed to stream.
     * @throws StorageEngageException If the adapter failed to interact with storage.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> unsortedIterator(String storageBucket)
        throws ReadException, WriteException, StorageEngageException, TransientException {
        return adapter.unsortedIterator(storageBucket);
    }

}

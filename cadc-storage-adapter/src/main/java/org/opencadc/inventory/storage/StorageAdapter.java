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

import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.URI;
import java.util.Iterator;
import java.util.Set;

import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;

/**
 * The interface to storage implementations.
 * 
 * @author majorb
 *
 */
public interface StorageAdapter {

    /**
     * Get from storage the artifact identified by storageID.
     * 
     * @param storageID The artifact location identifier.
     * @param wrapper An input stream wrapper to receive the bytes.
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public void get(URI storageID, InputStreamWrapper wrapper) throws ResourceNotFoundException, IOException, TransientException;
    
    /**
     * Get from storage the artifact identified by storageID.
     * 
     * @param storageID The artifact location identifier.
     * @param wrapper An input stream wrapper to receive the bytes.
     * @param cutouts Cutouts to be applied to the artifact
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public void get(URI storageID, InputStreamWrapper wrapper, Set<String> cutouts) throws ResourceNotFoundException, IOException, TransientException;
    
    /**
     * Write an artifact to storage.
     * The bucket value in the supplied artifact may be used by the storage implementation
     * to organize files.  Batches of artifacts can be listed by bucket in two of the
     * iterator signatures in this interface.
     * 
     * @param artifact The artifact metadata.
     * @param wrapper The wrapper for data of the artifact.
     * @return The storage metadata.
     * 
     * @throws StreamCorruptedException If the calculated checksum does not the expected checksum.
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred.
     */
    public StorageMetadata put(Artifact artifact, OutputStreamWrapper wrapper) throws StreamCorruptedException, IOException, TransientException;
        
    /**
     * Delete from storage the artifact identified by storageID.
     * @param storageID Identifies the artifact to delete.
     * 
     * @throws ResourceNotFoundException If the artifact could not be found.
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public void delete(URI storageID) throws ResourceNotFoundException, IOException, TransientException;
    
    /**
     * Iterator of items ordered by their storageIDs.
     * @return An iterator over an ordered list of items in storage.
     * 
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> iterator() throws IOException, TransientException;
    
    /**
     * Iterator of items ordered by their storageIDs in the given bucket.
     * @param bucket Only iterate over items in this bucket.
     * @return An iterator over an ordered list of items in this storage bucket.
     * 
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> iterator(String bucket) throws IOException, TransientException;
    
    /**
     * An unordered iterator of items in the given bucket.
     * @param bucket Only iterate over items in this bucket.
     * @return An iterator over an ordered list of items in this storage bucket.
     * 
     * @throws IOException If an unrecoverable error occurred.
     * @throws TransientException If an unexpected, temporary exception occurred. 
     */
    public Iterator<StorageMetadata> unsortedIterator(String bucket) throws IOException, TransientException;
    
}

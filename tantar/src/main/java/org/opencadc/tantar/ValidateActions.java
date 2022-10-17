/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2022.                            (c) 2022.
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
 *
 ************************************************************************
 */

package org.opencadc.tantar;

import java.net.URI;
import java.util.EventListener;

import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * Interface used to limit dependencies so this lib doesn't depend on cadc-inventory-db??
 * 
 * @author pdowler
 */
public interface ValidateActions extends EventListener {
    
    /**
     * Get existing artifact by uri (from inventory). This is to support possible changes in StorageLocation
     * for recovery of existing artifact-storage connection.
     * 
     * @param uri uri of the artifact to get from inventory
     * @return the artifact or null
     */
    Artifact getArtifact(URI uri);

    /**
     * Create a new Artifact using metadata from the given StorageMetadata.
     *
     * @param storageMetadata       The StorageMetadata to pull metadata from.
     * @throws Exception    Any unexpected error.
     */
    void createArtifact(final StorageMetadata storageMetadata) throws Exception;

    /**
     * Delete the given StorageMetadata.
     *
     * @param storageMetadata   The StorageMetadata to delete
     * @throws Exception    Any unexpected error.
     */
    void delete(final StorageMetadata storageMetadata) throws Exception;

    /**
     * Delete the given Artifact.
     *
     * @param artifact      The Artifact to remove.
     * @throws Exception    Any unexpected error.
     */
    void delete(final Artifact artifact) throws Exception;

    /**
     * Clear the StorageLocation of an Artifact.
     *
     * @param artifact The base artifact.  This MUST have a Storage Location.
     * @throws Exception Anything IO/Thread related.
     */
    void clearStorageLocation(final Artifact artifact) throws Exception;

    /**
     * Update the storageLocation of the given Artifact.
     *
     * @param artifact      Artifact to update
     * @param storageLoc    StorageLocation to assign
     * @throws Exception    Any unexpected error.
     */
    void updateArtifact(final Artifact artifact, final StorageLocation storageLoc) throws Exception;
    
    /**
     * Replace the given Artifact with a new one created from the given StorageMetadata instance.
     *
     * @param artifact          The Artifact to replace.
     * @param storageMetadata   The StorageMetadata from which to create a new Artifact.
     * @throws Exception    Any unexpected error.
     */
    void replaceArtifact(final Artifact artifact, final StorageMetadata storageMetadata) throws Exception;

    /**
     * Delay validation but increment count.
     */
    void delayAction();
}

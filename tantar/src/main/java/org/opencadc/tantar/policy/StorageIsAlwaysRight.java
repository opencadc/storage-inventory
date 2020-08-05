
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
 *
 ************************************************************************
 */

package org.opencadc.tantar.policy;

import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.tantar.Reporter;
import org.opencadc.tantar.ValidateEventListener;


public class StorageIsAlwaysRight extends ResolutionPolicy {

    public StorageIsAlwaysRight(final ValidateEventListener validateEventListener, final Reporter reporter) {
        super(validateEventListener, reporter);
    }

    /**
     * Use the logic of this Policy to correct a conflict caused by the two given items.  One of the arguments can
     * be null, but not both.
     *
     * @param artifact        The Artifact to use in deciding.
     * @param storageMetadata The StorageMetadata to use in deciding.
     */
    @Override
    public void resolve(final Artifact artifact, final StorageMetadata storageMetadata) throws Exception {
        if (artifact == null) {
            if (storageMetadata.isValid()) {
                // The Inventory has a file that does not exist in storage.  This is most unusual.
                reporter.report("Adding Artifact " + storageMetadata.getStorageLocation() + " as per policy.");

                validateEventListener.createArtifact(storageMetadata);
            } else {
                // If storage is always right, but it has no metadata, then leave it for someone to manually fix.
                reporter.report("Corrupt or invalid Storage Metadata (" + storageMetadata.getStorageLocation()
                                + ").  Skipping as per policy.");
            }
        } else if (storageMetadata == null) {
            reporter.report("Removing Unknown Artifact " + artifact.storageLocation + " as per policy.");
            validateEventListener.delete(artifact);
        } else {
            // Check metadata for discrepancies.
            if (haveDifferentStructure(artifact, storageMetadata)) {
                // The metadata differs, but for valid reasons (update).
                if (storageMetadata.isValid()) {
                    // Then prefer the Storage Metadata.
                    reporter.report("Replacing Artifact " + artifact.storageLocation + " as per policy.");

                    validateEventListener.replaceArtifact(artifact, storageMetadata);
                } else {
                    // If storage is always right, but it has no metadata, then leave it for someone to manually fix.
                    reporter.report("Invalid Storage Metadata (" + storageMetadata.getStorageLocation()
                                    + ").  Skipping as per policy.");
                }
            } else {
                reporter.report("Storage Metadata " + storageMetadata.getStorageLocation()
                                + " is valid as per policy.");
            }
        }
    }
}


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

package org.opencadc.inventory.storage.policy;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * Policy to ensure that a recovery from Storage (in the event of a disaster or a new site is brought online) will
 * dictate what goes into the Inventory Database. This policy differs from StorageIsAlwaysRight because... ???
 */
public class RecoverFromStorage extends StorageValidationPolicy {
    private static final Logger log = Logger.getLogger(RecoverFromStorage.class);

    public RecoverFromStorage(ValidateEventListener validateEventListener) {
        super(validateEventListener);
        throw new UnsupportedOperationException("** incomplete implementation **");
    }

    /**
     * Use the logic of this Policy to correct a conflict caused by the two given items.  Only the Artifact can be
     * null; it will be assumed that the given StorageMetadata is never null.
     *
     * @param artifact        The Artifact to use in deciding.  Can be null.
     * @param storageMetadata The StorageMetadata to use in deciding.  Never null.
     * @throws Exception For any unknown error that should be passed up.
     */
    @Override
    public void validate(final Artifact artifact, final StorageMetadata storageMetadata) throws Exception {
        if (artifact == null && storageMetadata == null) {
            throw new RuntimeException("BUG: both args to resolve are null");
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName());
        if (storageMetadata == null || !storageMetadata.isValid()) {
            sb.append(".noAction");
            if (artifact != null) {
                sb.append(" Artifact.id=").append(artifact.getID());
                sb.append(" Artifact.uri=").append(artifact.getURI());
                sb.append(" reason=no-matching-storageLocation");
            } else {
                sb.append(" reason=invalid-storageLocation");
            }
            log.info(sb.toString());
            return;
        }
        
        if (artifact == null) {
            sb.append(".createArtifact");
            sb.append(" Artifact.uri=").append(storageMetadata.getArtifactURI());
            sb.append(" loc=").append(storageMetadata.getStorageLocation());
            //log.info(String.format("Adding Artifact %s as per policy.", storageMetadata.getStorageLocation()));
            log.info(sb.toString());
            validateEventListener.createArtifact(storageMetadata);
        } else  {
            sb.append(".updateArtifact");
            sb.append(" Artifact.id=").append(artifact.getID());
            sb.append(" Artifact.uri=").append(artifact.getURI());
            sb.append(" loc=").append(storageMetadata.getStorageLocation());
            // This scenario is for an incomplete previous run.  Treat the Artifact as corrupt and set it back to the
            // StorageMetadata's values.
            //log.info(String.format("Updating Artifact %s as per policy.", storageMetadata.getStorageLocation()));
            log.info(sb.toString());
            validateEventListener.updateArtifact(artifact, storageMetadata);
        }
    }
}
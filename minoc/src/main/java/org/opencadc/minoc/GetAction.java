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

package org.opencadc.minoc;

import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.util.StringUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.opencadc.fits.FitsOperations;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.minoc.operations.ProxyRandomAccessFits;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.soda.ExtensionSlice;
import org.opencadc.soda.SodaParamValidator;

/**
 * Interface with storage and inventory to get an artifact.
 *
 * @author majorb
 */
public class GetAction extends ArtifactAction {
    
    private static final Logger log = Logger.getLogger(GetAction.class);
    private static final SodaParamValidator SODA_PARAM_VALIDATOR = new SodaParamValidator();

    // Allow implementor to set the name of the query parameter expected.  Default is left to SODA.
    private static final String SUB_PARAMETER_NAME_KEY = "sub-parameter";

    /**
     * Default, no-arg constructor.
     */
    public GetAction() {
        super();
    }

    /**
     * Download the artifact or cutouts of the artifact.
     */
    @Override
    public void doAction() throws Exception {
        
        initAndAuthorize(ReadGrant.class);
        
        Artifact artifact = getArtifact(artifactURI);
        HeadAction.setHeaders(artifact, syncOutput);

        final String configuredSubKey = initParams.get(SUB_PARAMETER_NAME_KEY);
        final String subKey = StringUtil.hasText(configuredSubKey) ? configuredSubKey : SodaParamValidator.SUB;
        final List<String> requestedSubs = syncInput.getParameters(subKey);

        StorageLocation storageLocation = new StorageLocation(artifact.storageLocation.getStorageID());
        storageLocation.storageBucket = artifact.storageLocation.storageBucket;
        log.debug("retrieving artifact from storage...");
        try {
            if (requestedSubs == null || requestedSubs.isEmpty()) {
                storageAdapter.get(storageLocation, syncOutput.getOutputStream());
            } else {
                // If any cutouts were requested
                final Map<String, List<String>> subMap = new HashMap<>();
                subMap.put(subKey, requestedSubs);
                final List<ExtensionSlice> slices = SODA_PARAM_VALIDATOR.validateSUB(subMap);
                final FitsOperations fitsOperations =
                        new FitsOperations(new ProxyRandomAccessFits(this.storageAdapter, artifact.storageLocation,
                                                                     artifact.getContentLength()));
                fitsOperations.cutoutToStream(slices, syncOutput.getOutputStream());
            }
        } catch (WriteException e) {
            // error on client write
            String msg = "write output error";
            log.debug(msg, e);
            if (e.getMessage() != null) {
                msg += ": " + e.getMessage();
            }
            throw new IllegalArgumentException(msg, e);
        }
        log.debug("retrieved artifact from storage");

    }

}

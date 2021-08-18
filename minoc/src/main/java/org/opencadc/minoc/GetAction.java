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

import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.util.CaseInsensitiveStringComparator;
import ca.nrc.cadc.util.StringUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import java.util.TreeMap;
import org.apache.log4j.Logger;
import org.opencadc.fits.FitsOperations;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.minoc.operations.CutoutFileNameFormat;
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
    private static final String CONTENT_DISPOSITION = "Content-Disposition";
    private static final String[] FITS_CONTENT_TYPES = new String[] {
        "application/fits", "image/fits"
    };

    private static final SodaParamValidator SODA_PARAM_VALIDATOR = new SodaParamValidator();


    /**
     * Default, no-arg constructor.
     */
    public GetAction() {
        super();
    }

    /**
     * Download the artifact or cutouts of the artifact.  In the event that an optional cutout was requested, then
     * mangle the output filename to reflect the requested values.
     * The META=true keyword can be passed in to print the headers, but only if NO OTHER cutout parameters were
     * supplied.
     */
    @Override
    public void doAction() throws Exception {
        
        checkReadable();
        authorize(ReadGrant.class);
        initDAO();
        initStorageAdapter();
        
        Artifact artifact = getArtifact(artifactURI);
        SodaCutout sodaCutout = new SodaCutout();

        StorageLocation storageLocation = new StorageLocation(artifact.storageLocation.getStorageID());
        storageLocation.storageBucket = artifact.storageLocation.storageBucket;
        
        log.debug("retrieving artifact from storage: " + storageLocation);
        
        ByteCountOutputStream bcos = null;
        try {
            if (sodaCutout.hasNoOperations()) {
                log.debug("No parameters specified.");
                HeadAction.setHeaders(artifact, syncOutput);
                bcos = new ByteCountOutputStream(syncOutput.getOutputStream());
                storageAdapter.get(storageLocation, bcos);
            } else {
                if (!isFITS(artifact)) {
                    throw new IllegalArgumentException("not a fits file: " + artifactURI);
                }
                
                final List<String> conflicts = sodaCutout.getConflicts();
                if (!conflicts.isEmpty()) {
                    throw new IllegalArgumentException("Conflicting SODA parameters found: " + conflicts);
                }
                
                final FitsOperations fitsOperations =
                        new FitsOperations(new ProxyRandomAccessFits(this.storageAdapter, artifact.storageLocation,
                                                                     artifact.getContentLength()));

                if (sodaCutout.hasSUB()) {
                    log.debug("SUB supplied");
                    final Map<String, List<String>> parameterMap = new TreeMap<>(new CaseInsensitiveStringComparator());
                    parameterMap.put(SodaParamValidator.SUB, sodaCutout.requestedSubs);
                    final List<ExtensionSlice> slices = SODA_PARAM_VALIDATOR.validateSUB(parameterMap);
                    final String schemePath = artifactURI.getSchemeSpecificPart();
                    final String fileName = schemePath.substring(schemePath.lastIndexOf("/") + 1);
                    final CutoutFileNameFormat cutoutFileNameFormat = new CutoutFileNameFormat(fileName);
                    syncOutput.setHeader(CONTENT_DISPOSITION, "inline; filename=\"" + cutoutFileNameFormat.format(slices) + "\"");
                    syncOutput.setHeader(HttpTransfer.CONTENT_TYPE, "application/fits");
                    bcos = new ByteCountOutputStream(syncOutput.getOutputStream());
                    fitsOperations.cutoutToStream(slices, bcos);
                    return;
                } 
                
                if (sodaCutout.isMETA()) {
                    log.debug("META supplied");
                    final String filename = InventoryUtil.computeArtifactFilename(artifact.getURI());
                    syncOutput.setHeader(CONTENT_DISPOSITION, "inline; filename=\"" + filename + ".txt\"");
                    syncOutput.setHeader(HttpTransfer.CONTENT_TYPE, "text/plain");
                    fitsOperations.headersToStream(syncOutput.getOutputStream());
                    return;
                }
                
                throw new RuntimeException("BUG: unhandled SODA parameters");
            }
        } catch (WriteException e) {
            // error on client write
            String msg = "write output error";
            log.debug(msg, e);
            if (e.getMessage() != null) {
                msg += ": " + e.getMessage();
            }
            throw new IllegalArgumentException(msg, e);
        } finally {
            if (bcos != null) {
                super.logInfo.setBytes(bcos.getByteCount());
            }
        }
        log.debug("retrieved artifact from storage");
    }

    private boolean isFITS(final Artifact artifact) {
        return StringUtil.hasText(artifact.contentType)
               && Arrays.stream(FITS_CONTENT_TYPES).anyMatch(s -> s.equals(artifact.contentType));
    }

    /**
     * Simple encompassing class to handle cutout checks.
     */
    private final class SodaCutout {
        final List<String> requestedSubs;
        final boolean requestedMeta;

        public SodaCutout() {
            this.requestedSubs = syncInput.getParameters(SodaParamValidator.SUB);
            this.requestedMeta = "true".equals(syncInput.getParameter(SodaParamValidator.META));
        }

        boolean hasSUB() {
            return requestedSubs != null && !requestedSubs.isEmpty();
        }

        boolean isMETA() {
            return requestedMeta;
        }

        boolean hasNoOperations() {
            return !hasSUB() && !isMETA();
        }

        /**
         * Obtain a list of conflicting parameters, if any.  Conflicting parameters include combining the META parameter
         * with any cutout, as well as combining the SUB parameter with any WCS (CIRCLE, POLYGON, etc.) cutout.
         * TODO: Evolve this method with WCS cutouts.
         * @return  List of SODA Parameter names, or empty List.  Never null.
         */
        List<String> getConflicts() {
            final List<String> conflicts = new ArrayList<>();
            if (isMETA() && hasSUB()) {
                conflicts.add(SodaParamValidator.META);
                conflicts.add(SodaParamValidator.SUB);
            }

            return conflicts;
        }
    }
}

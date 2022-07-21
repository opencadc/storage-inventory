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

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.Shape;
import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.io.ReadException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.RangeNotSatisfiableException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.CaseInsensitiveStringComparator;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.log4j.Logger;
import org.opencadc.fits.FitsOperations;
import org.opencadc.fits.NoOverlapException;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.minoc.operations.CutoutFileNameFormat;
import org.opencadc.minoc.operations.ProxyRandomAccessFits;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.soda.ExtensionSlice;
import org.opencadc.soda.SodaParamValidator;
import org.opencadc.soda.server.Cutout;

/**
 * Interface with storage and inventory to get an artifact.
 *
 * @author majorb
 */
public class GetAction extends ArtifactAction {
    
    private static final Logger log = Logger.getLogger(GetAction.class);
    private static final String RANGE = "range";
    private static final String CONTENT_DISPOSITION = "content-disposition";
    private static final String CONTENT_RANGE = "content-range";
    private static final String CONTENT_LENGTH = "content-length";
    private static final String[] FITS_CONTENT_TYPES = new String[] {
        "application/fits", "image/fits"
    };

    private static final SodaParamValidator SODA_PARAM_VALIDATOR = new SodaParamValidator();

    // constructor for unit tests with no config/init
    GetAction(boolean init) {
        super(init);
    }

    /**
     * Default, no-arg constructor.
     */
    public GetAction() {
        super();
    }

    /**
     * Perform auth checks and initialize resources.
     */
    @Override
    public void initAction() throws Exception {
        checkReadable();
        initAndAuthorize(ReadGrant.class);
        initDAO();
        initStorageAdapter();
    }

    /**
     * Download the artifact or cutouts of the artifact.  In the event that an optional cutout was requested, then
     * mangle the output filename to reflect the requested values.
     * The META=true keyword can be passed in to print the headers, but only if NO OTHER cutout parameters were
     * supplied.
     */
    @Override
    public void doAction() throws Exception {
        
        Artifact artifact = getArtifact(artifactURI);
        SodaCutout sodaCutout = new SodaCutout();

        String range = syncInput.getHeader(RANGE);
        log.debug("Range: " + range);
        
        ByteCountOutputStream bcos = null;
        try {
            // operations
            if (!sodaCutout.isEmpty()) {
                if (range != null) {
                    // TODO: UnsupportedOperationException to be explicit?
                    log.debug("Range (" + range + ") ignored in GET with operations");
                }
                
                if (!isFITS(artifact)) {
                    throw new IllegalArgumentException("not a fits file: " + artifactURI);
                }

                final List<String> conflicts = sodaCutout.getConflicts();
                if (!conflicts.isEmpty()) {
                    throw new IllegalArgumentException("Conflicting SODA parameters found: " + conflicts);
                }

                final FitsOperations fitsOperations = new FitsOperations(
                    new ProxyRandomAccessFits(this.storageAdapter, artifact.storageLocation, artifact.getContentLength()));
                
                bcos = doOperation(fitsOperations, sodaCutout);
                return;
            }
            
            // partial get
            ByteRange byteRange = getByteRange(range, artifact.getContentLength());
            if (byteRange != null) {
                bcos = doByteRangeRequest(artifact, byteRange);
                return;
            }
            
            // default: complete download
            HeadAction.setHeaders(artifact, syncOutput);
            bcos = new ByteCountOutputStream(syncOutput.getOutputStream());
            storageAdapter.get(artifact.storageLocation, bcos);
            
        } catch (RangeNotSatisfiableException e) {
            log.debug("Invalid Range - offset greater then the content length:" + range);
            syncOutput.setHeader(CONTENT_RANGE, "bytes */" + artifact.getContentLength());
            syncOutput.setHeader(CONTENT_LENGTH, "0");
            throw e;
        } catch (WriteException e) {
            // error on client write
            String msg = "write output error";
            log.debug(msg, e);
            if (e.getMessage() != null) {
                msg += ": " + e.getMessage();
            }
            
            if (bcos != null) {
                super.logInfo.setBytes(bcos.getByteCount());
            }
            super.logInfo.setSuccess(true); // TBD: WriteException is user-termination or WAN failure on the outside
            super.logInfo.setMessage(msg);

            throw new IllegalArgumentException(msg, e);
        } catch (NoOverlapException noOverlapException) {
            throw new IllegalArgumentException(noOverlapException.getMessage());
        } finally {
            if (bcos != null) {
                super.logInfo.setBytes(bcos.getByteCount());
            }
        }
        log.debug("retrieved artifact from storage");
    }

    private ByteCountOutputStream doByteRangeRequest(Artifact artifact, ByteRange byteRange) 
            throws InterruptedException, IOException, ResourceNotFoundException, 
                ReadException, WriteException, StorageEngageException, TransientException {
        HeadAction.setHeaders(artifact, syncOutput);
        syncOutput.setCode(206);
        long lastByte = byteRange.getOffset() + byteRange.getLength() - 1;
        syncOutput.setHeader(CONTENT_RANGE, "bytes " + byteRange.getOffset() + "-"
                + lastByte + "/" + artifact.getContentLength());
        // override content length
        syncOutput.setHeader(CONTENT_LENGTH, byteRange.getLength());

        ByteCountOutputStream bcos = new ByteCountOutputStream(syncOutput.getOutputStream());
        storageAdapter.get(artifact.storageLocation, bcos, byteRange);
        return bcos;
    }
    
    private ByteCountOutputStream doOperation(FitsOperations fitsOperations, SodaCutout sodaCutout)
            throws NoOverlapException, ReadException, IOException {
        
        if (sodaCutout.isMETA()) {
            log.debug("META supplied");
            final String filename = InventoryUtil.computeArtifactFilename(artifactURI) + ".txt";
            syncOutput.setHeader(CONTENT_DISPOSITION, "inline; filename=\"" + filename + "\"");
            syncOutput.setHeader(HttpTransfer.CONTENT_TYPE, "text/plain");

            ByteCountOutputStream bcos = new ByteCountOutputStream(syncOutput.getOutputStream());
            fitsOperations.headersToStream(bcos);
            return bcos;
        }
        
        if (sodaCutout.hasSUB()) {
            log.debug("SUB supplied");
            final Map<String, List<String>> parameterMap = new TreeMap<>(new CaseInsensitiveStringComparator());
            parameterMap.put(SodaParamValidator.SUB, sodaCutout.requestedSubs);
            final List<ExtensionSlice> slices = SODA_PARAM_VALIDATOR.validateSUB(parameterMap);
            final Cutout cutout = new Cutout();
            cutout.pixelCutouts = slices;

            final String schemePath = artifactURI.getSchemeSpecificPart();
            final String fileName = schemePath.substring(schemePath.lastIndexOf("/") + 1);
            final CutoutFileNameFormat cutoutFileNameFormat = new CutoutFileNameFormat(fileName);
            syncOutput.setHeader(CONTENT_DISPOSITION, "inline; filename=\""
                                                      + cutoutFileNameFormat.format(cutout) + "\"");
            syncOutput.setHeader(HttpTransfer.CONTENT_TYPE, "application/fits");
            
            ByteCountOutputStream bcos = new ByteCountOutputStream(syncOutput.getOutputStream());
            fitsOperations.cutoutToStream(cutout, bcos);
            return bcos;
        }
        
        if (sodaCutout.hasWCS()) {
            log.debug("WCS supplied.");
            final Cutout cutout = new Cutout();

            if (sodaCutout.hasCIRCLE()) {
                log.debug("CIRCLE supplied.");
                final Map<String, List<String>> parameterMap =
                        new TreeMap<>(new CaseInsensitiveStringComparator());
                parameterMap.put(SodaParamValidator.CIRCLE, sodaCutout.requestedCircles);
                final List<Circle> validCircles = SODA_PARAM_VALIDATOR.validateCircle(parameterMap);

                cutout.pos = assertSingleWCS(SodaParamValidator.CIRCLE, validCircles);
            }

            if (sodaCutout.hasPOLYGON()) {
                log.debug("POLYGON supplied.");
                final Map<String, List<String>> parameterMap =
                        new TreeMap<>(new CaseInsensitiveStringComparator());
                parameterMap.put(SodaParamValidator.POLYGON, sodaCutout.requestedPolygons);
                final List<Polygon> validPolygons = SODA_PARAM_VALIDATOR.validatePolygon(parameterMap);

                cutout.pos = assertSingleWCS(SodaParamValidator.POLYGON, validPolygons);
            }

            if (sodaCutout.hasPOS()) {
                log.debug("POS supplied.");
                final Map<String, List<String>> parameterMap =
                        new TreeMap<>(new CaseInsensitiveStringComparator());
                parameterMap.put(SodaParamValidator.POS, sodaCutout.requestedPOSs);
                final List<Shape> validShapes = SODA_PARAM_VALIDATOR.validatePOS(parameterMap);

                cutout.pos = assertSingleWCS(SodaParamValidator.POS, validShapes);
            }

            if (sodaCutout.hasBAND()) {
                log.debug("BAND supplied.");
                final Map<String, List<String>> parameterMap =
                        new TreeMap<>(new CaseInsensitiveStringComparator());
                parameterMap.put(SodaParamValidator.BAND, sodaCutout.requestedBands);
                final List<Interval> validBandIntervals = SODA_PARAM_VALIDATOR.validateBAND(parameterMap);

                cutout.band = assertSingleWCS(SodaParamValidator.BAND, validBandIntervals);
            }

            if (sodaCutout.hasTIME()) {
                log.debug("TIME supplied.");
                final Map<String, List<String>> parameterMap =
                        new TreeMap<>(new CaseInsensitiveStringComparator());
                parameterMap.put(SodaParamValidator.TIME, sodaCutout.requestedTimes);
                final List<Interval> validTimeIntervals = SODA_PARAM_VALIDATOR.validateTIME(parameterMap);

                cutout.time = assertSingleWCS(SodaParamValidator.TIME, validTimeIntervals);
            }

            if (sodaCutout.hasPOL()) {
                log.debug("POL supplied.");
                final Map<String, List<String>> parameterMap =
                        new TreeMap<>(new CaseInsensitiveStringComparator());
                parameterMap.put(SodaParamValidator.POL, sodaCutout.requestedPOLs);
                cutout.pol = SODA_PARAM_VALIDATOR.validatePOL(parameterMap);

                if (cutout.pol.size() != sodaCutout.requestedPOLs.size()) {
                    log.debug("Accepted " + cutout.pol + " valid POL states but " + sodaCutout.requestedPOLs
                              + " was requested.");
                }
            }

            final String schemePath = artifactURI.getSchemeSpecificPart();
            final String fileName = schemePath.substring(schemePath.lastIndexOf("/") + 1);
            final CutoutFileNameFormat cutoutFileNameFormat = new CutoutFileNameFormat(fileName);
            syncOutput.setHeader(CONTENT_DISPOSITION, "inline; filename=\""
                                                      + cutoutFileNameFormat.format(cutout) + "\"");
            syncOutput.setHeader(HttpTransfer.CONTENT_TYPE, "application/fits");
            
            ByteCountOutputStream bcos = new ByteCountOutputStream(syncOutput.getOutputStream());
            fitsOperations.cutoutToStream(cutout, bcos);
            return bcos;
        }
                
        throw new RuntimeException("BUG: unhandled SODA parameters");
    }
    
    private <T> T assertSingleWCS(final String key, final List<T> wcsValues) {
        if (wcsValues.isEmpty()) {
            log.debug("No valid " + key + "s found.");
            return null;
        } else if (wcsValues.size() > 1) {
            throw new IllegalArgumentException("More than one " + key + " provided.");
        } else {
            return wcsValues.get(0);
        }
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
        final List<String> requestedCircles;
        final List<String> requestedPolygons;
        final List<String> requestedPOSs;
        final List<String> requestedBands;
        final List<String> requestedTimes;
        final List<String> requestedPOLs;
        final boolean requestedMeta;

        public SodaCutout() {
            this.requestedSubs = syncInput.getParameters(SodaParamValidator.SUB);
            this.requestedCircles = syncInput.getParameters(SodaParamValidator.CIRCLE);
            this.requestedPolygons = syncInput.getParameters(SodaParamValidator.POLYGON);
            this.requestedPOSs = syncInput.getParameters(SodaParamValidator.POS);
            this.requestedBands = syncInput.getParameters(SodaParamValidator.BAND);
            this.requestedTimes = syncInput.getParameters(SodaParamValidator.TIME);
            this.requestedPOLs = syncInput.getParameters(SodaParamValidator.POL);
            this.requestedMeta = "true".equals(syncInput.getParameter(SodaParamValidator.META));
        }

        boolean hasSUB() {
            return requestedSubs != null && !requestedSubs.isEmpty();
        }

        public boolean hasWCS() {
            return hasCIRCLE() || hasPOLYGON() || hasPOS() || hasBAND() || hasTIME() || hasPOL();
        }

        public boolean hasPOL() {
            return this.requestedPOLs != null && !this.requestedPOLs.isEmpty();
        }

        public boolean hasPOS() {
            return this.requestedPOSs != null && !this.requestedPOSs.isEmpty();
        }

        public boolean hasCIRCLE() {
            return this.requestedCircles != null && !this.requestedCircles.isEmpty();
        }

        public boolean hasPOLYGON() {
            return this.requestedPolygons != null && !this.requestedPolygons.isEmpty();
        }

        public boolean hasBAND() {
            return this.requestedBands != null && !this.requestedBands.isEmpty();
        }

        public boolean hasTIME() {
            return this.requestedTimes != null && !this.requestedTimes.isEmpty();
        }

        boolean isMETA() {
            return requestedMeta;
        }

        boolean isEmpty() {
            return !hasSUB() && !isMETA() && !hasWCS();
        }

        /**
         * Obtain a list of conflicting parameters, if any.  Conflicting parameters include combining the META parameter
         * with any cutout, as well as combining the SUB parameter with any WCS (CIRCLE, POLYGON, etc.) cutout.
         *
         * @return  List of SODA Parameter names, or empty List.  Never null.
         */
        List<String> getConflicts() {
            final List<String> conflicts = new ArrayList<>();
            if (isMETA() && hasSUB()) {
                conflicts.add(SodaParamValidator.META);
                conflicts.add(SodaParamValidator.SUB);
            }

            final List<String> shapeConflicts = new ArrayList<>();
            if (hasCIRCLE()) {
                shapeConflicts.add(SodaParamValidator.CIRCLE);
            }

            if (hasPOLYGON()) {
                shapeConflicts.add(SodaParamValidator.POLYGON);
            }

            if (hasPOS()) {
                shapeConflicts.add(SodaParamValidator.POS);
            }

            // Only one spatial axis cutout is permitted.
            if (shapeConflicts.size() > 1) {
                conflicts.addAll(shapeConflicts);
            }

            return conflicts;
        }
    }

    private ByteRange getByteRange(String range, long contentLength) throws RangeNotSatisfiableException {
        SortedSet<ByteRange> ranges = parseRange(range, contentLength);
        if (ranges.isEmpty()) {
            return null;
        }
        if (ranges.size() == 1) {
            return ranges.first();
        }
        throw new RangeNotSatisfiableException("multiple ranges in request not supported");
    }
    
    // parse the complete HTTP range spec
    SortedSet<ByteRange> parseRange(String range, long contentLength) throws RangeNotSatisfiableException {
        SortedSet<ByteRange> result = new TreeSet<ByteRange>();
        if (range == null) {
            return result;
        }
        String sanitizedRange = range.replaceAll("\\s","");  // remove whitespaces
        if (!sanitizedRange.startsWith("bytes=")) {
            log.debug("Ignore Range with invalid unit (only bytes supported): " + range);
            return result;
        }
        String[] ranges = sanitizedRange.replace("bytes=", "").split(",");
        if (ranges.length > 1) {
            log.debug("Ignore multiple Ranges (only one supported): " + range);
            return result;
        }
        String[] interval = ranges[0].split("-");
        if ((interval.length == 0) || (interval.length > 2)) {
            log.debug("Ignore Range with invalid interval: " + range);
            return result;
        }
        try {
            String s =  (interval[0].length() == 0 ? "0" : interval[0]);
            Long start = Long.parseLong(s);
            if (start > contentLength - 1) {
                throw new RangeNotSatisfiableException("Offset greater than size of file");
            }
            long end = contentLength - 1;
            if (interval.length == 2) {
                end = Long.parseLong(interval[1]);
            }
            if (end < start) {
                log.debug("Ignore Range with invalid interval: " + range);
                return result;
            }
            if (end >= contentLength - 1) {
                end = contentLength - 1;
            }
            result.add(new ByteRange(start, end - start + 1));
            return result;
        } catch (NumberFormatException e) {
            log.debug("Ignore illegal range value in: " + range);
        }
        return result;
    }
}

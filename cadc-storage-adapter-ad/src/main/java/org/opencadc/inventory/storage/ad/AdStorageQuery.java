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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.tap.TapRowMapper;

/**
 * Provide query and RowMapper instance for grabbing data from ad.
 * RowMapper maps from ad's archive_files table to a StorageMetadata object.
 */
public class AdStorageQuery {
    private static final Logger log = Logger.getLogger(AdStorageQuery.class);
    // Query to use that will pull data in the order required by the mapRow function
    private String queryTemplate = "select archiveName, uri, contentMD5, fileSize, contentEncoding, contentType, ingestDate"
            + " from archive_files where archiveName='%s' order by uri asc, ingestDate desc";
    private String query = "";

    private static String MD5_ENCODING_SCHEME = "md5:";

    AdStorageQuery(String storageBucket) {
        InventoryUtil.assertNotNull(AdStorageQuery.class, "storageBucket", storageBucket);
        setQuery(storageBucket);
    }

    public TapRowMapper<StorageMetadata> getRowMapper() {
        return new AdStorageMetadataRowMapper();
    }

    class AdStorageMetadataRowMapper implements TapRowMapper<StorageMetadata> {

        public AdStorageMetadataRowMapper() { }

        @Override
        public StorageMetadata mapRow(List<Object> row) {
            Iterator i = row.iterator();

            // archive_files.archiveName
            String storageBucket = (String) i.next();

            // archive_files.uri
            URI artifactID = (URI) i.next();
            // check for null uri(storageID) and log error
            if (artifactID == null) {
                String sb = "ERROR: uri=null in ad.archive_files for archiveName=" + storageBucket;
                log.error(sb);
                return null;
            }

            // archive_files.contentMD5 is just the hex value
            URI contentChecksum = null;
            try {
                contentChecksum = new URI(MD5_ENCODING_SCHEME + i.next());
            } catch (URISyntaxException u) {
                log.debug("checksum error: " + artifactID.toString() + ": " + u.getMessage());
            }

            // archive_files.fileSize
            Long contentLength = (Long) i.next();
            if (contentLength == null) {
                log.debug("content length error (null): " + artifactID.toString());
            }

            // Set up StorageLocation object first
            StorageLocation storageLocation = new StorageLocation(deduceStorageURI(artifactID));
            storageLocation.storageBucket = storageBucket;

            // Build StorageMetadata object
            StorageMetadata storageMetadata;
            if (contentChecksum == null || (contentLength == null || contentLength == 0)) {
                storageMetadata = new StorageMetadata(storageLocation);
            } else {
                storageMetadata = new StorageMetadata(storageLocation, contentChecksum, contentLength);
            }
            storageMetadata.artifactURI = artifactID;

            // Set optional values into ret at this point - allowed to be null
            storageMetadata.contentEncoding = (String) i.next();
            storageMetadata.contentType = (String) i.next();
            storageMetadata.contentLastModified = (Date) i.next();

            log.debug("StorageMetadata: " + storageMetadata);
            return storageMetadata;
        }

        private URI deduceStorageURI(final URI artifactURI) {
            // AD TAP returns the artifact URI which in some cases is different than AD storage URI
            String filePath = artifactURI.getSchemeSpecificPart();
            String scheme = artifactURI.getScheme();
            if (scheme.equals("cadc")) {
                scheme = "ad";
            }
            String[] pathComp = filePath.split("/");
            if (pathComp[0].equals("Gemini")) {
                pathComp[0] = "GEM"; // This is how Gemini archive is known to data WS
            }
            URI result = null;
            try {
                result = new URI(scheme, String.join("/", pathComp), null);
            } catch (URISyntaxException ex) {
                log.error("BUG: Wrong deduced storage URI: " + ex.getMessage());
            }
            return result;
        }
    }

    // Getters & Setters

    public String getQuery() {
        return this.query;
    }

    public void setQuery(String storageBucket) {
        if (storageBucket == null || storageBucket.length() == 0) {
            throw new IllegalArgumentException("Storage bucket (archive) an not be null.");
        }
        this.query = String.format(this.queryTemplate, storageBucket);
    }
}


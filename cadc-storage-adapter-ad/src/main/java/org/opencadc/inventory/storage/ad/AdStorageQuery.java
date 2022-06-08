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
 * RowMapper maps from the AD archive_files table to a StorageMetadata object.
 */
public class AdStorageQuery {
    private static final Logger log = Logger.getLogger(AdStorageMetadataRowMapper.class); // intentional: log message are from nested class

    private static final String MD5_ENCODING_SCHEME = "md5:";
    
    private static final String QTMPL = "SELECT uri, inventoryURI, contentMD5, fileSize, ingestDate,"
            + " contentEncoding, contentType"
            + " FROM archive_files WHERE archiveName = '%s' %s"
            + " ORDER BY uri ASC, ingestDate DESC";

    static final String DISAMBIGUATE_PREFIX = "x-";
    //private static final List<String> ARC_PREFIX_ARC = Arrays.asList("CFHT", "GEM", "JCMT");
    
    private final String storagebucket;
    private final String query;

    AdStorageQuery(String storageBucket) {
        InventoryUtil.assertNotNull(AdStorageQuery.class, "storageBucket", storageBucket);
        this.storagebucket = storageBucket;
        String archive = bucket2archive(this.storagebucket);
        String bucketConstraint = "";
        if (this.storagebucket.contains(":")) {
            // a bucket is a subset of an archive defined by the first digits of
            // the checksumMD5 of the file
            String[] parts = this.storagebucket.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Unexpected bucket format: " + storageBucket);
            }
            String bucket = parts[1];
            // determine the bucket
            try {
                if ((bucket.length() == 0) || (bucket.length() > 3)) {
                    throw new IllegalArgumentException("Bucket part must be between 1 and 3 digits: " + storagebucket);
                }
                Integer.decode("0x" + bucket);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("invalid checksum URI: "
                        + bucket + " contains invalid hex value, expected hex value");
            }
            String minMD5Checksum = bucket + "0000000000000000".substring(0, 16 - bucket.length());
            String maxMD5Checksum = bucket + "ffffffffffffffff".substring(0, 16 - bucket.length());
            bucketConstraint = " AND contentMD5 between '" + minMD5Checksum + "' AND '" + maxMD5Checksum + "'";
        }
        this.query = String.format(this.QTMPL, archive, bucketConstraint);
    }

    public TapRowMapper<StorageMetadata> getRowMapper() {
        return new AdStorageMetadataRowMapper();
    }

    //private String archive2bucket(String arc) {
    //    for (String pre : ARC_PREFIX_ARC) {
    //        if (!arc.equals(pre) && arc.startsWith(pre)) {
    //            return DISAMBIGUATE_PREFIX + arc;
    //        }
    //    }
    //    return arc;
    //}
    
    private String bucket2archive(String sb) {
        String result = sb;
        if (sb.startsWith(DISAMBIGUATE_PREFIX)) {
            result = sb.substring(DISAMBIGUATE_PREFIX.length());
        }
        if (result.contains(":")) {
            result = result.split(":")[0];
        }
        return result;
    }
    
    class AdStorageMetadataRowMapper implements TapRowMapper<StorageMetadata> {
        public AdStorageMetadataRowMapper() { }

        @Override
        public StorageMetadata mapRow(List<Object> row) {
            Iterator i = row.iterator();

            URI storageID = (URI) i.next();
            if (storageID == null) {
                log.warn(AdStorageMetadataRowMapper.class.getSimpleName() + ".SKIP reason=null-uri");
                return null;
            }
            if (storageID.getScheme().equals("gemini")) {
                // hack to preserve previously generated storageID values for GEM
                storageID = URI.create("ad:" + storageID.getSchemeSpecificPart());
            }
            
            URI artifactURI = (URI) i.next();
            if (artifactURI == null) {
                log.warn(AdStorageMetadataRowMapper.class.getSimpleName() + ".SKIP uri=" + storageID + " reason=null-artifactURI");
                return null;
            }
            
            // archive_files.contentMD5 is just the hex value
            URI contentChecksum = null;
            String hex = (String) i.next();
            try {
                contentChecksum = new URI(MD5_ENCODING_SCHEME + hex);
                InventoryUtil.assertValidChecksumURI(AdStorageQuery.class, "contentChecksum", contentChecksum);
            } catch (IllegalArgumentException | URISyntaxException u) {
                log.warn(AdStorageMetadataRowMapper.class.getSimpleName() + ".SKIP uri=" + storageID + " reason=invalid=contentChecksum");
                return null;
            }

            // archive_files.fileSize
            Long contentLength = (Long) i.next();
            if (contentLength == null) {
                log.warn(AdStorageMetadataRowMapper.class.getSimpleName() + ".SKIP uri=" + storageID + " reason=null-contentLength");
                return null;
            }
            if (contentLength == 0L) {
                log.warn(AdStorageMetadataRowMapper.class.getSimpleName() + ".SKIP uri=" + storageID + " reason=zero-contentLength");
                return null;
            }

            StorageLocation storageLocation = new StorageLocation(storageID);
            if (storagebucket.contains(":")) {
                // return the actual 3 digit "bucket" even if a 1 or 2 digit superbucket was specified
                storageLocation.storageBucket = storagebucket.substring(0, storagebucket.indexOf(':') + 1)
                        + hex.substring(0, 3);
            } else {
                storageLocation.storageBucket = storagebucket;
            }

            Date contentLastModified = (Date) i.next();
            if (contentLastModified == null) {
                // work-around for cases with NULL ingestDate:
                // select archiveName, count(*) from archive_files where ingestDate=Null group by archiveName;
                // JCMT   30525
                // XDSS 1696503
                // CFHT   22286
                contentLastModified = new Date();
            }

            StorageMetadata storageMetadata = new StorageMetadata(storageLocation, contentChecksum, contentLength, contentLastModified);
            storageMetadata.artifactURI = artifactURI;

            // optional values
            storageMetadata.contentEncoding = (String) i.next();
            storageMetadata.contentType = (String) i.next();
            

            return storageMetadata;
        }
    }

    public String getQuery() {
        return this.query;
    }
}


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

import java.net.URI;
import java.util.Date;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;

/**
 * Class to hold artifact metadata from a storage implementation.
 * 
 * @author majorb
 *
 */
public class StorageMetadata implements Comparable<StorageMetadata> {
    private final StorageLocation storageLocation;
    private final URI contentChecksum;
    private final Long contentLength;
    private final Date contentLastModified;
    private final URI artifactURI;
    

    public String contentEncoding;
    public String contentType;
    
    public boolean deletePreserved = false;
    
    /**
     * Constructor for a valid stored object.
     * 
     * @param storageLocation location of the file in back end storage
     * @param contentChecksum checksum of the file in the form {algorithm}:{hex value}
     * @param contentLength length of the file in bytes
     * @param contentLastModified timestamp when the file was last modified
     */
    public StorageMetadata(StorageLocation storageLocation, URI artifactURI, URI contentChecksum, Long contentLength, Date contentLastModified) {
        InventoryUtil.assertNotNull(StorageMetadata.class, "storageLocation", storageLocation);
        InventoryUtil.assertNotNull(StorageMetadata.class, "artifactURI", artifactURI);
        InventoryUtil.assertNotNull(StorageMetadata.class, "contentChecksum", contentChecksum);
        InventoryUtil.assertNotNull(StorageMetadata.class, "contentLength", contentLength);
        InventoryUtil.assertNotNull(StorageMetadata.class, "contentLastModified", contentLastModified);
        if (contentLength <= 0L) {
            throw new IllegalArgumentException("invalid " + StorageMetadata.class.getSimpleName() + ".contentLength: " + contentLength);
        }
        this.storageLocation = storageLocation;
        this.artifactURI = artifactURI;
        this.contentChecksum = contentChecksum;
        this.contentLength = contentLength;
        this.contentLastModified = contentLastModified;
    }
    
    /**
     * Constructor for an invalid stored object that should be cleaned up.
     * 
     * @param storageLocation location of the file in back end storage
     */
    public StorageMetadata(StorageLocation storageLocation) {
        InventoryUtil.assertNotNull(StorageMetadata.class, "storageLocation", storageLocation);
        this.storageLocation = storageLocation;
        this.artifactURI = null;
        this.contentChecksum = null;
        this.contentLength = null;
        this.contentLastModified = null;
    }

    public boolean isValid() {
        return artifactURI != null;
    }
    
    public StorageLocation getStorageLocation() {
        return storageLocation;
    }

    public URI getArtifactURI() {
        return artifactURI;
    }
    
    public URI getContentChecksum() {
        return contentChecksum;
    }

    public Long getContentLength() {
        return contentLength;
    }

    public Date getContentLastModified() {
        return contentLastModified;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof StorageMetadata)) {
            return false;
        }
        StorageMetadata other = (StorageMetadata) o;
        return this.storageLocation.equals(other.storageLocation);
    }

    @Override
    public int hashCode() {
        return storageLocation.hashCode();
    }

    @Override
    public int compareTo(StorageMetadata rhs) {
        return storageLocation.compareTo(rhs.storageLocation);
    }

    @Override
    public String toString() {
        return "StorageMetadata[" + storageLocation + "," + artifactURI + ","
                + contentLength + "," + contentChecksum + "]";
    }
}

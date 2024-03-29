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

package org.opencadc.fenwick;

import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import java.io.IOException;
import java.net.URI;
import java.security.AccessControlException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.util.Date;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.StorageSiteDAO;
import org.opencadc.tap.TapClient;


/**
 * Class to pull a single StorageSite from a remote TAP (Luskan) service.  This class is enabled by the
 * org.opencadc.fenwick.trackSiteLocations property in the fenwick.properties file.
 */
public class StorageSiteSync extends AbstractSync {
    private static final Logger log = Logger.getLogger(StorageSiteSync.class);

    // column order folllowing model declarations
    private static final String STORAGE_SITE_QUERY =
            "SELECT resourceID, name, allowRead, allowWrite, id, lastModified, metaChecksum FROM inventory.StorageSite";

    private final TapClient<StorageSite> tapClient;
    private final StorageSiteDAO storageSiteDAO;
    private final MessageDigest messageDigest;
    
    private StorageSite currentStorageSite;

    public StorageSiteSync(final StorageSiteDAO storageSiteDAO, URI resourceID, 
            int querySleepInterval, int maxRetryInterval) {
        super(resourceID, querySleepInterval, maxRetryInterval);
        this.storageSiteDAO = storageSiteDAO;
        try {
            this.tapClient = new TapClient<>(resourceID);
        } catch (ResourceNotFoundException ex) {
            throw new IllegalArgumentException("invalid config: query service not found: " + resourceID);
        }
        try {
            this.messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("BUG: " + e.getMessage(), e);
        }
    }

    // ctor for unit tests
    
    StorageSiteSync() {
        super(true);
        this.tapClient = null;
        this.storageSiteDAO = null;
        try {
            this.messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("BUG: " + e.getMessage(), e);
        }
    }

    public StorageSite getCurrentStorageSite() {
        return currentStorageSite;
    }
    
    @Override
    public void doit() throws AccessControlException, ResourceNotFoundException, NotAuthenticatedException, 
            IllegalArgumentException, TransientException, IOException, InterruptedException {
        StorageSite storageSite;
        long t1 = System.currentTimeMillis();
        try (final ResourceIterator<StorageSite> storageSiteIterator = queryStorageSites()) {
            long dt = System.currentTimeMillis() - t1;
            log.info("StorageSite.QUERY dt=" + dt);
            if (storageSiteIterator.hasNext()) {
                storageSite = storageSiteIterator.next();
                // Ensure there is only one returned.
                if (storageSiteIterator.hasNext()) {
                    throw new IllegalStateException("More than one Storage Site found.");
                }
            } else {
                throw new IllegalStateException("No storage sites available to sync.");
            }
        }

        final URI metaChecksum = storageSite.getMetaChecksum();
        final URI computedMetaChecksum = storageSite.computeMetaChecksum(messageDigest);

        if (!metaChecksum.equals(computedMetaChecksum)) {
            throw new IllegalStateException(
                    String.format("Discovered Storage Site checksum (%s) does not match computed value (%s).",
                            metaChecksum, computedMetaChecksum));
        }

        if (currentStorageSite != null && !currentStorageSite.getID().equals(storageSite.getID())) {
            throw new IllegalStateException(
                "StorageSiteSync: entity changed during sync - was: "
                + currentStorageSite.getID() + " aka " + currentStorageSite.getResourceID()
                + " now: " 
                + storageSite.getID() + " aka " + storageSite.getResourceID());
        }
        
        // StorageSite only changes if read/write config changes so optimise to keep logging tidy
        if (currentStorageSite == null 
            || !currentStorageSite.getMetaChecksum().equals(storageSite.getMetaChecksum())) {
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            log.info("StorageSiteSync.putStorageSite id=" + storageSite.getID()
                + " resourceID=" + storageSite.getResourceID()
                + " lastModified=" + df.format(storageSite.getLastModified()));

            storageSiteDAO.put(storageSite);
            this.currentStorageSite = storageSite;
        }
    }

    /**
     * Override this method in tests to avoid recreating a TapClient.
     * @return      Iterator over Storage Sites, or empty Iterator.  Never null.
     * @throws AccessControlException permission denied
     * @throws NotAuthenticatedException authentication attempt failed or rejected
     * @throws ByteLimitExceededException input or output limit exceeded
     * @throws IllegalArgumentException null method arguments or invalid query
     * @throws ResourceNotFoundException remote resource not found
     * @throws TransientException temporary failure of TAP service: same call could work in future
     * @throws IOException failure to send or read data stream
     * @throws InterruptedException thread interrupted
     */
    ResourceIterator<StorageSite> queryStorageSites() throws AccessControlException, ResourceNotFoundException,
                                                             ByteLimitExceededException, NotAuthenticatedException,
                                                             IllegalArgumentException, TransientException, IOException,
                                                             InterruptedException {
        return tapClient.query(STORAGE_SITE_QUERY, row -> {
            int index = 0;
            // column order folllowing model declarations
            final URI rid = (URI) row.get(index++);
            final String name = row.get(index++).toString();
            final boolean allowRead = (Boolean) row.get(index++);
            final boolean allowWrite = (Boolean) row.get(index++);
            final UUID id = (UUID) row.get(index++);

            final StorageSite storageSite = new StorageSite(id, rid, name, allowRead, allowWrite);

            InventoryUtil.assignLastModified(storageSite, (Date) row.get(index++));
            InventoryUtil.assignMetaChecksum(storageSite, (URI) row.get(index));

            return storageSite;
        });
    }
}

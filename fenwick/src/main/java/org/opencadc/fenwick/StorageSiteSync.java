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
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.log.EventLifeCycle;
import ca.nrc.cadc.log.EventLogInfo;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;

import java.io.IOException;
import java.net.URI;
import java.security.AccessControlException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
public class StorageSiteSync {
    private static final Logger log = Logger.getLogger(StorageSiteSync.class);

    private static final String CLASS_NAME = StorageSiteSync.class.getName();
    // column order folllowing model declarations
    private static final String STORAGE_SITE_QUERY =
            "SELECT resourceid, name, allowRead, allowWrite, id, lastmodified, metachecksum FROM inventory.storagesite";

    private final TapClient<StorageSite> tapClient;
    private final StorageSiteDAO storageSiteDAO;

    /**
     * Constructor that accepts a TapClient to query Storage Sites to sync.  Useful for testing.
     *
     * @param tapClient The TapClient to use.
     * @param storageSiteDAO    The DAO for the StorageSite table.  Used for PUTs.
     */
    public StorageSiteSync(final TapClient<StorageSite> tapClient, final StorageSiteDAO storageSiteDAO) {
        this.tapClient = tapClient;
        this.storageSiteDAO = storageSiteDAO;
    }

    /**
     * Query for a Storage Site from a remote TAP (Luskan) service.  This method is expected to return a single
     * StorageSite instance.  The result will typically be used to set to an Artifact's site locations during metadata
     * synchronization.
     *
     * @return StorageSite  There should be a single StorageSite found.
     * @throws AccessControlException permission denied
     * @throws NotAuthenticatedException authentication attempt failed or rejected
     * @throws ByteLimitExceededException input or output limit exceeded
     * @throws IllegalArgumentException null method arguments or invalid query
     * @throws ResourceNotFoundException remote resource not found
     * @throws TransientException temporary failure of TAP service: same call could work in future
     * @throws IOException failure to send or read data stream
     * @throws InterruptedException thread interrupted
     */
    public StorageSite doit() throws AccessControlException, ResourceNotFoundException,
                                     ByteLimitExceededException, NotAuthenticatedException,
                                     IllegalArgumentException, TransientException, IOException,
                                     InterruptedException {
        StorageSite storageSite;
        EventLogInfo eventLogInfo = new EventLogInfo(Main.APPLICATION_NAME, CLASS_NAME, "QUERY");
        long start = System.currentTimeMillis();
        try (final ResourceIterator<StorageSite> storageSiteIterator = queryStorageSites()) {
            eventLogInfo.setElapsedTime(System.currentTimeMillis() - start);
            eventLogInfo.setLifeCycle(EventLifeCycle.CREATE);
            if (storageSiteIterator.hasNext()) {
                storageSite = storageSiteIterator.next();
                eventLogInfo.setEntityID(storageSite.getID());
                eventLogInfo.setSuccess(true);
                // Ensure there is only one returned.
                if (storageSiteIterator.hasNext()) {
                    eventLogInfo.setSuccess(false);
                    log.info(eventLogInfo.singleEvent());
                    throw new IllegalStateException("More than one Storage Site found.");
                }
                log.info(eventLogInfo.singleEvent());
            } else {
                eventLogInfo.setSuccess(false);
                log.info(eventLogInfo.singleEvent());
                throw new IllegalStateException("No storage sites available to sync.");
            }
        }

        try {
            final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            final URI metaChecksum = storageSite.getMetaChecksum();
            final URI computedMetaChecksum = storageSite.computeMetaChecksum(messageDigest);

            if (!metaChecksum.equals(computedMetaChecksum)) {
                throw new IllegalStateException(
                        String.format("Discovered Storage Site checksum (%s) does not match computed value (%s).",
                                      metaChecksum, computedMetaChecksum));
            }

            log.debug("Found Storage Site " + storageSite.getResourceID());
            // Update the local inventory database values for this Storage Site.
            storageSiteDAO.put(storageSite);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return storageSite;
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
        return tapClient.execute(STORAGE_SITE_QUERY, row -> {
            int index = 0;
            // column order folllowing model declarations
            final URI resourceID = (URI) row.get(index++);
            final String name = row.get(index++).toString();
            final boolean allowRead = (Boolean) row.get(index++);
            final boolean allowWrite = (Boolean) row.get(index++);
            final UUID id = (UUID) row.get(index++);

            final StorageSite storageSite = new StorageSite(id, resourceID, name, allowRead, allowWrite);

            InventoryUtil.assignLastModified(storageSite, (Date) row.get(index++));
            InventoryUtil.assignMetaChecksum(storageSite, (URI) row.get(index));

            return storageSite;
        });
    }
}

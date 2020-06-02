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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.fenwick;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.tap.TapClient;
import org.opencadc.tap.TapRowMapper;

/**
 * Class to query the DeletedStorageLocationEvent table using a TAP service
 * and return an iterator over over the query results.
 */
public class DeletedStorageLocationEventSync {

    private static final Logger log = Logger.getLogger(DeletedStorageLocationEventSync.class);

    private static final String DELETED_STORAGE_LOCATION_QUERY =
            "SELECT id, lastModified, metaChecksum FROM inventory.DeletedStorageLocationEvent"
                + " %s order by lastModified";

    private final TapClient<DeletedStorageLocationEvent> tapClient;
    public Date startTime;

    /**
     * Constructor
     *
     * @param tapClient The TAP client.
     */
    public DeletedStorageLocationEventSync(TapClient<DeletedStorageLocationEvent> tapClient) {
        this.tapClient = tapClient;
    }

    /**
     * Query the DeletedStorageLocationEvent table for rows with a lastModified date after the given start date,
     * and return an iterator over the rows.
     *
     * @return Iterator over the query results.
     * @throws InterruptedException thread interrupted
     * @throws IOException failure to write or read the data stream
     * @throws ResourceNotFoundException remote resource not found
     * @throws TransientException temporary failure of TAP service: same call could work in future
     */
    public ResourceIterator<DeletedStorageLocationEvent> getEvents()
            throws InterruptedException, IOException, ResourceNotFoundException, TransientException {

        String where;
        if (this.startTime == null) {
            where = "";
        } else {
            where = "WHERE lastModified >= '" + getDateFormat().format(this.startTime) + "'";
        }
        final String query = String.format(DELETED_STORAGE_LOCATION_QUERY, where);
        log.debug("tap query: " + query);
        return tapClient.execute(query, new DeletedStorageLocationEventRowMapper());
    }

    /**
     * Get a DateFormat instance.
     *
     * @return ISO DateFormat in UTC.
     */
    protected DateFormat getDateFormat() {
        return DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
    }

    /**
     * Class to map the query results to a DeletedStorageLocationEvent.
     */
    static class DeletedStorageLocationEventRowMapper implements TapRowMapper<DeletedStorageLocationEvent> {

        @Override
        public DeletedStorageLocationEvent mapRow(List<Object> row) {
            int index = 0;
            final UUID id = (UUID) row.get(index++);
            final Date lastModified = (Date) row.get(index++);
            final URI metaChecksum = (URI) row.get(index);

            final DeletedStorageLocationEvent deletedStorageLocationEvent = new DeletedStorageLocationEvent(id);
            InventoryUtil.assignLastModified(deletedStorageLocationEvent, lastModified);
            InventoryUtil.assignMetaChecksum(deletedStorageLocationEvent, metaChecksum);

            return deletedStorageLocationEvent;
        }
    }

}



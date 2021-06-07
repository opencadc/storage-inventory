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

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.StringUtil;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.tap.TapClient;
import org.opencadc.tap.TapRowMapper;


/**
 * Artifact sync class.  This class is responsible for querying a remote TAP (Luskan) site to obtain desired Artifact
 * instances to then be used to store in the local inventory.
 */
public class ArtifactSync {

    private static final Logger LOGGER = Logger.getLogger(ArtifactSync.class);

    private final TapClient<Artifact> tapClient;
    

    // Mutable field to use in the query.  Will be AND'd to the query but isolated in parentheses.
    public String includeClause;
    public Date startTime;
    public Date endTime;

    /**
     * Complete constructor.
     *
     * @param tapClient        The TapClient to interface with a site's TAP (luskan) service.
     */
    public ArtifactSync(final TapClient<Artifact> tapClient) {
        this.tapClient = tapClient;
    }

    /**
     * Execute the query and return the iterator back.
     *
     * @return ResourceIterator over Artifact instances matching this sync's constraint(s).
     *
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    public ResourceIterator<Artifact> iterator()
            throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                   InterruptedException {
        final String query = buildQuery();
        LOGGER.debug("\nExecuting query '" + query + "'\n");
        return tapClient.execute(query, new ArtifactRowMapper());
    }

    /**
     * Assemble the WHERE clause and return the full query.  Very useful for testing separately.
     * @return  String query.  Never null.
     */
    String buildQuery() {
        final StringBuilder query = new StringBuilder();
        query.append("SELECT id, uri, contentChecksum, contentLastModified, contentLength, contentType, ")
                   .append("contentEncoding, lastModified, metaChecksum FROM inventory.Artifact");

        if (this.startTime != null) {
            LOGGER.debug("\nInjecting lastModified date compare '" + this.startTime + "'\n");
            query.append(" WHERE lastModified >= '").append(getDateFormat().format(this.startTime)).append("'");

            if (this.endTime != null) {
                query.append(" AND ").append("lastModified < '").append(
                        getDateFormat().format(this.endTime)).append("'");
            }
        } else if (this.endTime != null) {
            query.append(" WHERE ").append("lastModified < '").append(getDateFormat().format(this.endTime)).append("'");
        }

        if (StringUtil.hasText(this.includeClause)) {
            LOGGER.debug("\nInjecting clause '" + this.includeClause + "'\n");

            if (query.indexOf("WHERE") < 0) {
                query.append(" WHERE ");
            } else {
                query.append(" AND ");
            }

            query.append("(").append(this.includeClause.trim()).append(")");
        }

        query.append(" ORDER BY lastModified");

        return query.toString();
    }

    /**
     * Get a DateFormat instance.
     *
     * @return ISO DateFormat in UTC.
     */
    protected DateFormat getDateFormat() {
        return DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
    }

    private static final class ArtifactRowMapper implements TapRowMapper<Artifact> {

        /**
         * Map raw row data into a domain object. The values in the row (list) will
         * already be converted from text or binary into suitable immutable value objects,
         * such as Double, Integer, String... or extended types (DALI xtypes) (Point,
         * Circle, Date, etc.) or custom xtypes (URI, UUID, etc.) supported by the cadc-dali
         * library. The presence of null values is allowed and depends entirely on the TAP
         * table that was queried. The number and order of values is consistent with the
         * list of items in the select clause of the ADQL query.
         *
         * @param row list of values for one row
         * @return domain-specific value object created for single row
         */
        @Override
        public Artifact mapRow(final List<Object> row) {
            int index = 0;
            final Artifact artifact = new Artifact((UUID) row.get(index++),
                                                   (URI) row.get(index++),
                                                   (URI) row.get(index++),
                                                   (Date) row.get(index++),
                                                   (Long) row.get(index++));

            artifact.contentType = (String) row.get(index++);
            artifact.contentEncoding = (String) row.get(index++);

            InventoryUtil.assignLastModified(artifact, (Date) row.get(index++));
            InventoryUtil.assignMetaChecksum(artifact, (URI) row.get(index));

            return artifact;
        }
    }
}

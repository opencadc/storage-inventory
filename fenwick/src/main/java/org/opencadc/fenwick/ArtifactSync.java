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

import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.tap.TapClient;


/**
 * Artifact sync class.  This class is responsible for querying a remote TAP (Luskan) site to obtain desired Artifact
 * instances to then be used to store in the local inventory.
 */
public class ArtifactSync {

    private static final Logger LOGGER = Logger.getLogger(ArtifactSync.class);

    // First string replace is the date to verify, and the second is the constraint clause as set by the selector.
    private static final String ARTIFACT_QUERY_TEMPLATE =
            "SELECT id, uri, contentchecksum, contentlastmodified, contentlength, contenttype, "
            + "contentencoding, sitelocations, storagelocation_storageid, storagelocation_storagebucket, lastmodified, "
            + "metachecksum WHERE lastModified >= %s%s";

    private final ArtifactSelector selector;
    private final TapClient<Artifact> tapClient;
    private final Date currLastModified;

    /**
     * Complete constructor.
     *
     * @param selector         The ArtifactSelector used to constrain the query.
     * @param tapClient        The TapClient to interface with a site's TAP (Luskan) service.
     * @param currLastModified The last modified date to start the query at.
     */
    public ArtifactSync(final ArtifactSelector selector, final TapClient<Artifact> tapClient,
                        final Date currLastModified) {
        this.selector = selector;
        this.tapClient = tapClient;
        this.currLastModified = currLastModified;
    }

    /**
     * Execute the query and return the iterator back.
     *
     * @return ResourceIterator over Artifact instances matching the selector's query parameters.
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

        final String query = assembleQuery();
        LOGGER.debug("\nExecuting query '" + query + "'\n");
        return tapClient.execute(query, row -> {
            int index = 0;
            final Artifact artifact = new Artifact((UUID) row.get(index++),
                                                   URI.create(row.get(index++).toString()),
                                                   URI.create(row.get(index++).toString()),
                                                   (Date) row.get(index++),
                                                   (Long) row.get(index++));
            artifact.contentType = row.get(index++).toString();
            artifact.contentEncoding = row.get(index++).toString();
            artifact.siteLocations.addAll((Set<SiteLocation>) row.get(index++));

            final StorageLocation storageLocation = new StorageLocation(URI.create(row.get(index++).toString()));
            storageLocation.storageBucket = row.get(index++).toString();

            InventoryUtil.assignMetaChecksum(artifact, URI.create(row.get(index++).toString()));
            InventoryUtil.assignLastModified(artifact, (Date) row.get(index));

            return artifact;
        });
    }

    /**
     * Create a query for the TAP (Luskan) query.  This method will tally up the selector's clauses into a formulated
     * query for the TapClient.
     * <p>A potential risk with this method is that there is no enforcement for at least one clause.  This could result
     * in a very large query.
     *
     * @return String ADQL query with expected WHERE clauses.
     *
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     */
    private String assembleQuery() throws ResourceNotFoundException, IOException, IllegalStateException {
        final StringBuilder whereClauseBuilder = new StringBuilder();
        final Iterator<String> whereClauseIterator = selector.iterator();

        whereClauseIterator.forEachRemaining(clause -> whereClauseBuilder.append(" AND (").append(clause.trim())
                                                                         .append(")"));

        return String.format(ARTIFACT_QUERY_TEMPLATE, currLastModified, whereClauseBuilder.toString());
    }
}

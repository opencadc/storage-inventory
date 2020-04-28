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
************************************************************************
*/

package org.opencadc.inventory.db;

import ca.nrc.cadc.io.ResourceIterator;

import java.net.URI;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 * @author pdowler
 */
public class ArtifactDAO extends AbstractDAO<Artifact> {
    private static final Logger log = Logger.getLogger(ArtifactDAO.class);

    public ArtifactDAO() {
        super();
    }

    public ArtifactDAO(AbstractDAO dao) {
        super(dao);
    }
    
    public Artifact get(UUID id) {
        return super.get(Artifact.class, id);
    }
    
    public Artifact get(URI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("uri cannot be null");
        }
        checkInit();
        log.debug("get: " + uri);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            
            SQLGenerator.ArtifactGet get = (SQLGenerator.ArtifactGet) gen.getEntityGet(Artifact.class);
            get.setURI(uri);
            Artifact a = get.execute(jdbc);
            return a;
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("get: " + uri + " " + dt + "ms");
        }
    }
    
    // delete an artifact, all SiteLocation(s), and StorageLocation
    // caller must also fire an appropriate event via DeletedEventDAO in same txn
    // unless performing this delete in reaction to such an event
    public void delete(UUID id) {
        super.delete(Artifact.class, id);
    }
    
    /**
     * Iterate over Artifacts in StorageLocation order. This only shows artifacts with
     * a storageLocation value and is used to validate inventory vs storage.
     * 
     * @param storageBucketPrefix null, prefix, or complete storageBucket string
     * @return iterator over artifacts sorted by StorageLocation
     */
    public ResourceIterator storedIterator(String storageBucketPrefix) {
        checkInit();
        log.debug("iterator: " + storageBucketPrefix);
        long t = System.currentTimeMillis();

        try {
            SQLGenerator.ArtifactIteratorQuery iter = (SQLGenerator.ArtifactIteratorQuery) gen.getEntityIteratorQuery(Artifact.class, true);
            iter.setPrefix(storageBucketPrefix);
            return iter.query(dataSource);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("iterator: " + storageBucketPrefix + " " + dt + "ms");
        }
    }
    
    /**
     * Iterate over Artifacts with no StorageLocation. This only returns artifacts with
     * no storageLocation value so the content can be downloaded and stored locally.
     * 
     * @param uriBucketPrefix null, prefix, or complete Artifact.uriBucket string
     * @return iterator over artifacts with no StorageLocation
     */
    public ResourceIterator unstoredIterator(String uriBucketPrefix) {
        checkInit();
        //log.debug("iterator: ");
        long t = System.currentTimeMillis();

        try {
            SQLGenerator.ArtifactIteratorQuery iter = (SQLGenerator.ArtifactIteratorQuery) gen.getEntityIteratorQuery(Artifact.class, false);
            iter.setPrefix(uriBucketPrefix);
            return iter.query(dataSource);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("iterator: " + dt + "ms");
        }
    }
}

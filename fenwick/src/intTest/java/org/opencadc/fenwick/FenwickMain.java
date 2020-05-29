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

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;

import java.io.File;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedEventDAO;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.inventory.db.HarvestStateDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.storage.StorageMetadata;


public class FenwickMain {
    private static final Logger LOGGER = Logger.getLogger(StorageSiteSyncTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.fenwick", Level.DEBUG);
    }

    // The local inventory database DAOs.
    private final ArtifactDAO artifactDAO = new ArtifactDAO();
    private final DeletedEventDAO<DeletedArtifactEvent> deletedArtifactEventDAO = new DeletedEventDAO<>();
    private final DeletedEventDAO<DeletedStorageLocationEvent> deletedStorageLocationEventDAO = new DeletedEventDAO<>();
    private final HarvestStateDAO harvestStateDAO = new HarvestStateDAO();


    public FenwickMain() throws Exception {
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
        DBUtil.createJNDIDataSource("jdbc/FenwickIntTest", cc);

        Map<String,Object> config = new TreeMap<String,Object>();
        config.put(SQLGenerator.class.getName(), SQLGenerator.class);
        config.put("jndiDataSourceName", "jdbc/FenwickIntTest");
        config.put("database", TestUtil.DATABASE);
        config.put("schema", TestUtil.SCHEMA);

        artifactDAO.setConfig(config);
        deletedArtifactEventDAO.setConfig(config);
        deletedStorageLocationEventDAO.setConfig(config);
        harvestStateDAO.setConfig(config);
    }

    private void wipe_clean(Iterator<Artifact> artifactIterator) {
        LOGGER.debug("wipe_clean running...");
        int deletedArtifacts = 0;

        while (artifactIterator.hasNext()) {
            LOGGER.debug("something to remove...");
            Artifact a = artifactIterator.next();
            LOGGER.debug("deleting test uri: " + a.getURI() + " ID: " + a.getID());
            artifactDAO.delete(a.getID());
            deletedArtifacts++;
        }

        if (deletedArtifacts > 0) {
            LOGGER.info("deleted " + deletedArtifacts + " artifacts");
        }
    }

    @Before
    public void cleanTestEnvironment() {
        LOGGER.debug("cleaning stored artifacts...");
        Iterator<Artifact> storedArtifacts = artifactDAO.storedIterator(null);
        LOGGER.debug("got an iterator back: " + storedArtifacts);
        wipe_clean(storedArtifacts);

        LOGGER.debug("cleaning unstored artifacts...");
        Iterator<Artifact> unstoredArtifacts = artifactDAO.unstoredIterator(null);
        LOGGER.debug("got an iterator back: " + storedArtifacts);
        wipe_clean(unstoredArtifacts);

        final HarvestState existingHarvestState =
                harvestStateDAO.get(Artifact.class.getName(), URI.create(TestUtil.LUSKAN_URI));
        if (existingHarvestState != null) {
            harvestStateDAO.delete(existingHarvestState.getID());
        }
    }

    @Test
    public void runThrough() throws Exception {
        Main.main(new String[0]);
    }
}

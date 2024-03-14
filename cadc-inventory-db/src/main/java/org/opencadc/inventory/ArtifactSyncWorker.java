/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

package org.opencadc.inventory;

import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.inventory.db.HarvestStateDAO;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.db.NodeDAO;

/**
 * This class performs the work of synchronizing the size of Data Nodes between backend storage and Node Persistence
 * 
 * @author adriand
 */
public class ArtifactSyncWorker implements Runnable {
    private static final Logger log = Logger.getLogger(ArtifactSyncWorker.class);

    private final HarvestState harvestState;
    private final NodeDAO nodeDAO;
    private final ArtifactDAO artifactDAO;
    private final HarvestStateDAO harvestStateDAO;
    private final Namespace storageNamespace;

    public ArtifactSyncWorker(HarvestStateDAO harvestStateDAO, HarvestState harvestState, ArtifactDAO artifactDAO,
                              Namespace namespace) {
        this.harvestState = harvestState;
        this.harvestStateDAO = harvestStateDAO;
        this.nodeDAO = new NodeDAO(harvestStateDAO);
        this.artifactDAO = artifactDAO;
        this.storageNamespace = namespace;
    }

    @Override
    public void run() {
        log.debug("Start harvesting " + harvestState.toString() + " at " + harvestState.curLastModified);

        TransactionManager tm = nodeDAO.getTransactionManager();
        try (final ResourceIterator<Artifact> iter = artifactDAO.iterator(storageNamespace, null,
                harvestState.curLastModified, true)) {
            while (iter.hasNext()) {
                Artifact artifact = iter.next();
                DataNode node = nodeDAO.getDataNode(artifact.getURI());
                if ((node != null) && !artifact.getContentLength().equals(node.bytesUsed)) {
                    node.bytesUsed = artifact.getContentLength();
                    tm.startTransaction();
                    try {
                        nodeDAO.put(node);
                        harvestState.curLastModified = artifact.getLastModified();
                        harvestState.curID = node.getID();
                        harvestStateDAO.put(harvestState, true);
                        tm.commitTransaction();
                        log.debug("Updated size of data node " + node.getName());
                    } catch (Exception ex) {
                        log.debug("Failed to update data node size for " + node.getName(), ex);
                        tm.rollbackTransaction();
                        throw ex;
                    } finally {
                        if (tm.isOpen()) {
                            log.error("BUG: transaction open in finally. Rolling back...");
                            tm.rollbackTransaction();
                            log.error("Rollback: OK");
                            throw new RuntimeException("BUG: transaction open in finally");
                        }
                    }
                }
            }
        } catch (IOException ex) {
            log.error("Error closing iterator", ex);
            throw new RuntimeException("error while closing ResourceIterator", ex);
        }
        log.debug("End harvesting " + harvestState.toString() + " at " + harvestState.curLastModified);
    }
}

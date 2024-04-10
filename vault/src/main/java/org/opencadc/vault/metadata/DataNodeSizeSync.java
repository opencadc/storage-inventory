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

package org.opencadc.vault.metadata;

import java.net.URI;
import java.util.Date;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.HarvestState;
import org.opencadc.inventory.db.HarvestStateDAO;
import org.opencadc.vospace.db.DataNodeSizeWorker;

/**
 * Main artifact-sync agent that enables incremental sync of Artifact
 * metadata to Node.
 * 
 * @author pdowler
 */
public class DataNodeSizeSync implements Runnable {
    private static final Logger log = Logger.getLogger(DataNodeSizeSync.class);

    private static final long ROUNDS = 6000L; // 6 sec
    private static final long SHORT_SLEEP = 5 * ROUNDS;
    private static final long LONG_SLEEP = 10 * ROUNDS;
    private static final long EVICT_AGE = 12 * ROUNDS;
    
    private static final long FAIL_SLEEP = 10 * ROUNDS; // slightly smaller than evict
    
    private final UUID instanceID = UUID.randomUUID();
    private final HarvestStateDAO stateDAO;
    private final ArtifactDAO artifactDAO;
    private final Namespace artifactNamespace;
    private final boolean isStorageSite;
    
    private boolean offline = false;
    
    public DataNodeSizeSync(HarvestStateDAO stateDAO, ArtifactDAO artifactDAO, Namespace artifactNamespace, boolean isStorageSite) { 
        this.stateDAO = stateDAO;
        this.artifactDAO = artifactDAO;
        this.artifactNamespace = artifactNamespace;
        this.isStorageSite = isStorageSite;
        
        // we need continuous timestamp updates to retain leader status, so only schedule maintenance
        stateDAO.setUpdateBufferCount(0);
        stateDAO.setMaintCount(9999); // every 1e4
    }

    public void setOffline(boolean offline) {
        this.offline = offline;
    }

    @Override
    public void run() {
        String name = Artifact.class.getSimpleName();
        URI resourceID = URI.create("jdbc:inventory");
        try {
            Thread.sleep(1 * ROUNDS); // delay startup a bit
            
            while (true) {
                while (offline) {
                    log.warn("disabled: sleep=" + LONG_SLEEP);
                    Thread.sleep(LONG_SLEEP);
                }

                log.debug("check leader " + instanceID);
                HarvestState state = stateDAO.get(name, resourceID);
                log.debug("check leader " + instanceID + " found: " + state);
                if (state.instanceID == null) {
                    state.instanceID = instanceID;
                    stateDAO.put(state);
                    state = stateDAO.get(state.getID());
                    log.debug("created: " + state);
                }
                
                long t1 = System.currentTimeMillis();
                BackgroundLogInfo logInfo = new BackgroundLogInfo(instanceID.toString());
                logInfo.setOperation(DataNodeSizeWorker.class.getSimpleName());
                logInfo.setSuccess(false);
                
                // determine leader
                boolean leader = checkLeaderStatus(state);
                logInfo.leader = leader;
                logInfo.setLastModified(state.curLastModified);
                long sleep = LONG_SLEEP;
                if (leader) {
                    log.debug("leader: " + state);
                    boolean fail = false;
                    log.info(logInfo.start());
                    try {
                        DataNodeSizeWorker worker = new DataNodeSizeWorker(stateDAO, state, artifactDAO, artifactNamespace, isStorageSite);
                        worker.run();
                        logInfo.setLastModified(state.curLastModified);
                        logInfo.processed = worker.getNumArtifactsProcessed();
                    } catch (Exception ex) {
                        log.error("unexpected worker fail", ex);
                        fail = true;
                    }
                    
                    leader = checkLeaderStatus(state);
                    if (!fail && leader) {
                        try {
                            stateDAO.flushBufferedState();
                        } catch (Exception ex) {
                            log.error("unexpected HarvestState flush fail", ex);
                            fail = true;
                        }
                    }
                    
                    if (!fail && leader) {
                        try {
                            // update leader timestamp before sleeping
                            stateDAO.put(state, true);
                        } catch (Exception ex) {
                            log.error("unexpected HarvestState force update fail", ex);
                            fail = true;
                        }
                    }

                    if (fail) {
                        sleep = FAIL_SLEEP;
                    } else {
                        sleep = SHORT_SLEEP;
                    }
                    logInfo.setSuccess(!fail);
                    logInfo.setElapsedTime(System.currentTimeMillis() - t1);
                } else {
                    // not leader success
                    sleep = LONG_SLEEP;
                    logInfo.setSuccess(true);
                }
                logInfo.sleep = sleep;
                log.info(logInfo.end());
                Thread.sleep(sleep);
            }
        } catch (InterruptedException ex) {
            log.debug("interrupted - assuming shutdown", ex);
        }
    }
    
    private boolean checkLeaderStatus(HarvestState state) {
        boolean leader = false;
        if (instanceID.equals(state.instanceID)) {
            stateDAO.put(state, true);
            leader = true;
        } else {
            // see if we should perform a coup...
            Date now = new Date();
            long age = now.getTime() - state.getLastModified().getTime();
            if (age > EVICT_AGE) {
                log.info("EVICTING " + state.instanceID + " because age " + age + " > " + EVICT_AGE);
                state.instanceID = instanceID;
                stateDAO.put(state);
                leader = true;
            }
        }
        return leader;
    }
}

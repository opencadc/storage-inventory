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
************************************************************************
*/

package org.opencadc.fenwick;

import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.db.ArtifactDAO;

/**
 *
 * @author pdowler
 */
public class InventoryHarvester {
    private static final Logger log = Logger.getLogger(InventoryHarvester.class);

    private final ArtifactDAO artifactDAO;
    private final URI resourceID;
    private final Capability inventoryTAP;
    private final ArtifactSelector selector;
    
    /**
     * Constructor.
     * 
     * @param daoConfig config map to pass to cadc-inventory-db DAO classes
     * @param resourceID identifier for the remote query service
     * @param selector selector implementation
     */
    public InventoryHarvester(Map<String,Object> daoConfig, URI resourceID, ArtifactSelector selector) {
        InventoryUtil.assertNotNull(InventoryHarvester.class, "daoConfig", daoConfig);
        InventoryUtil.assertNotNull(InventoryHarvester.class, "resourceID", resourceID);
        InventoryUtil.assertNotNull(InventoryHarvester.class, "selector", selector);
        this.artifactDAO = new ArtifactDAO();
        artifactDAO.setConfig(daoConfig);
        this.resourceID = resourceID;
        this.selector = selector;
        
        try {
            RegistryClient rc = new RegistryClient();
            Capabilities caps = rc.getCapabilities(resourceID); 
            // above call throws IllegalArgumentException... should be ResourceNotFoundException but out of scope to fix
            this.inventoryTAP = caps.findCapability(Standards.TAP_10);
            if (inventoryTAP == null) {
                throw new IllegalArgumentException("invalid config: remote query service " + resourceID + " does not implement " + Standards.TAP_10);
            }
        } catch (IOException ex) {
            throw new IllegalArgumentException("invalid config", ex);
        }
    }
    
    // general behaviour is that this process runs continually and manages it's own schedule
    // - harvest everything up to *now*
    // - go idle for a dynamically determined amount of time
    // - repeat until fail/killed
    
    public void run() {
        while (true) {
            try {
                doit();
                
                // TODO: dynamic depending on how rapidly the remote content is changing
                // ... this value and the reprocess-last-N-seconds should be related
                long dt = 60 * 1000L;
                Thread.sleep(dt);
            } catch (InterruptedException ex) {
                throw new RuntimeException("interrupted", ex);
            }
        }
    }
    
    // use TapClient to get remote StorageSite record and store it locally
    // - only global inventory needs to track remote StorageSite(s); should be exactly one (minoc) record at a storage site
    // - could be a harmless simplification to ignore the fact that storage sites don't need to sync other StorageSite records from global
    // - keep the StorageSite UUID for creating SiteLocation objects below

    // get tracked progress (latest timestamp seen from any iterator)
    // - the head of an iterator is volatile: events can be inserted that are slightly out of monotonic time sequence
    // - actions taken while processing are idempotent
    // - therefore: it should be OK to re-process the last N seconds (eg startTime = curLastModfied - 120 seconds)

    // use TapClient to open multiple iterator streams: Artifact, DeletedArtifactEvent, DeletedStorageLocationEvent
    // - incremental: use latest timestamp from above for all iterators
    // - selective: use ArtifactSelector to modify the query for Iterator<Artifact>

    // process multiple iterators in timestamp order
    // - taking appropriate action for each track progress
    // - stop iterating when reaching a timestamp that exceeds the timestamp when query executed
    
    private void doit() {
        throw new UnsupportedOperationException("TODO");
    }
    
}

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

package org.opencadc.critwall;

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
import org.opencadc.inventory.storage.StorageAdapter;

/**
 *
 * @author pdowler
 */
public class FileSync {
    private static final Logger log = Logger.getLogger(FileSync.class);

    private static final int MAX_THREADS = 16;
    
    private final ArtifactDAO artifactDAO;
    private StorageAdapter localStorage;
    private final URI resourceID;
    private final Capability locator;
    private final BucketSelector selector;
    private final int nthreads;
    
    /**
     * Constructor.
     * 
     * @param daoConfig config map to pass to cadc-inventory-db DAO classes
     * @param localStorage adapter to put to local storage
     * @param resourceID identifier for the remote query service
     * @param selector selector implementation
     * @param nthreads number of threads in download thread pool
     */
    public FileSync(Map<String,Object> daoConfig, StorageAdapter localStorage, URI resourceID, BucketSelector selector, int nthreads) {
        InventoryUtil.assertNotNull(FileSync.class, "daoConfig", daoConfig);
        InventoryUtil.assertNotNull(FileSync.class, "localStorage", localStorage);
        InventoryUtil.assertNotNull(FileSync.class, "resourceID", resourceID);
        InventoryUtil.assertNotNull(FileSync.class, "selector", selector);

        if (nthreads <= 0 || nthreads > MAX_THREADS) {
            throw new IllegalArgumentException("invalid config: nthreads must be in [1," + MAX_THREADS + "], found: " + nthreads);
        }

        this.artifactDAO = new ArtifactDAO();
        artifactDAO.setConfig(daoConfig);
        this.resourceID = resourceID;
        this.selector = selector;
        this.nthreads = nthreads;

        throw new UnsupportedOperationException("TODO");

        // To be completed in s2575, ta 13061
        //        try {
        //            RegistryClient rc = new RegistryClient();
        //            Capabilities caps = rc.getCapabilities(resourceID);
        //            // above call throws IllegalArgumentException... should be ResourceNotFoundException but out of scope to fix
        //            this.locator = caps.findCapability(Standards.SI_LOCATE);
        //            if (locator == null) {
        //                throw new IllegalArgumentException("invalid config: remote query service " + resourceID + " does not implement " + Standards.SI_LOCATE);
        //            }
        //        } catch (IOException ex) {
        //            throw new IllegalArgumentException("invalid config", ex);
        //        }
    }
    
    // general behaviour:
    // - create a job queue
    // - create a thread pool to execute jobs (ta 13063)
    // - query inventory for Artifact with null StorageLocation and use Iterator<Artifact>
    //   to keep the queue finite in size (not empty, not huge)
    // job: transfer negotiation  (with global) + HttpGet with output to local StorageAdapter
    // - the job wrapper should balance HttpGet retries to a single URL and cycling through each
    //   negotiated URL (once)... so if more URLs to try, fewer retries each (separate task)
    // - if a job fails, just log it and move on
    
    // run until Iterator<Artifact> finishes: 
    // - terminate?
    // - manage idle and run until serious failure?
    
    public void run() {
        throw new UnsupportedOperationException("TODO");
    }



}

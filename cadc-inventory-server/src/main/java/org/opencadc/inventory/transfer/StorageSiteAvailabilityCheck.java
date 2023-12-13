/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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

package org.opencadc.inventory.transfer;

import ca.nrc.cadc.vosi.Availability;
import ca.nrc.cadc.vosi.AvailabilityClient;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.log4j.Logger;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.StorageSiteDAO;

/**
 * Background check of storage site availability. This class stores and
 * maintains a Map of site availability check results. ProtocolsGenerator 
 * consults the map when generating URLs to files so it can skip sites that
 * are off-line.
 * 
 * @author pdowler
 */
public class StorageSiteAvailabilityCheck implements Runnable {
    private static final Logger log = Logger.getLogger(StorageSiteAvailabilityCheck.class);

    static final int AVAILABILITY_CHECK_TIMEOUT = 30; //secs
    static final int AVAILABILITY_FULL_CHECK_TIMEOUT = 300; //secs
    
    private final StorageSiteDAO storageSiteDAO;
    private final Map<URI, SiteState> siteStates;
    private final Map<URI, Availability> siteAvailabilities;

    public StorageSiteAvailabilityCheck(StorageSiteDAO storageSiteDAO, String siteAvailabilitiesKey) {
        this.storageSiteDAO = storageSiteDAO;
        this.siteStates = new HashMap<>();
        this.siteAvailabilities = new HashMap<>();

        try {
            Context initialContext = new InitialContext();
            // check if key already bound, if so unbind
            try {
                initialContext.unbind(siteAvailabilitiesKey);
            } catch (NamingException ignore) {
                // ignore
            }
            initialContext.bind(siteAvailabilitiesKey, this.siteAvailabilities);
        } catch (NamingException e) {
            throw new IllegalStateException(String.format("unable to bind %s to initial context: %s",
                    siteAvailabilitiesKey, e.getMessage()), e);
        }
    }

    @Override
    public void run() {
        int lastSiteQuerySecs = 0;
        while (true) {
            Set<StorageSite> sites = storageSiteDAO.list();
            if (lastSiteQuerySecs >= AVAILABILITY_FULL_CHECK_TIMEOUT) {
                sites = storageSiteDAO.list();
                lastSiteQuerySecs = 0;
            } else {
                lastSiteQuerySecs += AVAILABILITY_CHECK_TIMEOUT;
            }

            for (StorageSite site : sites) {
                URI resourceID = site.getResourceID();
                log.debug("checking site: " + resourceID);
                SiteState siteState = this.siteStates.get(resourceID);
                if (siteState == null) {
                    siteState = new SiteState(false, 0);
                }
                boolean minDetail = siteState.isMinDetail();
                Availability availability;
                try {
                    availability = getAvailability(resourceID, minDetail);
                } catch (Exception e) {
                    availability = new Availability(false, e.getMessage());
                    log.debug(String.format("failed %s - %s", resourceID, e.getMessage()));
                }
                final boolean prev = siteState.available;
                siteState.available = availability.isAvailable();
                this.siteStates.put(resourceID, siteState);
                this.siteAvailabilities.put(resourceID, availability);
                String message = String.format("%s %s - %s", minDetail ? "MIN" : "FULL",
                        resourceID, siteState.available ? "UP" : "DOWN");
                if (!siteState.available) {
                    log.warn(message);
                } else if (prev != siteState.available) {
                    log.info(message);
                } else {
                    log.debug(message);
                }
            }

            try {
                log.debug(String.format("sleep availability checks for %d secs", AVAILABILITY_CHECK_TIMEOUT));
                Thread.sleep(AVAILABILITY_CHECK_TIMEOUT * 1000);
            } catch (InterruptedException e) {
                throw new IllegalStateException("AvailabilityCheck thread interrupted during sleep");
            }
        }
    }

    private Availability getAvailability(URI resourceID, boolean minDetail) {
        AvailabilityClient client = new AvailabilityClient(resourceID, minDetail);
        return client.getAvailability();
    }

    private class SiteState {

        public boolean available;
        public int lastFullCheckSecs;

        public SiteState(boolean available, int lastFullCheckSecs) {
            this.available = available;
            this.lastFullCheckSecs = lastFullCheckSecs;
        }

        public boolean isMinDetail() {
            log.debug(String.format("isMinDetail() available=%b, lastFullCheckSecs=%d",
                    available, lastFullCheckSecs));
            if (this.available && this.lastFullCheckSecs < AVAILABILITY_FULL_CHECK_TIMEOUT) {
                this.lastFullCheckSecs += AVAILABILITY_CHECK_TIMEOUT;
                return true;
            }
            this.lastFullCheckSecs = 0;
            return false;
        }
    }
}

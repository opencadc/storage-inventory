/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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

package org.opencadc.raven;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.vosi.Availability;
import ca.nrc.cadc.vosi.AvailabilityPlugin;
import ca.nrc.cadc.vosi.avail.CheckCertificate;
import ca.nrc.cadc.vosi.avail.CheckException;
import ca.nrc.cadc.vosi.avail.CheckResource;
import ca.nrc.cadc.vosi.avail.CheckWebService;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.NoSuchElementException;
import org.apache.log4j.Logger;

/**
 * This class performs the work of determining if the executing locate
 * service is operating as expected.
 * 
 * @author majorb
 */
public class ServiceAvailability implements AvailabilityPlugin {

    private static final Logger log = Logger.getLogger(ServiceAvailability.class);
    private static final File AAI_PEM_FILE = new File(System.getProperty("user.home") + "/.ssl/cadcproxy.pem");
    
    /**
     * Default, no-arg constructor.
     */
    public ServiceAvailability() {
    }

    /**
     * Sets the name of the application.
     * @param string unique application name
     */
    @Override
    public void setAppName(String string) {
        //no-op
    }

    /**
     * Performs a simple check for the availability of the object.
     * @return true always
     */
    @Override
    public boolean heartbeat() {
        return true;
    }

    /**
     * Do a comprehensive check of the service and it's dependencies.
     * @return Information of the availability check.
     */
    @Override
    public Availability getStatus() {
        boolean isGood = true;
        String note = "service is accepting requests";
        
        try {
            // check other services we depend on
            RegistryClient reg = new RegistryClient();
            LocalAuthority localAuthority = new LocalAuthority();

            URI credURI = null;
            try {
                credURI = localAuthority.getServiceURI(Standards.CRED_PROXY_10.toString());
                URL url = reg.getServiceURL(credURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
                if (url != null) {
                        CheckResource checkResource = new CheckWebService(url);
                        checkResource.check();
                    } else {
                        log.debug("check skipped: " + credURI + " does not provide " + Standards.VOSI_AVAILABILITY);
                    }
            } catch (NoSuchElementException ex) {
                log.debug("not configured: " + Standards.CRED_PROXY_10);
            }

            URI usersURI = null;
            try {
                usersURI = localAuthority.getServiceURI(Standards.UMS_USERS_01.toString());
                URL url = reg.getServiceURL(usersURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
                if (url != null) {
                        CheckResource checkResource = new CheckWebService(url);
                        checkResource.check();
                    } else {
                        log.debug("check skipped: " + usersURI + " does not provide " + Standards.VOSI_AVAILABILITY);
                    }
            } catch (NoSuchElementException ex) {
                log.debug("not configured: " + Standards.CRED_PROXY_10);
            }

            URI groupsURI = null;
            try {
                groupsURI = localAuthority.getServiceURI(Standards.GMS_SEARCH_01.toString());
                if (!groupsURI.equals(usersURI)) {
                    URL url = reg.getServiceURL(groupsURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
                    if (url != null) {
                        CheckResource checkResource = new CheckWebService(url);
                        checkResource.check();
                    } else {
                        log.debug("check skipped: " + groupsURI + " does not provide " + Standards.VOSI_AVAILABILITY);
                    }
                }
            } catch (NoSuchElementException ex) {
                log.debug("not configured: " + Standards.CRED_PROXY_10);
            }

            if (credURI != null || usersURI != null) {
                if (AAI_PEM_FILE.exists() && AAI_PEM_FILE.canRead()) {
                    // check for a certificate needed to perform network A&A ops
                    CheckCertificate checkCert = new CheckCertificate(AAI_PEM_FILE);
                    checkCert.check();
                } else {
                    log.debug("AAI cert not found or unreadable");
                }
            }
            
        } catch (CheckException ce) {
            // tests determined that the resource is not working
            isGood = false;
            note = ce.getMessage();
        } catch (Throwable t) {
            // the test itself failed
            log.debug("failure", t);
            isGood = false;
            note = "test failed, reason: " + t;
        }

        return new Availability(isGood, note);
    }

    /**
     * Sets the state of the service.
     */
    @Override
    public void setState(String state) {
        // ignore
    }

}

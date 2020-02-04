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

package org.opencadc.minoc;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.vosi.AvailabilityPlugin;
import ca.nrc.cadc.vosi.AvailabilityStatus;
import ca.nrc.cadc.vosi.avail.CheckException;
import ca.nrc.cadc.vosi.avail.CheckResource;
import ca.nrc.cadc.vosi.avail.CheckWebService;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.opencadc.inventory.db.version.InitDatabase;

/**
 * This class performs the work of determining if the executing artifact
 * service is operating as expected.
 * 
 * @author majorb
 */
public class ServiceAvailability implements AvailabilityPlugin {

    private static final Logger log = Logger.getLogger(ServiceAvailability.class);

    /**
     * Default, no-arg constructor.
     */
    public ServiceAvailability() {
    }

    /**
     * Sets the name of the application.
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
    public AvailabilityStatus getStatus() {
        boolean isGood = true;
        String note = "service is accepting requests";
        
        try {
            
            log.info("init database...");
            DataSource ds = DBUtil.findJNDIDataSource(ArtifactAction.JNDI_DATASOURCE);
            MultiValuedProperties props = ArtifactAction.readConfig();
            Map<String, Object> config = ArtifactAction.getDaoConfig(props);
            InitDatabase init = new InitDatabase(ds, (String) config.get("database"), (String) config.get("schema"));
            init.doInit();
            log.info("init database... OK");

            // check other services we depend on
            RegistryClient reg = new RegistryClient();
            String url;
            CheckResource checkResource;
            
            LocalAuthority localAuthority = new LocalAuthority();

            URI credURI = localAuthority.getServiceURI(Standards.CRED_PROXY_10.toString());
            url = reg.getServiceURL(credURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON).toExternalForm();
            checkResource = new CheckWebService(url);
            checkResource.check();

            URI usersURI = localAuthority.getServiceURI(Standards.UMS_USERS_01.toString());
            url = reg.getServiceURL(usersURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON).toExternalForm();
            checkResource = new CheckWebService(url);
            checkResource.check();
            
            URI groupsURI = localAuthority.getServiceURI(Standards.GMS_SEARCH_01.toString());
            url = reg.getServiceURL(groupsURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON).toExternalForm();
            checkResource = new CheckWebService(url);
            checkResource.check();
            
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

        return new AvailabilityStatus(isGood, null, null, null, note);
    }

    /**
     * Sets the state of the service.
     */
    @Override
    public void setState(String state) {
        // ignore
    }

}

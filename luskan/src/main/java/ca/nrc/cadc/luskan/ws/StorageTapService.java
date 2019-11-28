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

package ca.nrc.cadc.luskan.ws;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.AuthenticatorImpl;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.tap.impl.WebTmpUtil;
import ca.nrc.cadc.tap.schema.InitDatabaseTS;
import ca.nrc.cadc.uws.server.impl.InitDatabaseUWS;
import ca.nrc.cadc.vosi.AvailabilityPlugin;
import ca.nrc.cadc.vosi.AvailabilityStatus;
import ca.nrc.cadc.vosi.avail.CheckCertificate;
import ca.nrc.cadc.vosi.avail.CheckDataSource;
import ca.nrc.cadc.vosi.avail.CheckException;
import ca.nrc.cadc.vosi.avail.CheckResource;
import ca.nrc.cadc.vosi.avail.CheckWebService;
import java.io.File;
import java.net.URI;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 *
 * @author adriand
 */
public class StorageTapService implements AvailabilityPlugin {

    private static final Logger log = Logger.getLogger(StorageTapService.class);
    
    private static final Principal TRUSTED = new X500Principal("cn=servops_4a2,ou=cadc,o=hia,c=ca");

    private static final String TAP_TEST = "select schema_name from tap_schema.schemas11 where schema_name='tap_schema'";
    private static final String UWS_TEST = "select jobID from uws.Job limit 1";

    private String appName;

    public StorageTapService() {
    }

    @Override
    public void setAppName(String appName) {
        this.appName = appName;
    }
    
    @Override
    public AvailabilityStatus getStatus() {
        boolean isGood = true;
        String note = "service is accepting queries";
        try {
            String state = getState();
            if (RestAction.STATE_OFFLINE.equals(state)) {
                return new AvailabilityStatus(false, null, null, null, RestAction.STATE_OFFLINE_MSG);
            }
            if (RestAction.STATE_READ_ONLY.equals(state)) {
                return new AvailabilityStatus(false, null, null, null, RestAction.STATE_READ_ONLY_MSG);
            }

            String tapDsName = DataSourceProviderImpl.getDataSourceName(null, "tapadm");
            DataSource tapadm = DBUtil.findJNDIDataSource(tapDsName);
            InitDatabaseTS tsi = new InitDatabaseTS(tapadm, null, "tap_schema");
            tsi.doInit();

            String uwsDsName = DataSourceProviderImpl.getDataSourceName(null, "uws");
            DataSource uws = DBUtil.findJNDIDataSource(uwsDsName);
            InitDatabaseUWS uwsi = new InitDatabaseUWS(uws, null, "uws");
            uwsi.doInit();

            // run a couple of queries
            CheckResource cr = new CheckDataSource(tapDsName, TAP_TEST);
            cr.check();

            cr = new CheckDataSource(uwsDsName, UWS_TEST);
            cr.check();

            LocalAuthority localAuthority = new LocalAuthority();
            RegistryClient reg = new RegistryClient();

            URI credURI = localAuthority.getServiceURI(Standards.CRED_PROXY_10.toString());
            String url = reg.getServiceURL(credURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON).toExternalForm();
            CheckResource checkResource = new CheckWebService(url);
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
            log.error("test failed", t);
            isGood = false;
            note = "test failed, reason: " + t;
        }
        return new AvailabilityStatus(isGood, null, null, null, note);
    }

    @Override
    public void setState(String state) {
        AccessControlContext acContext = AccessController.getContext();
        Subject subject = Subject.getSubject(acContext);

        if (subject == null) {
            return;
        }

        Principal caller = AuthenticationUtil.getX500Principal(subject);
        if (AuthenticationUtil.equals(TRUSTED, caller)) {
            String key = appName + RestAction.STATE_MODE_KEY;
            if (RestAction.STATE_OFFLINE.equalsIgnoreCase(state)) {
                System.setProperty(key, RestAction.STATE_OFFLINE);
            //} else if (RestAction.STATE_READ_ONLY.equalsIgnoreCase(state)) {
            //    System.setProperty(key, RestAction.STATE_READ_ONLY);
            } else if (RestAction.STATE_READ_WRITE.equalsIgnoreCase(state)) {
                System.setProperty(key, RestAction.STATE_READ_WRITE);
            } else {
                throw new IllegalArgumentException("invalid state: " + state 
                    + " expected: " + RestAction.STATE_READ_WRITE + "|" + RestAction.STATE_OFFLINE);
            }
            log.info("WebService state changed: " + key + "=" + state + " by " + caller + " [OK]");
        } else {
            log.warn("WebService state change by " + caller + " [DENIED]");
        }
    }
    
    @Override
    public boolean heartbeat() throws RuntimeException {
        return true;
    }

    private String getState() {
        String key = appName + RestAction.STATE_MODE_KEY;
        String ret = System.getProperty(key);
        if (ret == null) {
            return RestAction.STATE_READ_WRITE;
        }
        return ret;
    }

}

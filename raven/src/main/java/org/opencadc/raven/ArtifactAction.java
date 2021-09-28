/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2021.                            (c) 2021.
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
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vosi.Availability;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.log4j.Logger;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.server.PermissionsCheck;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.WriteGrant;


/**
 * Abstract class for all that raven action classes have in common,
 * including request parsing, authentication, and authentication.
 *
 * @author adriand
 */
public abstract class ArtifactAction extends RestAction {
    private static final Logger log = Logger.getLogger(ArtifactAction.class);

    // The target artifact
    URI artifactURI;
    Transfer transfer;

    String user;

    // immutable state set in constructor
    protected final ArtifactDAO artifactDAO;
    protected final File publicKeyFile;
    protected final File privateKeyFile;
    protected final List<URI> readGrantServices = new ArrayList<>();
    protected final List<URI> writeGrantServices = new ArrayList<>();

    protected final boolean authenticateOnly;
    protected Map<URI, Availability> siteAvailabilities;
    protected Map<URI, StorageSiteRule> siteRules;

    // constructor for unit tests with no config/init
    ArtifactAction(boolean init) {
        super();
        this.authenticateOnly = false;
        this.publicKeyFile = null;
        this.privateKeyFile = null;
        this.artifactDAO = null;
    }

    protected ArtifactAction() {
        super();
        MultiValuedProperties props = RavenInitAction.getConfig();

        List<String> readGrants = props.getProperty(RavenInitAction.READ_GRANTS_KEY);
        if (readGrants != null) {
            for (String s : readGrants) {
                try {
                    URI u = new URI(s);
                    readGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + RavenInitAction.READ_GRANTS_KEY + "=" + s + " must be a valid URI");
                }
            }
        }

        List<String> writeGrants = props.getProperty(RavenInitAction.WRITE_GRANTS_KEY);
        if (writeGrants != null) {
            for (String s : writeGrants) {
                try {
                    URI u = new URI(s);
                    writeGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + RavenInitAction.WRITE_GRANTS_KEY + "=" + s + " must be a valid URI");
                }
            }
        }
        
        String ao = props.getFirstPropertyValue(RavenInitAction.DEV_AUTH_ONLY_KEY);
        if (ao != null) {
            try {
                this.authenticateOnly = Boolean.valueOf(ao);
                log.warn("(configuration) authenticateOnly = " + authenticateOnly);
            } catch (Exception ex) {
                throw new IllegalStateException("invalid config: " + RavenInitAction.DEV_AUTH_ONLY_KEY + "=" + ao + " must be true|false or not set");
            }
        } else {
            authenticateOnly = false;
        }

        // technically, raven only needs the private key to generate pre-auth tokens
        // but both are requied here for clarity
        // - in principle, raven could export it's public key and minoc(s) could retrieve it
        // - for now, minoc(s) need to be configured with the public key to validate pre-auth

        String pubkeyFileName = props.getFirstPropertyValue(RavenInitAction.PUBKEYFILE_KEY);
        String privkeyFileName = props.getFirstPropertyValue(RavenInitAction.PRIVKEYFILE_KEY);
        this.publicKeyFile = new File(System.getProperty("user.home") + "/config/" + pubkeyFileName);
        this.privateKeyFile = new File(System.getProperty("user.home") + "/config/" + privkeyFileName);
        if (!publicKeyFile.exists() || !privateKeyFile.exists()) {
            throw new IllegalStateException("invalid config: missing public/private key pair files -- " + publicKeyFile + " | " + privateKeyFile);
        }

        Map<String, Object> config = RavenInitAction.getDaoConfig(props);
        this.artifactDAO = new ArtifactDAO();
        artifactDAO.setConfig(config); // connectivity tested

        // set the user for logging
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (authMethod != null && !authMethod.equals(AuthMethod.ANON)) {
            Set<String> userids = AuthenticationUtil.getUseridsFromSubject();
            if (userids.size() > 0) {
                user = userids.iterator().next();
            }
        }

        // get the storage site rules
        this.siteRules = RavenInitAction.getStorageSiteRules(props);
    }

    protected void initAndAuthorize() throws Exception {
        init();

        Class grantClass = ReadGrant.class;
        if ((transfer != null) && (transfer.getDirection().equals(Direction.pushToVoSpace))) {
            grantClass = WriteGrant.class;
        }
        PermissionsCheck permissionsCheck = new PermissionsCheck(this.artifactURI, this.authenticateOnly,
                                                                 this.logInfo);
        if (ReadGrant.class.isAssignableFrom(grantClass)) {
            permissionsCheck.checkReadPermission(this.readGrantServices);
        } else if (WriteGrant.class.isAssignableFrom(grantClass)) {
            permissionsCheck.checkWritePermission(this.writeGrantServices);
        } else {
            throw new IllegalStateException("Unsupported grant class: " + grantClass);
        }
    }

    /**
     * Method to set the artifactURI of the request as well as other attributes. Information might be in URL path
     * or sent in the request message (negotiation)
     */
    abstract void parseRequest() throws Exception;
    
    void init() throws Exception {
        parseRequest();
        if (artifactURI == null) {
            throw new IllegalArgumentException("Missing artifact URI from path or request content");
        }

        String siteAvailabilitiesKey = this.appName + RavenInitAction.JNDI_AVAILABILITY_NAME;
        log.debug("siteAvailabilitiesKey: " + siteAvailabilitiesKey);
        try {
            Context initContext = new InitialContext();
            this.siteAvailabilities = (Map<URI, Availability>) initContext.lookup(siteAvailabilitiesKey);
        } catch (NamingException e) {
            log.error("JNDI lookup error: " + e.getMessage());
            throw new IllegalStateException("JNDI lookup error", e);
        }
    }

}

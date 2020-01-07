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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.baldur;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.PropertiesReader;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.AccessControlException;
import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;

public abstract class PermissionsAction extends RestAction {
    private static final Logger log = Logger.getLogger(PermissionsAction.class);

    private static final String DEFAULT_CONFIG_DIR = System.getProperty("user.home") + "/config/";
    private static final String USER_DNS_PROPERTY = "users";

    protected static final String PERMISSIONS_AUTH_PROPERTIES = "permissionsAuthProperties";
    protected static final String PERMISSIONS_CONFIG_PROPERTIES = "permissionsConfigProperties";
    protected static final String PERMISSIONS_CONTENT_TYPE = "application/x-permissions+xml";

    public PermissionsAction() { }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

    // TODO: add util methods in core to do user auth and group membership using a properties file.
    // Most below copied from core:cadc-log logControlServlet
    /**
     * Check for proper group membership.
     *
     * @param subject The calling user Subject.
     * @return true if the calling user is authorized to make the request, false otherwise.
     */
    protected boolean isAuthorized(Subject subject)
        throws AccessControlException {
        // Get the permissions properties if they exist.
        String authPropertiesFilename = initParams.get(PERMISSIONS_AUTH_PROPERTIES);
        if (authPropertiesFilename == null) {
            throw new IllegalStateException(PERMISSIONS_AUTH_PROPERTIES + " not configured in web.xml");
        }
        PropertiesReader propertiesReader = getPropertiesReader(authPropertiesFilename);

        // check if request user matches an authorized users
        Set<Principal> authorizedUsers = getAuthorizedUserPrincipals(propertiesReader);
        if (isAuthorizedUser(subject, authorizedUsers)) {
            log.info(subject.getPrincipals(X500Principal.class) + " is an authorized user");
            return true;
        }
        return false;
    }

    /**
     * Get a File object for the given filename. The file is assumed to be in a directory name
     * config with the users home directory.
     *
     * @param filename Name of the file.
     * @return A File.
     */
    protected File getConfigFile(String filename) {
        if (filename == null) {
            throw new IllegalArgumentException("filename cannot be null.");
        }

        String configDir = DEFAULT_CONFIG_DIR;
        log.debug("Reading from config dir: " + configDir);

        if (!configDir.endsWith("/")) {
            configDir = configDir + "/";
        }

        String filepath = configDir + filename;
        File file = new File(filepath);

        if (!file.canRead()) {
            log.warn("File at " + filepath + " does not exist or cannot read.");
        }
        return file;
    }

    /**
     * Read the properties file and returns a PropertiesReader.
     *
     * @return A PropertiesReader, or null if the properties file does not
     *          exist or can not be read.
     */
    protected PropertiesReader getPropertiesReader(String propertiesFilename) {
        PropertiesReader reader = null;
        if (propertiesFilename != null) {
            reader = new PropertiesReader(propertiesFilename);
            if (!reader.canRead()) {
                reader = null;
            }
        }
        return reader;
    }

    /**
     * Get a Set of X500Principal's from the permissions properties. Return
     * an empty set if the properties do not exist or can't be read.
     *
     * @return Set of authorized X500Principals, can be an empty Set if none configured.
     */
    Set<Principal> getAuthorizedUserPrincipals(PropertiesReader propertiesReader) {
        Set<Principal> principals = new HashSet<Principal>();
        if (propertiesReader != null) {
            try {
                List<String> properties = propertiesReader.getPropertyValues(USER_DNS_PROPERTY);
                if (properties != null) {
                    for (String property : properties) {
                        if (!property.isEmpty()) {
                            principals.add(new X500Principal(property));
                        }
                    }
                }
            } catch (IllegalArgumentException e) {
                log.debug("No authorized users configured");
            }
        }
        return principals;
    }

    /**
     * Checks if the caller Principal matches an authorized Principal
     * from the properties file.

     * @return true if the calling user is an authorized user, false otherwise.
     */
    protected boolean isAuthorizedUser(Subject subject, Set<Principal> authorizedUsers) {
        if (!authorizedUsers.isEmpty()) {
            Set<X500Principal> principals = subject.getPrincipals(X500Principal.class);
            for (Principal caller : principals) {
                for (Principal authorizedUser : authorizedUsers) {
                    if (AuthenticationUtil.equals(authorizedUser, caller)) {
                        return true;
                    }
                }
            }
        } else {
            log.debug("Authorized users not configured.");
        }
        return false;
    }

    /**
     * Get the permissions for the given Artifact URI.
     *
     * @param artifactURI The Artifact URI
     * @return Permissions object
     * @throws IOException for errors reading the permissions configuration file.
     */
    protected Permissions getPermissions(URI artifactURI) throws IOException {
        String configPropertiesFilename = initParams.get(PERMISSIONS_CONFIG_PROPERTIES);
        if (configPropertiesFilename == null) {
            throw new IllegalStateException(PERMISSIONS_CONFIG_PROPERTIES + " not configured in web.xml");
        }
        File configFile = getConfigFile(configPropertiesFilename);
        PermissionsConfig config = new PermissionsConfig(configFile);
        return  config.getPermissions(artifactURI);
    }

}

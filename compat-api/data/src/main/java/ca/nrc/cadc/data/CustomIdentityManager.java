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

package ca.nrc.cadc.data;

import ca.nrc.cadc.ac.ACIdentityManager;
import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.AuthorizationToken;
import ca.nrc.cadc.auth.AuthorizationTokenPrincipal;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.reg.client.RegistryClient;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class CustomIdentityManager extends ACIdentityManager {
    private static final Logger log = Logger.getLogger(CustomIdentityManager.class);

    public CustomIdentityManager() { 
    }

    @Override
    public Subject validate(Subject subject) throws AccessControlException {
        Subject ret = super.validate(subject);
        
        Set<AuthorizationTokenPrincipal> raw = ret.getPrincipals(AuthorizationTokenPrincipal.class);
        log.debug("raw: " + raw.size());
        
        if (!raw.isEmpty()) {
            for (AuthorizationTokenPrincipal p : raw) {
                log.debug("raw header: " + p.getHeaderKey() + " " + p.getHeaderValue());

                String[] ss = p.getHeaderValue().split(" ");
                if (ss.length != 2) {
                    throw new NotAuthenticatedException(p.getHeaderKey(), NotAuthenticatedException.AuthError.INVALID_REQUEST,
                        "incomplete authorization header");
                }
                String username = null;
                String password = null;
                if (AuthenticationUtil.CHALLENGE_TYPE_BASIC.equalsIgnoreCase(ss[0])) {
                    Base64.Decoder dec = Base64.getDecoder();
                    byte[] b = dec.decode(ss[1]);
                    String creds = new String(b); // default charset
                    String[] up = creds.split(":");
                    username = up[0];
                    password = up[1];
                }
                if (username != null && password != null) {
                    LocalAuthority loc = new LocalAuthority();
                    URI resourceID = loc.getServiceURI(Standards.SECURITY_METHOD_PASSWORD.toASCIIString());
                    if (resourceID != null) {
                        RegistryClient reg = new RegistryClient();
                        URL loginURL = reg.getServiceURL(resourceID, Standards.SECURITY_METHOD_PASSWORD, AuthMethod.ANON);
                        Map<String,Object> params = new TreeMap<>();
                        params.put("username", username);
                        params.put("password", password);
                        HttpPost login = new HttpPost(loginURL, params, true);
                        try {
                            log.warn("attempting login...");
                            login.prepare();

                            String token = login.getResponseHeader("x-vo-bearer");

                            //String domainHdr = login.getResponseHeader("x-vo-bearer-domains");
                            //List<String> domains = Arrays.asList(domainHdr.split(" ,"));
                            List<String> domains = Arrays.asList(new String[] {"cadc-ccda.hia-iha.nrc-cnrc.gc.ca"});

                            AuthorizationToken at = new AuthorizationToken("bearer", token, domains);
                            ret.getPrincipals().remove(p);
                            ret.getPrincipals().add(new HttpPrincipal(username));
                            ret.getPublicCredentials().add(at);
                            
                        } catch (ByteLimitExceededException | ResourceAlreadyExistsException ignore) {
                            log.debug("ignore exception: " + ignore);
                        } catch (IOException | InterruptedException | ResourceNotFoundException ex) {
                            throw new RuntimeException("CONFIG: login failed", ex);
                        }
                    }
                }
            }
        }
        return ret;
    }
}

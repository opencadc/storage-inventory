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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.rest.Version;
import ca.nrc.cadc.util.InvalidConfigException;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
abstract class BaseAction extends RestAction {
    private static final Logger log = Logger.getLogger(BaseAction.class);
    
    private static final URI RAVEN = URI.create("ivo://cadc.nrc.ca/global/raven");

    protected final URL ravenFilesURL;
    protected final URL ravenLocateURL;
    private String defaultScheme;
    private final Map<String,List<String>> schemeMap = new TreeMap<>();
    
    private boolean authRequired = false;
    private String fileSpec;
    
    public BaseAction() throws InvalidConfigException {
        RegistryClient reg = new RegistryClient();
        this.ravenFilesURL = reg.getServiceURL(RAVEN, Standards.SI_FILES, AuthMethod.ANON);
        this.ravenLocateURL = reg.getServiceURL(RAVEN, Standards.SI_LOCATE, AuthMethod.ANON);
    }

    @Override
    protected String getServerImpl() {
        // no null version checking because fail to build correctly can't get past basic testing
        Version v = getVersionFromResource();
        String ret = "data-" + v.getMajorMinor();
        return ret;
    }
    
    @Override
    public void initAction() throws Exception {
        super.initAction();
        
        // path to Artifact.uri mapping
        try {
            URL resURL = super.getResource("uri-scheme-map");
            Properties props = new Properties();
            props.load(resURL.openStream());
            
            this.defaultScheme = props.getProperty("default");
            for (String key : props.stringPropertyNames()) {
                if (!key.equals("default")) {
                    String val = props.getProperty(key);
                    String[] tokens = val.split(" ");
                    List<String> tlist = Arrays.asList(tokens);
                    schemeMap.put(key, tlist);
                }
            }
        } catch (IOException ex) {
            throw new InvalidConfigException("CONFIG: failed to read uri-scheme-map", ex);
        }
        
        String aq = super.initParams.get("authRequired");
        this.authRequired = "true".equals(aq);
    }
    
    protected final void fireNotFound() throws ResourceNotFoundException {
        throw new ResourceNotFoundException(fileSpec);
    }
    
    protected final void fireAuthRedirect() {
        StringBuilder sb = new StringBuilder();
        sb.append(syncInput.getRequestURI().replace("/pub/", "/auth/"));
        String sep = "?";
        for (String p : syncInput.getParameterNames()) {
            for (String v : syncInput.getParameters(p)) {
                sb.append(sep).append(p).append("=").append(NetUtil.encode(v));
                sep = "&";
            }
        }
        String loc = sb.toString();
        syncOutput.setCode(303);
        syncOutput.setHeader("location", loc);
    }

    private void initAuth() {
        if (!authRequired) {
            return;
        }
        
        Subject curSubject = AuthenticationUtil.getCurrentSubject();
        AuthMethod am = AuthenticationUtil.getAuthMethod(curSubject);
        if (am != null && !AuthMethod.ANON.equals(am)) {
            return; // authenticated
        }
        
        syncOutput.addHeader(AuthenticationUtil.AUTHENTICATE_HEADER, "Basic realm=\"CADC\", charset=\"UTF-8\"");
        throw new NotAuthenticatedException("authentication required");
    }
    
    protected final List<URI> getURIs() {
        initAuth();
        
        String path = syncInput.getPath();
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("missing path: {archive}/{filename}");
        }
        this.fileSpec = path;
        final List<URI> ret = new ArrayList<>();
        String[] ss = path.split("/");
        String archive = ss[0];
        if (schemeMap.containsKey(archive)) {
            for (String sc : schemeMap.get(archive)) {
                URI uri = URI.create(sc + ":" + path);
                ret.add(uri);
            }
        } else {
            URI uri = URI.create(defaultScheme + ":" + path);
            ret.add(uri);
            
        }
        return ret;
    }
}

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
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class DataTest {
    private static final Logger log = Logger.getLogger(DataTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.data", Level.INFO);
    }
    
    static final URI RESOURCE_ID = URI.create("ivo://cadc.nrc.ca/data");
    
    static final URI PUBLIC_DATA = URI.create("cadc:IRIS/I125B4H0.fits");
    static final URI PUBLIC_DATA_MAST = URI.create("mast:HST/product/iexu15040_jit.fits");
    
    static final URI PUBLIC_DATA_CADC_GEM = URI.create("cadc:GEMINI/01APR19_067_th.jpg");
    static final URI PUBLIC_DATA_GEM_GEM = URI.create("gemini:GEMINI/S20100110S0334.jpg");
    
    static final URI[] PUBLIC_DATA_ALL = new URI[] {
        PUBLIC_DATA, PUBLIC_DATA_MAST, PUBLIC_DATA_CADC_GEM, PUBLIC_DATA_GEM_GEM
    };
    
    static final URI PROP_DATA = URI.create("cadc:CFHT/61545o.fits");
    
    final File certKeyFile;
    final URL pubURL;
    final URL usmURL;
    
    public DataTest() throws Exception { 
        RegistryClient reg = new RegistryClient();
        this.pubURL = reg.getServiceURL(RESOURCE_ID, Standards.DATA_10, AuthMethod.ANON);
        URL cap = reg.getServiceURL(RESOURCE_ID, Standards.VOSI_CAPABILITIES, AuthMethod.ANON);
        this.usmURL = new URL(cap.toExternalForm().replace("/capabilities", "/uri-scheme-map"));
        
        String filename = System.getProperty("user.name") + ".pem";
        this.certKeyFile = FileUtil.getFileFromResource(filename, DataTest.class);
    }
    
    @Test
    public void testSchemaMapConfig() throws Exception {
        HttpGet get = new HttpGet(usmURL, true);
        get.prepare();
        log.info("code: " + get.getResponseCode());
        Assert.assertEquals(200, get.getResponseCode());
        
        Properties props = new Properties();
        props.load(get.getInputStream());
        Assert.assertFalse(props.isEmpty());
        boolean foundDefault = false;
        for (Object key : props.keySet()) {
            Object val = props.get(key);
            log.info("found: " + key + " = " + val);
            if (key.equals("default")) {
                foundDefault = true;
            }
        }
        Assert.assertTrue("found default", foundDefault);
    }
    
    @Test
    public void testPublicHEAD() throws Exception {
        URL u = new URL(pubURL.toExternalForm() + "/" + PUBLIC_DATA.getSchemeSpecificPart());
        HttpGet head = new HttpGet(u, false);  // must be final URL
        head.setHeadOnly(true);
        head.prepare();
        log.info("code: " + head.getResponseCode());
        Assert.assertEquals(200, head.getResponseCode());
        Assert.assertNotNull(head.getContentLength());
        Assert.assertNotNull(head.getDigest());
    }
    
    @Test
    public void testPublicHEAD_ALL() throws Exception {
        for (URI pub : PUBLIC_DATA_ALL) {
            log.info("trying: " + pub);
            URL u = new URL(pubURL.toExternalForm() + "/" + pub.getSchemeSpecificPart());
            HttpGet head = new HttpGet(u, false);  // must be final URL
            head.setHeadOnly(true);
            head.prepare();
            log.info("code: " + head.getResponseCode());
            Assert.assertEquals(200, head.getResponseCode());
            Assert.assertNotNull(head.getContentLength());
            Assert.assertNotNull(head.getDigest());
        }
    }
    
    @Test
    public void testPublicGET() throws Exception {
        URL u = new URL(pubURL.toExternalForm() + "/" + PUBLIC_DATA.getSchemeSpecificPart());
        HttpGet get = new HttpGet(u, false);
        get.prepare();
        log.info("code: " + get.getResponseCode());
        URL redirect = get.getRedirectURL();
        Assert.assertNotNull(redirect);
        
        HttpGet download = new HttpGet(redirect, false); // must be final URL
        download.setFollowRedirects(false); 
        download.prepare();
        log.info("code: " + download.getResponseCode());
        Assert.assertEquals(200, download.getResponseCode());
        Assert.assertNotNull(download.getContentLength());
        Assert.assertNotNull(download.getDigest());
        
        InputStream istream = download.getInputStream();
        Assert.assertNotNull(istream);
        istream.close();
    }
    
    @Test
    public void testProprietaryHEAD() throws Exception {
        URL u = new URL(pubURL.toExternalForm() + "/" + PROP_DATA.getSchemeSpecificPart());
        HttpGet head = new HttpGet(u, false); // redirect to challenge
        head.setHeadOnly(true);
        try {
            // make sure the target data is really proprietary
            log.info("try: " + u);
            head.prepare();
            log.info("anon head: " + head.getResponseCode() + " " + head.getRedirectURL());
            Assert.assertEquals(303, head.getResponseCode());
            URL authURL = head.getRedirectURL();
            Assert.assertNotNull("auth redirect", authURL);
            
            log.info("try: " + authURL);
            HttpGet checkAuth = new HttpGet(authURL, false); // must be this URL
            checkAuth.setHeadOnly(true);
            checkAuth.prepare();
            Assert.fail("expected NotAuthenticatedException, got: " + head.getResponseCode());
        } catch (NotAuthenticatedException ex) {
            log.info("caught expected: " + ex);
            
            // try with netrc so we can respond to challenge
            log.info("try: " + head.getRedirectURL() + " with netrc");
            Subject subject = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));
            final HttpGet auth = new HttpGet(head.getRedirectURL(), false); // must be this URL
            auth.setHeadOnly(true);
            try {
                Subject.doAs(subject, (PrivilegedExceptionAction<Object>) () -> {
                    auth.prepare();
                    return null;
                });
            } catch (PrivilegedActionException pex) {
                throw pex.getException();
            }
            log.info("auth head: " + auth.getResponseCode() + " " + auth.getRedirectURL());
            Assert.assertEquals(200, auth.getResponseCode());
            Assert.assertNotNull(auth.getContentLength());
            Assert.assertNotNull(auth.getDigest());
        }
    }
    
    @Test
    public void testProprietaryGET() throws Exception {
        String paramsInAuthRedirect = "FOO=1&FOO=2";
        URL u = new URL(pubURL.toExternalForm() + "/" + PROP_DATA.getSchemeSpecificPart()
            + "?" + paramsInAuthRedirect);
        HttpGet get = new HttpGet(u, false);
        get.prepare();
        log.info("challenge: " + get.getResponseCode() + " " + get.getRedirectURL());
        URL authURL = get.getRedirectURL();
        Assert.assertNotNull(authURL);
        Assert.assertTrue(authURL.toExternalForm().contains(paramsInAuthRedirect));
        
        // try again with username+password
        Subject subject = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));
        final HttpGet auth = new HttpGet(authURL, false);
        try {
            Subject.doAs(subject, (PrivilegedExceptionAction<Object>) () -> {
                auth.prepare();
                return null;
            });
        } catch (PrivilegedActionException pex) {
            throw pex.getException();
        }
        log.info("auth get: " + auth.getResponseCode() + " " + auth.getRedirectURL());
        URL redirect = auth.getRedirectURL();
        Assert.assertNotNull(redirect);
        
        log.info("check download URL: " + redirect);
        HttpGet download = new HttpGet(redirect, false); // must be final URL
        download.setFollowRedirects(false); 
        download.setHeadOnly(true); // for this test
        download.prepare();
        log.info("code: " + download.getResponseCode());
        Assert.assertEquals(200, download.getResponseCode());
        Assert.assertNotNull(download.getContentLength());
        Assert.assertNotNull(download.getDigest());
    }
    
    @Test
    public void testProprietaryCert() throws Exception {
        Subject subject = SSLUtil.createSubject(certKeyFile);
        URL u = new URL(pubURL.toExternalForm() + "/" + PROP_DATA.getSchemeSpecificPart());
        final HttpGet auth = new HttpGet(u, false);
        try {
            Subject.doAs(subject, (PrivilegedExceptionAction<Object>) () -> {
                auth.prepare();
                return null;
            });
        } catch (PrivilegedActionException pex) {
            throw pex.getException();
        }
        log.info("auth get: " + auth.getResponseCode() + " " + auth.getRedirectURL());
        URL redirect = auth.getRedirectURL();
        Assert.assertNotNull(redirect);
        
        log.info("check download URL: " + redirect);
        HttpGet download = new HttpGet(redirect, false); // must be final URL
        download.setFollowRedirects(false); 
        download.setHeadOnly(true); // for this test
        download.prepare();
        log.info("code: " + download.getResponseCode());
        Assert.assertEquals(200, download.getResponseCode());
        Assert.assertNotNull(download.getContentLength());
        Assert.assertNotNull(download.getDigest());
    }
}

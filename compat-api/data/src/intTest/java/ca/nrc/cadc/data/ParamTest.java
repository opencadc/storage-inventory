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
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;

import java.net.URL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test that supported data params are transformed to SI (raven/minoc) params and others are dropped.
 * 
 * @author pdowler
 */
public class ParamTest {
    private static final Logger log = Logger.getLogger(ParamTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.data", Level.INFO);
    }
    
    final URL pubURL;
    
    public ParamTest() {
        RegistryClient reg = new RegistryClient();
        this.pubURL = reg.getServiceURL(DataTest.RESOURCE_ID, Standards.DATA_10, AuthMethod.ANON);
    }
    
    @Test
    public void testTransformToMETA() throws Exception {
        // check case-insensitive param name handling, including SI API pass through
        for (String param : new String[] { "fhead", "FHEAD", "fHeAd", "MeTa" }) {
            URL u = new URL(pubURL.toExternalForm() + "/" + DataTest.PUBLIC_DATA.getSchemeSpecificPart() + "?" + param + "=true");
            log.info("try: " + u);
            HttpGet get = new HttpGet(u, false);
            get.prepare();
            log.info("code: " + get.getResponseCode() + " " + get.getRedirectURL());
            URL redirect = get.getRedirectURL();
            Assert.assertNotNull(redirect);
            Assert.assertNotNull(redirect.getQuery());
            Assert.assertTrue(redirect.getQuery().toLowerCase().contains("meta=true"));
        }
    }
    
    @Test
    public void testTransformToSUB() throws Exception {
        // check case-insensitive param name handling, including SI API passthrough
        String pval = NetUtil.encode("[1][10:20]");
        for (String param : new String[] { "cutout", "CUTOUT", "cUtOuT", "SuB" }) {
            URL u = new URL(pubURL.toExternalForm() + "/" + DataTest.PUBLIC_DATA.getSchemeSpecificPart() + "?" + param + "=" + pval);
            log.info("try: " + u);
            HttpGet get = new HttpGet(u, false);
            get.prepare();
            log.info("code: " + get.getResponseCode() + " " + get.getRedirectURL());
            URL redirect = get.getRedirectURL();
            Assert.assertNotNull(redirect);
            String query = redirect.getQuery();
            Assert.assertNotNull(query);
            String[] pv = query.split("=");
            
            Assert.assertEquals("sub", pv[0].toLowerCase());
            Assert.assertEquals(pval, pv[1]);
        }
    }
    
    @Test
    public void testTransformMultipleToSUB() throws Exception {
        String pval1 = NetUtil.encode("[1][10:20]");
        String pval2 = NetUtil.encode("[2][30:40]");
        String param = "cutout";
        URL u = new URL(pubURL.toExternalForm() + "/" + DataTest.PUBLIC_DATA.getSchemeSpecificPart() 
                + "?" + param + "=" + pval1 + "&" + param + "=" + pval2);
        log.info("try: " + u);
        HttpGet get = new HttpGet(u, false);
        get.prepare();
        log.info("code: " + get.getResponseCode() + " " + get.getRedirectURL());
        URL redirect = get.getRedirectURL();
        Assert.assertNotNull(redirect);
        String query = redirect.getQuery();
        Assert.assertNotNull(query);

        String[] pvs = query.split("&");
        Assert.assertEquals(2, pvs.length);

        String[] p1 = pvs[0].split("=");
        Assert.assertEquals("sub", p1[0].toLowerCase());
        Assert.assertEquals(pval1, p1[1]);

        String[] p2 = pvs[1].split("=");
        Assert.assertEquals("sub", p2[0].toLowerCase());
        Assert.assertEquals(pval2, p2[1]);
    }
    
    @Test
    public void testIgnoreUnknownParam() throws Exception {
        String param = "unknownParam";
        URL u = new URL(pubURL.toExternalForm() + "/" + DataTest.PUBLIC_DATA.getSchemeSpecificPart() + "?" + param + "=123");
        log.info("try: " + u);
        HttpGet get = new HttpGet(u, false);
        get.prepare();
        log.info("code: " + get.getResponseCode() + " " + get.getRedirectURL());
        URL redirect = get.getRedirectURL();
        Assert.assertNotNull(redirect);
        Assert.assertNull(redirect.getQuery());
    }
}

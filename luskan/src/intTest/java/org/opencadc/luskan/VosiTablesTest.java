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
 *  $Revision: 5 $
 *
 ************************************************************************
 */

package org.opencadc.luskan;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.net.ContentType;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vosi.TableReader;
import ca.nrc.cadc.vosi.TableSetReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author original pdowler - adapted by adriand
 */
public class VosiTablesTest
{
    private static final Logger log = Logger.getLogger(VosiTablesTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.luskan", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.reg.client", Level.DEBUG);
    }
    
    URL tablesURL;
    
    public VosiTablesTest()
    {
        RegistryClient rc = new RegistryClient();
        this.tablesURL = rc.getServiceURL(Constants.RESOURCE_ID, Standards.VOSI_TABLES_11, AuthMethod.ANON);
    }

    @Test
    public void testValidateTablesetDoc()
    {
        try
        {
            TableSetReader tsr = new TableSetReader(true);
            log.info("testValidateTablesetDoc: " + tablesURL.toExternalForm()); 
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(tablesURL, bos);
            get.run();
            Assert.assertEquals(200, get.getResponseCode());
            ContentType ct = new ContentType(get.getContentType());
            Assert.assertEquals("text/xml", ct.getBaseType());
            
            TapSchema ts = tsr.read(new ByteArrayInputStream(bos.toByteArray()));
            Assert.assertNotNull(ts);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testValidateTableDoc()
    {
        try
        {
            TableReader tr = new TableReader(true);
            String s = tablesURL.toExternalForm() + "/tap_schema.tables";
            log.info("testValidateTableDoc: " + s);
            
            URL url = new URL(s);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, bos);
            get.run();
            Assert.assertEquals(200, get.getResponseCode());
            ContentType ct = new ContentType(get.getContentType());
            Assert.assertEquals("text/xml", ct.getBaseType());
            
            TableDesc td = tr.read(new ByteArrayInputStream(bos.toByteArray()));
            Assert.assertNotNull(td);
            Assert.assertEquals("tap_schema.tables", td.getTableName());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testTableNotFound()
    {
        try
        {
            String s = tablesURL.toExternalForm() + "/tap_schema.no_such_table";
            log.info("testTableNotFound: " + s);

            URL url = new URL(s);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, bos);
            get.run();
            Assert.assertEquals(404, get.getResponseCode());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testDetailMin()
    {
        try
        {
            TableSetReader tsr = new TableSetReader(true);
            String s = tablesURL.toExternalForm() + "?detail=min";
            log.info("testDetailMin: " + s);

            URL url = new URL(s);
            TapSchema ts = tsr.read(url.openStream());
            Assert.assertNotNull(ts);
            Assert.assertFalse(ts.getSchemaDescs().isEmpty());
            SchemaDesc sd = ts.getSchema("tap_schema");
            log.debug("testDetailMin: " + sd.getSchemaName());
            Assert.assertFalse(sd.getTableDescs().isEmpty());
            for (TableDesc td : sd.getTableDescs())
            {
                Assert.assertTrue("no columns:" + td.getTableName(), td.getColumnDescs().isEmpty());
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}

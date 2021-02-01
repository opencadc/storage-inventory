/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2020.                            (c) 2020.
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
 *
 ************************************************************************
 */

package org.opencadc.inventory.storage.ad;

import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.tap.TapRowMapper;


public class AdStorageQueryTest {

    private static final Logger log = Logger.getLogger(AdStorageQueryTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage", Level.DEBUG);
    }

    @Test
    public void testStorageURI() throws Exception {
        // Gemini cadc scheme
        AdStorageQuery query = new AdStorageQuery("Gemini");
        TapRowMapper mapper = query.getRowMapper();
        ArrayList<Object> row = new ArrayList<>(7);
        row.add("Gemini");
        row.add(new URI("cadc:Gemini/foo_th.jpg"));
        row.add(new URI("abc"));
        row.add(new Long(33));
        row.add(new URI("ad:GEM/foo_th.jpg"));
        row.add("");
        row.add("application/fits");
        Date now = new Date(System.currentTimeMillis());
        row.add(now);
        StorageMetadata metadata = (StorageMetadata) mapper.mapRow(row);
        Assert.assertTrue("cadc:Gemini/foo_th.jpg".equals(metadata.artifactURI.toString()));
        Assert.assertTrue("ad:GEM/foo_th.jpg".equals(metadata.getStorageLocation().getStorageID().toString()));
        Assert.assertTrue(metadata.getStorageLocation().storageBucket.equals("Gemini"));
        Assert.assertTrue(metadata.getContentChecksum().toString().equals("md5:abc"));
        Assert.assertEquals(33, metadata.getContentLength().longValue());
        Assert.assertTrue(metadata.contentEncoding.equals(""));
        Assert.assertTrue(metadata.contentType.equals("application/fits"));
        Assert.assertEquals(now, metadata.contentLastModified);
        Assert.assertTrue(metadata.artifactURI.toString().equals("cadc:Gemini/foo_th.jpg"));

        // Gemini gemini scheme
        query = new AdStorageQuery("Gemini");
        mapper = query.getRowMapper();
        row = new ArrayList<>(7);
        row.add("Gemini");
        row.add(new URI("gemini:Gemini/foo.fits"));
        row.add(new URI("md5:abc"));
        row.add(new Long(33));
        row.add(new URI("gemini:GEM/foo.fits"));
        row.add("");
        row.add("application/fits");
        row.add(new Date(System.currentTimeMillis()));
        metadata = (StorageMetadata) mapper.mapRow(row);
        Assert.assertTrue("gemini:Gemini/foo.fits".equals(metadata.artifactURI.toString()));
        Assert.assertTrue("gemini:GEM/foo.fits".equals(metadata.getStorageLocation().getStorageID().toString()));

        // MAST
        query = new AdStorageQuery("HST");
        mapper = query.getRowMapper();
        row = new ArrayList<>(7);
        row.add("HST");
        row.add(new URI("mast:HST/foo.fits"));
        row.add(new URI("md5:abc"));
        row.add(new Long(33));
        row.add(new URI("mast:HST/foo.fits"));
        row.add("");
        row.add("application/fits");
        row.add(new Date(System.currentTimeMillis()));
        metadata = (StorageMetadata) mapper.mapRow(row);
        Assert.assertTrue("mast:HST/foo.fits".equals(metadata.artifactURI.toString()));
        Assert.assertTrue("mast:HST/foo.fits".equals(metadata.getStorageLocation().getStorageID().toString()));

        // CFHT
        query = new AdStorageQuery("CFHT");
        mapper = query.getRowMapper();
        row = new ArrayList<>(7);
        row.add("CFHT");
        row.add(new URI("cadc:CFHT/foo.fits"));
        row.add(new URI("md5:abc"));
        row.add(new Long(33));
        row.add(new URI("ad:CFHT/foo.fits"));
        row.add("");
        row.add("application/fits");
        row.add(new Date(System.currentTimeMillis()));
        metadata = (StorageMetadata) mapper.mapRow(row);
        Assert.assertTrue("cadc:CFHT/foo.fits".equals(metadata.artifactURI.toString()));
        Assert.assertTrue("ad:CFHT/foo.fits".equals(metadata.getStorageLocation().getStorageID().toString()));
    }

}

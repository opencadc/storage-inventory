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
    public void testQuery() throws Exception {
        AdStorageQuery asq = new AdStorageQuery("FOO");
        String query = asq.getQuery();
        Assert.assertNotNull(query);
        Assert.assertTrue(query.contains("archiveName = 'FOO'"));
    }
    
    @Test
    public void testQueryDisambiguateBucket() throws Exception {
        AdStorageQuery asq = new AdStorageQuery(AdStorageQuery.DISAMBIGUATE_PREFIX + "DIS");
        String query = asq.getQuery();
        Assert.assertNotNull(query);
        Assert.assertTrue(query.contains("archiveName = 'DIS'"));
    }
    
    @Test
    public void testQueryArchive() throws Exception {
        AdStorageQuery asq = new AdStorageQuery("ARC");
        String query = asq.getQuery();
        Assert.assertNotNull(query);
        Assert.assertTrue(query.contains("archiveName = 'ARC'"));
    }

    @Test
    public void testQueryVOSpacBuckets() throws Exception {
        AdStorageQuery asq = new AdStorageQuery("VOSpac:0");
        String query = asq.getQuery();
        String expected =
                "SELECT uri, inventoryURI, contentMD5, fileSize, ingestDate, contentEncoding, "
                +      "contentType , convert('binary', convert('smallint', contentMD5)) as bucket "
                + "FROM archive_files "
                + "WHERE archiveName = 'VOSpac'  AND contentMD5 between '0000000000000000' AND "
                +       "'0fffffffffffffff' ORDER BY bucket,  uri ASC, ingestDate DESC";
        Assert.assertEquals("archive query constraints", expected, query);

        asq = new AdStorageQuery("VOSpac:fff");
        query = asq.getQuery();
        Assert.assertTrue("archive query constraints", query.contains("WHERE archiveName = 'VOSpac'  "
                + "AND contentMD5 between 'fff0000000000000' AND 'ffffffffffffffff'"));

        // bucket size
        Assert.assertThrows(IllegalArgumentException.class, () -> new AdStorageQuery("VOSpac:"));
        Assert.assertThrows(IllegalArgumentException.class, () -> new AdStorageQuery("VOSpac:12345"));
        // bucket not hex
        Assert.assertThrows(IllegalArgumentException.class, () -> new AdStorageQuery("VOSpac:zzz"));

    }
    
    @Test
    public void testRowMapper() throws Exception {
        AdStorageQuery query = new AdStorageQuery("FOO");
        TapRowMapper mapper = query.getRowMapper();
        
        ArrayList<Object> row = new ArrayList<>(7);
        row.add(new URI("ad:FOO/foo.fits.gz"));
        row.add(new URI("cadc:FOO/foo.fits.gz"));
        row.add("e687e2ecea45e78822eb68294566e6a1");
        row.add(new Long(33));
        Date now = new Date();
        row.add(now);
        row.add("gzip");
        row.add("application/fits");
       
        StorageMetadata metadata = (StorageMetadata) mapper.mapRow(row);
        Assert.assertTrue("cadc:FOO/foo.fits.gz".equals(metadata.getArtifactURI().toString()));
        Assert.assertTrue("ad:FOO/foo.fits.gz".equals(metadata.getStorageLocation().getStorageID().toString()));
        Assert.assertTrue(metadata.getStorageLocation().storageBucket.equals("FOO"));
        Assert.assertTrue(metadata.getContentChecksum().toString().equals("md5:e687e2ecea45e78822eb68294566e6a1"));
        Assert.assertEquals(33, metadata.getContentLength().longValue());
        Assert.assertEquals(now, metadata.getContentLastModified());
        Assert.assertTrue(metadata.contentEncoding.equals("gzip"));
        Assert.assertTrue(metadata.contentType.equals("application/fits"));
    }
    
    @Test
    public void testRowMapperDisambiguate() throws Exception {
        AdStorageQuery query = new AdStorageQuery("x-FOO");
        TapRowMapper mapper = query.getRowMapper();
        
        ArrayList<Object> row = new ArrayList<>(7);
        row.add(new URI("ad:FOO/foo.fits.gz"));
        row.add(new URI("cadc:FOO/foo.fits.gz"));
        row.add("e687e2ecea45e78822eb68294566e6a1");
        row.add(new Long(33));
        Date now = new Date();
        row.add(now);
        row.add("gzip");
        row.add("application/fits");
       
        StorageMetadata metadata = (StorageMetadata) mapper.mapRow(row);
        Assert.assertTrue("cadc:FOO/foo.fits.gz".equals(metadata.getArtifactURI().toString()));
        Assert.assertTrue("ad:FOO/foo.fits.gz".equals(metadata.getStorageLocation().getStorageID().toString()));
        Assert.assertTrue(metadata.getStorageLocation().storageBucket.equals("x-FOO"));
        Assert.assertTrue(metadata.getContentChecksum().toString().equals("md5:e687e2ecea45e78822eb68294566e6a1"));
        Assert.assertEquals(33, metadata.getContentLength().longValue());
        Assert.assertEquals(now, metadata.getContentLastModified());
        Assert.assertTrue(metadata.contentEncoding.equals("gzip"));
        Assert.assertTrue(metadata.contentType.equals("application/fits"));
    }
    
    @Test
    public void testRowMapperGeminiWorkaround() throws Exception {
        AdStorageQuery query = new AdStorageQuery("FOO");
        TapRowMapper mapper = query.getRowMapper();
        
        ArrayList<Object> row = new ArrayList<>(7);
        //  work-around: archive_files.uri scheme is gemini
        row.add(new URI("gemini:FOO/foo.fits.gz"));
        row.add(new URI("cadc:FOO/foo.fits.gz"));
        row.add("e687e2ecea45e78822eb68294566e6a1");
        row.add(new Long(33));
        Date now = new Date();
        row.add(now);
        row.add("gzip");
        row.add("application/fits");
       
        StorageMetadata metadata = (StorageMetadata) mapper.mapRow(row);
        Assert.assertTrue("cadc:FOO/foo.fits.gz".equals(metadata.getArtifactURI().toString()));
        // work-around: storageID scheme is ad
        Assert.assertTrue("ad:FOO/foo.fits.gz".equals(metadata.getStorageLocation().getStorageID().toString()));
        Assert.assertTrue(metadata.getStorageLocation().storageBucket.equals("FOO"));
        Assert.assertTrue(metadata.getContentChecksum().toString().equals("md5:e687e2ecea45e78822eb68294566e6a1"));
        Assert.assertEquals(33, metadata.getContentLength().longValue());
        Assert.assertEquals(now, metadata.getContentLastModified());
        Assert.assertTrue(metadata.contentEncoding.equals("gzip"));
        Assert.assertTrue(metadata.contentType.equals("application/fits"));
    }

    @Test
    public void testRowMapperVault() throws Exception {
        AdStorageQuery query = new AdStorageQuery("VOSpac:12");
        TapRowMapper mapper = query.getRowMapper();

        ArrayList<Object> row = new ArrayList<>(7);
        //  work-around: archive_files.uri scheme is gemini
        row.add(new URI("ad:VOSpac/foo"));
        row.add(new URI("cadc:vault/foo"));
        row.add("1237e2ecea45e78822eb68294566e6a1");
        row.add(new Long(33));
        Date now = new Date();
        row.add(now);
        row.add("");
        row.add("application/octet-stream");

        StorageMetadata metadata = (StorageMetadata) mapper.mapRow(row);
        Assert.assertTrue("VOSpac:1237".equals(metadata.getStorageLocation().storageBucket));
    }
}

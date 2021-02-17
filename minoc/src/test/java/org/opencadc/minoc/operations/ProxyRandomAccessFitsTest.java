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

package org.opencadc.minoc.operations;

import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.List;

import nom.tam.fits.Header;
import nom.tam.fits.HeaderCard;
import nom.tam.util.Cursor;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.fits.FitsOperations;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter;

/**
 * Test ProxyRandomAccessFits wrapper using filesystem storage adapter for the back end.
 * 
 * @author pdowler
 */
public class ProxyRandomAccessFitsTest {
    private static final Logger log = Logger.getLogger(ProxyRandomAccessFitsTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.minoc.operations", Level.DEBUG);
        //Log4jInit.setLevel("org.opencadc.inventory.storage.fs", Level.DEBUG);
    }

    public ProxyRandomAccessFitsTest() { 
    }

    @Test
    public void testGetPrimaryHeader() {
        try {
            // setup
            File testFile = FileUtil.getFileFromResource("sample-mef.fits", ProxyRandomAccessFitsTest.class);
            FileInputStream fis = new FileInputStream(testFile);
            File rootDir = new File("build/tmp/saroot");
            if (!rootDir.exists()) {
                rootDir.mkdirs();
            }
            StorageAdapter sa = new OpaqueFileSystemStorageAdapter(rootDir, 1);
            NewArtifact na = new NewArtifact(URI.create("cadc:TEST/" + testFile.getName()));
            na.contentLength = testFile.length();
            StorageMetadata sm = sa.put(na, fis, null);
            ProxyRandomAccessFits in = new ProxyRandomAccessFits(sa, sm.getStorageLocation(), testFile.length());
            
            FitsOperations fop = new FitsOperations(in);
            Header h = fop.getPrimaryHeader();
            //h.dumpHeader(System.out);
            Cursor<String,HeaderCard> iter = h.iterator();
            for (int i = 0; i <= 5; i++) {
                HeaderCard hc = iter.next();
                log.info(hc.getKey() + " = " + hc.getValue());
            }
            log.info("...");

            long nbytes = h.getDataSize();
            log.info("data size: " + nbytes);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetHeaders() {
        try {
            // setup
            File testFile = FileUtil.getFileFromResource("sample-mef.fits", ProxyRandomAccessFitsTest.class);
            FileInputStream fis = new FileInputStream(testFile);
            File rootDir = new File("build/tmp/saroot");
            if (!rootDir.exists()) {
                rootDir.mkdirs();
            }
            StorageAdapter sa = new OpaqueFileSystemStorageAdapter(rootDir, 1);
            NewArtifact na = new NewArtifact(URI.create("cadc:TEST/" + testFile.getName()));
            na.contentLength = testFile.length();
            StorageMetadata sm = sa.put(na, fis, null);
            ProxyRandomAccessFits in = new ProxyRandomAccessFits(sa, sm.getStorageLocation(), testFile.length());

            FitsOperations fop = new FitsOperations(in);
            List<Header> hdrs = fop.getHeaders();
            
            for (int i = 0; i < hdrs.size(); i++) {
                Header h = hdrs.get(i);
                log.info("** header: " + i);
                Cursor<String,HeaderCard> iter = h.iterator();
                for (int c = 0; c <= 5; c++) {
                    HeaderCard hc = iter.next();
                    log.info(hc.getKey() + " = " + hc.getValue());
                }
                long nbytes = h.getDataSize();
                log.info("** data size: " + nbytes);
                log.info("...");
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}

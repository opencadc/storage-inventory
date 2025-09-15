/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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

package org.opencadc.inventory.storage.eos;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.PropertiesReader;
import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 *
 * @author pdowler
 */
public class EosFindTest {
    private static final Logger log = Logger.getLogger(EosFindTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage", Level.INFO);
    }

    EosFind eos = new EosFind(URI.create("root://eos-mgm.keel-dev.arbutus.cloud"), 
            "/eos/keel-dev.arbutus.cloud/data/lsst", "zteos64:invalid-token", "lsst");

    public EosFindTest() { 
    }

    @Test
    public void testPathStorageLocation() throws Exception {
        String path = "users/fabio/hello.txt";
        StorageLocation sloc = eos.pathToStorageLocation(path);
        log.info("sloc: " + sloc);
        Assert.assertNotNull(sloc);
        Assert.assertEquals("hello.txt", sloc.getStorageID().toASCIIString());
        Assert.assertEquals("users/fabio", sloc.storageBucket);
    }

    @Test
    public void testParseFileInfoSkip() throws Exception {
        String fileInfoLine = "";
        StorageMetadata sm = eos.parseFileInfo(fileInfoLine);
        Assert.assertNull(sm);
        
        try {
            sm = eos.parseFileInfo("blah blah");
            Assert.fail("blah blah");
        } catch (StorageEngageException expected) {
            log.info("caught expected: " + expected);
        }
        
        sm = eos.parseFileInfo("");
        Assert.assertNull(sm);
        
        sm = eos.parseFileInfo(" ");
        Assert.assertNull(sm);
    }

    @Test
    public void testParseFileInfo() throws Exception {
        // eos find -f --fileinfo /eos/keel-dev.arbutus.cloud/data/lsst/users
        // date from eos ls -l: 2025-03-18T08:19:12.102
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        final Date expectedDate = df.parse("2025-03-18T08:19:12.102");
        
        // eos find -f --fileinfo:
        String fileInfoLine = "keylength.file=59 file=/eos/keel-dev.arbutus.cloud/data/lsst/users/fabio/hello.txt size=12 status=healthy"
            + " mtime=1742285952.707288000 ctime=1742285952.102409887 btime=1742285952.102409887 atime=1742285952.102410975"
            + " clock=1751496240343894024 mode=0644 uid=5000 gid=5000 fxid=00000007 fid=7 ino=1879048192 pid=21 pxid=00000015"
            + " xstype=adler xs=1e3d045f etag=\"1879048192:1e3d045f\""
            + " detached=0 layout=plain nstripes=1 lid=00100002 nrep=1 xattrn=sys.eos.btime xattrv=1742285952.102409887"
            + " xattrn=sys.fs.tracking xattrv=+3 xattrn=sys.utrace xattrv=ac39a4b8-03d1-11f0-9ee4-423bedf0b9a2"
            + " xattrn=sys.vtrace xattrv=[Tue Mar 18 09:19:12 2025] uid:5000[lsst] gid:5000[lsst]"
            + " tident:http name:lsst dn: prot:https app:http host:[::ffff:134.158.240.252] domain:158.240.252] geo: sudo:0 fsid=3";
        
        // eos find -f --format path,size,checksumtype,checksum,ctime
        String findLine = "path=\"/eos/keel-dev.arbutus.cloud/data/lsst/users/fabio/hello.txt\""
                + " size=12 checksumtype=adler checksum=1e3d045f ctime=1742285952.102409887";
        
        StorageMetadata sm = eos.parseFileInfo(findLine);
        log.info("parsed: " + sm);
        Assert.assertNotNull(sm);
        
        Assert.assertEquals("lsst:users/fabio/hello.txt", sm.getArtifactURI().toASCIIString());
        Assert.assertEquals(12L, sm.getContentLength().longValue());
        Assert.assertEquals("adler:1e3d045f", sm.getContentChecksum().toASCIIString());
        Assert.assertEquals(expectedDate.getTime(), sm.getContentLastModified().getTime());
        StorageLocation sloc = sm.getStorageLocation();
        Assert.assertEquals("hello.txt", sloc.getStorageID().toASCIIString());
        Assert.assertEquals("users/fabio", sloc.storageBucket);
    }
}

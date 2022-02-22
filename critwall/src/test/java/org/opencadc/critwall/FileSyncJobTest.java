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

package org.opencadc.critwall;

import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.storage.PutTransaction;

/**
 *
 * @author pdowler
 */
public class FileSyncJobTest {
    private static final Logger log = Logger.getLogger(FileSyncJobTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.critwall", Level.INFO);
    }
    
    public FileSyncJobTest() { 
    }
    
    @Test
    public void testSegmentPlan() {
        try {
            
            Artifact a = new Artifact(URI.create("cadc:TEST/foo"), 
                new URI("md5:646d3c548ffb98244a0fc52b60556082"), new Date(), 1008000L);
            
            PutTransaction[] txns = new PutTransaction[] {
                new PutTransaction("limit-none", 1L, null),
                new PutTransaction("limit-mid", 256 * 1024L, 256 * 1024L),
                new PutTransaction("limit-range", 256 * 1024L, 2 * a.getContentLength()),
                new PutTransaction("limit-large", 2 * a.getContentLength(), 2 * a.getContentLength())
            };
            int[] expectedNumSegments = new int[] { 2, 4, 2, 1 };
            
            FileSyncJob.SEGMENT_SIZE_PREF = 512 * 1024L; // 512KiB
            log.info("FileSyncJob pref: " + FileSyncJob.SEGMENT_SIZE_PREF);
            
            for (int i = 0; i < txns.length; i++) {
                PutTransaction pt = txns[i];
                int expnum = expectedNumSegments[i];
                
                log.info("txn: " + pt);
                List<FileSyncJob.PutSegment> segs = FileSyncJob.getSegmentPlan(a, pt);
                Assert.assertEquals("num segments", expnum, segs.size());
                
                long totLen = 0L;
                for (FileSyncJob.PutSegment s : segs) {
                    totLen += s.contentLength;
                    log.info("length: " + s.contentLength + " range: " + s.getRangeHeaderVal());
                }
                Assert.assertEquals("total content length", a.getContentLength().longValue(), totLen);
                log.info("txn: " + pt + " DONE");
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception: " + unexpected);
            Assert.fail("unexpected exception");
        }
    }
    

}

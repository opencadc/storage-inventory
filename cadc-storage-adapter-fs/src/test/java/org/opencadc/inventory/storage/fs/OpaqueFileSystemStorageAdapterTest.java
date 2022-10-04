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

package org.opencadc.inventory.storage.fs;

import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.Log4jInit;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;

/**
 * Unit tests that create base directory structure but do not otherwise
 * interact with the file system.
 * 
 * @author pdowler
 */
public class OpaqueFileSystemStorageAdapterTest {
    private static final Logger log = Logger.getLogger(OpaqueFileSystemStorageAdapterTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage.fs", Level.INFO);
    }
    
    OpaqueFileSystemStorageAdapter adapter;
    int depth = 2;
            
    public OpaqueFileSystemStorageAdapterTest() throws InvalidConfigException {
        File tmp = new File("build/tmp");
        File root = new File(tmp, "opaque-unit-tests");
        root.mkdir();

        this.adapter = new OpaqueFileSystemStorageAdapter(root, depth); // opaque
        log.info("    content path: " + adapter.contentPath);
        log.info("transaction path: " + adapter.txnPath);
        Assert.assertTrue("testInit: contentPath", Files.exists(adapter.contentPath));
        Assert.assertTrue("testInit: txnPath", Files.exists(adapter.txnPath));
    }
    
    @Test
    public void testStorageLocationMethods() {
        try {
            Path p = adapter.txnPath.resolve(UUID.randomUUID().toString());
            StorageLocation sloc = adapter.pathToStorageLocation(p);
            log.info("created: " + sloc);
            Assert.assertNotNull(sloc);
            Assert.assertNotNull(sloc.storageBucket);
            Assert.assertEquals(depth, sloc.storageBucket.length());
            
            Path pth = adapter.storageLocationToPath(sloc);
            log.info("path: " + pth);
            Assert.assertNotNull(pth);
            int num = 0;
            Path rel = adapter.contentPath.relativize(pth);
            Iterator<Path> i = rel.iterator();
            while (i.hasNext()) {
                log.info("comp: " + i.next());
                num++;
            }
            Assert.assertEquals("relative path depth", (depth + 1), num);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    
}

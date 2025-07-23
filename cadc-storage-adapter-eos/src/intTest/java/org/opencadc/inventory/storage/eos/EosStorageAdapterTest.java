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

import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.MessageDigestAPI;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * @author pdowler
 */
public class EosStorageAdapterTest {
    
    private static final Logger log = Logger.getLogger(EosStorageAdapterTest.class);
    
    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.net", Level.DEBUG);
    }
    
    final EosStorageAdapter eosAdapter;
    
    public EosStorageAdapterTest() throws InvalidConfigException {
        this.eosAdapter = new EosStorageAdapter();    
    }

    @Test
    public void testGet() {
        StorageLocation loc = new StorageLocation(URI.create("hello.txt"));
        loc.storageBucket = "users/fabio";
        URI contentChecksum = URI.create("adler:1e3d045f");
        
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            eosAdapter.get(loc, bos);
            byte[] data = bos.toByteArray();
            Assert.assertNotNull(data);
            Assert.assertEquals(12, data.length);
            log.info("data: len=" + data.length);
            String content = new String(data, StandardCharsets.UTF_8);
            log.info("data: " + content);
            
            MessageDigestAPI digest = MessageDigestAPI.getInstance(contentChecksum.getScheme());
            digest.update(data);
            byte[] metaChecksumBytes = digest.digest();
            String hexMetaChecksum = HexUtil.toHex(metaChecksumBytes);
            String alg = digest.getAlgorithmName().toLowerCase();
            URI actualChecksum = new URI(alg, hexMetaChecksum, null);
            log.info("actual checksum: " + actualChecksum);
            Assert.assertEquals(contentChecksum, actualChecksum);
                    
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetByteRange() {
        StorageLocation loc = new StorageLocation(URI.create("hello.txt"));
        loc.storageBucket = "users/fabio";
        ByteRange range = new ByteRange(0, 4);
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            eosAdapter.get(loc, bos, range);
            byte[] data = bos.toByteArray();
            Assert.assertNotNull(data);
            Assert.assertEquals(4, data.length);
            log.info("data: len=" + data.length);
            String content = new String(data, StandardCharsets.UTF_8);
            log.info("data: " + content);
            Assert.assertEquals("hell", content);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testIterator() {
        ResourceIterator<StorageMetadata> iter = null;
        try {
            iter = (ResourceIterator) eosAdapter.iterator();
            int num = 0;
            while (iter.hasNext()) {
                StorageMetadata sm = iter.next();
                Assert.assertNotNull(sm);
                log.info("found: " + sm);
                num++;
            }
            log.info("files found: " + num);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            if (iter != null) {
                try {
                    iter.close();
                } catch (Exception ex) {
                    log.error("failed to close iter: ", ex);
                }
            }
        }         
        
    }
}

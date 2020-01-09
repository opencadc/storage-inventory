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
************************************************************************
*/

package org.opencadc.inventory;

import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Date;
import java.util.UUID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class EntityTest {
    private static final Logger log = Logger.getLogger(EntityTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.si", Level.INFO);
    }
    
    public EntityTest() { 
    }
    
    @Test
    public void testTemplate() {
        //Assert.fail("not implemented");
    }
    
    @Test
    public void testArtifact() {
        URI uri = URI.create("cadc:FOO/bar");
        URI contentChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
        Date contentLastModified = new Date();
        Long contentLength = 1024L;
        
        try {
            Artifact ok = new Artifact(uri, contentChecksum, contentLastModified, contentLength);
            log.info("created: " + ok);
            Assert.assertEquals(uri, ok.getURI());
            Assert.assertEquals(contentChecksum, ok.getContentChecksum());
            Assert.assertEquals(contentLastModified, ok.getContentLastModified());
            Assert.assertEquals(contentLength, ok.getContentLength());
            Assert.assertNotNull(ok.getBucket());
            
            UUID id = UUID.randomUUID();
            Artifact recon = new Artifact(id, uri, contentChecksum, contentLastModified, contentLength);
            log.info("created: " + recon);
            Assert.assertEquals(id, recon.getID());
            Assert.assertEquals(uri, recon.getURI());
            Assert.assertEquals(contentChecksum, recon.getContentChecksum());
            Assert.assertEquals(contentLastModified, recon.getContentLastModified());
            Assert.assertEquals(contentLength, recon.getContentLength());
            Assert.assertNotNull(recon.getBucket());
            
            try {
                Artifact invalid = new Artifact(null, contentChecksum, contentLastModified, contentLength);
                Assert.fail("created: " + invalid);
            } catch (IllegalArgumentException expected) {
                log.info("expected: " + expected);
            }
            
            try {
                Artifact invalid = new Artifact(uri, null, contentLastModified, contentLength);
                Assert.fail("created: " + invalid);
            } catch (IllegalArgumentException expected) {
                log.info("expected: " + expected);
            }
            
            try {
                Artifact invalid = new Artifact(uri, contentChecksum, null, contentLength);
                Assert.fail("created: " + invalid);
            } catch (IllegalArgumentException expected) {
                log.info("expected: " + expected);
            }
            
            try {
                Artifact invalid = new Artifact(uri, contentChecksum, contentLastModified, null);
                Assert.fail("created: " + invalid);
            } catch (IllegalArgumentException expected) {
                log.info("expected: " + expected);
            }
            
            try {
                Artifact invalid = new Artifact(uri, contentChecksum, contentLastModified, -1L);
                Assert.fail("created: " + invalid);
            } catch (IllegalArgumentException expected) {
                log.info("expected: " + expected);
            }
            
            // invalid URI
            try {
                URI u2 = URI.create("cadc:/foo/bar"); // absolute path
                Artifact invalid = new Artifact(u2, contentChecksum, contentLastModified, -1L);
                Assert.fail("created: " + invalid);
            } catch (IllegalArgumentException expected) {
                log.info("expected: " + expected);
            }
            
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testArtifactTransientState() {
        URI uri = URI.create("cadc:FOO/bar");
        URI contentChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
        Date contentLastModified = new Date();
        Long contentLength = 1024L;
        
        try {
            Artifact ok = new Artifact(uri, contentChecksum, contentLastModified, contentLength);
            log.info("created: " + ok);
            
            URI mcs1 = ok.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            
            //ok.storageLocation = new StorageLocation(ok.getID(), URI.create("ceph:" + UUID.randomUUID()));
            ok.storageLocation = new StorageLocation(URI.create("ceph:" + UUID.randomUUID()));
            URI mcs2 = ok.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals(mcs1, mcs2);
            
            //ok.siteLocations.add(new SiteLocation(ok.getID(), UUID.randomUUID()));
            ok.siteLocations.add(new SiteLocation(UUID.randomUUID()));
            URI mcs3 = ok.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals(mcs1, mcs3);
            
            try {
                DeletedArtifactEvent invalid = new DeletedArtifactEvent(null);
                Assert.fail("created: " + invalid);
            } catch (IllegalArgumentException expected) {
                log.info("expected: " + expected);
            }
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testDeletedArtifactEvent() {
        try {
            DeletedArtifactEvent ok = new DeletedArtifactEvent(UUID.randomUUID());
            log.info("created: " + ok);
            
            try {
                DeletedArtifactEvent invalid = new DeletedArtifactEvent(null);
                Assert.fail("created: " + invalid);
            } catch (IllegalArgumentException expected) {
                log.info("expected: " + expected);
            }
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testDeletedStorageLocationEvent() {
        try {
            DeletedStorageLocationEvent ok = new DeletedStorageLocationEvent(UUID.randomUUID());
            log.info("created: " + ok);
            
            try {
                DeletedStorageLocationEvent invalid = new DeletedStorageLocationEvent(null);
                Assert.fail("created: " + invalid);
            } catch (IllegalArgumentException expected) {
                log.info("expected: " + expected);
            }
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testStorageSite() {
        final URI resourceID = URI.create("ivo://example.net/foo");
        final String name = "foo";
        
        final URI resourceID2 = URI.create("ivo://example.net/bar");
        final String name2 = "flibble";
                
        try {
            StorageSite ok = new StorageSite(resourceID, name);
            log.info("created: " + ok);
            Assert.assertEquals(resourceID, ok.getResourceID());
            Assert.assertEquals(name, ok.getName());

            UUID id = UUID.randomUUID();
            StorageSite recon = new StorageSite(id, resourceID, name);
            log.info("created: " + recon);
            Assert.assertEquals(id, recon.getID());
            Assert.assertEquals(resourceID, recon.getResourceID());
            Assert.assertEquals(name, recon.getName());

            // rename
            StorageSite ren = new StorageSite(resourceID, name);
            log.info("created: " + ren);
            ren.setResourceID(resourceID2);
            Assert.assertEquals(resourceID2, ren.getResourceID());
            Assert.assertEquals(name, ren.getName());
            ren.setName(name2);
            Assert.assertEquals(resourceID2, ren.getResourceID());
            Assert.assertEquals(name2, ren.getName());
            
            recon.setResourceID(resourceID);
            try {
                StorageSite invalid = new StorageSite(null, name);
                Assert.fail("created: " + invalid);
            } catch (IllegalArgumentException expected) {
                log.info("expected: " + expected);
            }
            
            try {
                StorageSite invalid = new StorageSite(resourceID, null);
                Assert.fail("created: " + invalid);
            } catch (IllegalArgumentException expected) {
                log.info("expected: " + expected);
            }
            
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
}

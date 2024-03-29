
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
 *
 ************************************************************************
 */

package org.opencadc.tantar;

import ca.nrc.cadc.util.Log4jInit;

import java.net.URI;
import java.util.Date;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.StoredArtifactComparator;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.tantar.policy.InventoryIsAlwaysRight;
import org.opencadc.tantar.policy.ResolutionPolicy;

/**
 * Test that ResolutionPolicy implementations log the correct events.
 * 
 * @author pdowler
 */
public class BucketValidatorTest {
    private static final Logger log = Logger.getLogger(BucketValidatorTest.class);
    
    static {
        Log4jInit.setLevel("org.opencadc.tantar", Level.INFO);
    }

    @Test
    public void testIteratorMerge() throws Exception {
        SortedSet<StorageMetadata> stored = new TreeSet<>();
        SortedSet<Artifact> artifacts = new TreeSet<>(new StoredArtifactComparator());

        StorageMetadata sm = new StorageMetadata(new StorageLocation(URI.create("id:123")), URI.create("test:FOO/bar1"), 
                URI.create("md5:a5b4861ccc5da8454e84fc1a686e40aa"), 1024L, new Date());
        Artifact a = new Artifact(sm.getArtifactURI(), sm.getContentChecksum(), sm.getContentLastModified(), sm.getContentLength());
        a.storageLocation = sm.getStorageLocation();
        stored.add(sm);
        artifacts.add(a);
        
        sm = new StorageMetadata(new StorageLocation(URI.create("id:456")), URI.create("test:FOO/bar2"), 
                URI.create("md5:a5b4861ccc5da8454e84fc1a686e40aa"), 1024L, new Date());
        a = new Artifact(sm.getArtifactURI(), sm.getContentChecksum(), sm.getContentLastModified(), sm.getContentLength());
        a.storageLocation = sm.getStorageLocation();
        stored.add(sm);
        artifacts.add(a);
        
        sm = new StorageMetadata(new StorageLocation(URI.create("id:789")), URI.create("test:FOO/bar3"), 
                URI.create("md5:a5b4861ccc5da8454e84fc1a686e40aa"), 1024L, new Date());
        a = new Artifact(sm.getArtifactURI(), sm.getContentChecksum(), sm.getContentLastModified(), sm.getContentLength());
        a.storageLocation = sm.getStorageLocation();
        stored.add(sm);
        artifacts.add(a);
    
        final TestPolicy testPolicy = new TestPolicy();
        BucketValidator bv = new BucketValidator(testPolicy) {
            @Override
            Iterator<StorageMetadata> getStorageIterator() throws StorageEngageException {
                return stored.iterator();
            }

            @Override
            Iterator<Artifact> getInventoryIterator() {
                return artifacts.iterator();
            }
        };
    
        bv.validate();
        Assert.assertEquals("number validated", artifacts.size(), testPolicy.numArtifacts);
        Assert.assertEquals("number validated", stored.size(), testPolicy.numStorage);
        Assert.assertEquals("number validated", 3, testPolicy.numMatch);
    }
    
    // TODO: test ValdiateEventListener methods for correctness
    
    
    String random16Bytes() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }
    
    private class TestPolicy extends ResolutionPolicy {

        int numArtifacts = 0;
        int numStorage = 0;
        int numMatch = 0;
        
        @Override
        public void validate(Artifact artifact, StorageMetadata storageMetadata) throws Exception {
            log.info("validate: " + artifact + " vs " + storageMetadata);
            if (artifact != null) {
                numArtifacts++;
            }
            if (storageMetadata != null) {
                numStorage++;
            }
            if (artifact != null && storageMetadata != null) {
                numMatch++;
            }
        }
        
    }
    
    private class TestActions implements ValidateActions {

        int num = 0;
        
        @Override
        public Artifact getArtifact(URI uri) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createArtifact(StorageMetadata storageMetadata) throws Exception {
            num++;
        }

        @Override
        public void delete(StorageMetadata storageMetadata) throws Exception {
            num++;
        }

        @Override
        public void delete(Artifact artifact) throws Exception {
            num++;
        }

        @Override
        public void clearStorageLocation(Artifact artifact) throws Exception {
            num++;
        }

        @Override
        public void replaceArtifact(Artifact artifact, StorageMetadata storageMetadata) throws Exception {
            num++;
        }

        @Override
        public void updateArtifact(Artifact artifact, StorageMetadata storageMetadata) throws Exception {
            num++;
        }

        @Override
        public void delayAction() {
            num++;
        }
        
    }
}

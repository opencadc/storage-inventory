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

package org.opencadc.tantar;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.DeletedStorageLocationEventDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.db.StorageLocationEventDAO;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.PutTransaction;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter;
import org.opencadc.tantar.policy.ResolutionPolicy;

/**
 * Basic tantar integration test
 * @author pdowler
 */
abstract class TantarTest {
    private static final Logger log = Logger.getLogger(TantarTest.class);

    static File ROOT = new File("build/tmp/storage");
    static final String SERVER = "INVENTORY_TEST";
    static final String DATABASE = "cadctest";
    
    static {
        Log4jInit.setLevel("org.opencadc.tantar", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.storage.fs", Level.INFO);
        ROOT.mkdirs();
    }
    
    final StorageAdapter adapter;
    final ArtifactDAO artifactDAO;
    final StorageLocationEventDAO sleDAO;
    final DeletedArtifactEventDAO daeDAO;
    final DeletedStorageLocationEventDAO dsleDAO;
    
    final BucketValidator validator;
    
    protected TantarTest(ResolutionPolicy policy) throws Exception {
        this.adapter = new OpaqueFileSystemStorageAdapter(ROOT, 1);
        
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(SERVER, DATABASE);

        Map<String,Object> daoConfig = new TreeMap<>();
        daoConfig.put(SQLGenerator.class.getName(), SQLGenerator.class);
        daoConfig.put("schema", "inventory");
        
        this.validator = new BucketValidator(daoConfig, cc, adapter, policy, "0-f", false);
        
        this.artifactDAO = new ArtifactDAO();
        artifactDAO.setConfig(daoConfig);
        sleDAO = new StorageLocationEventDAO(artifactDAO);
        daeDAO = new DeletedArtifactEventDAO(artifactDAO);
        dsleDAO = new DeletedStorageLocationEventDAO(artifactDAO);
    }
    
    void cleanupBefore() throws Exception {

        log.debug("cleaning stored artifacts...");
        Iterator<Artifact> storedArtifacts = artifactDAO.storedIterator(null);
        log.debug("got an iterator back: " + storedArtifacts);
        cleanupDatabase(storedArtifacts);

        log.debug("cleaning unstored artifacts...");
        Iterator<Artifact> unstoredArtifacts = artifactDAO.unstoredIterator(null);
        log.debug("got an iterator back: " + storedArtifacts);
        cleanupDatabase(unstoredArtifacts);

        // Clean up all test content in fsroot
        log.debug("deleting contents of test directories in: " + ROOT);
        Iterator<StorageMetadata> smi = adapter.iterator(null, true);
        while (smi.hasNext()) {
            StorageLocation loc = smi.next().getStorageLocation();
            log.debug("delete storage: " + loc);
            adapter.delete(loc);
        }
    }
    
    private void cleanupDatabase(Iterator<Artifact> artifactIterator) {
        log.debug("wipe_clean running...");
        int deletedArtifacts = 0;

        while (artifactIterator.hasNext()) {
            log.debug("something to remove...");
            Artifact a = artifactIterator.next();
            log.debug("deleting test uri: " + a.getURI() + " ID: " + a.getID());
            artifactDAO.delete(a.getID());
            deletedArtifacts++;
        }

        if (deletedArtifacts > 0) {
            log.info("deleted " + deletedArtifacts + " artifacts");
        }
    }

    private Artifact create(StorageMetadata sm) {
        return new Artifact(sm.getArtifactURI(), sm.getContentChecksum(), sm.getContentLastModified(), sm.getContentLength());
    }
    
    // correct artifact+storage
    // a1,a3,a5: artifacts with storageLocation + matching stored object
    //
    // discrepancies:
    // a2: artifact  with storageLocation + no stored object
    // a4: artifact with no storageLocation + stored object with same artifact uri/metadata (recoverable)
    // a6: stored object with no matching artifact
    // a7: artifact with storageLocation + stored object with different metadata (checksum) (metadata conflict)
    // a8: artifact with no storageLocation + stored object with same artifact uri/different metadata (not recoverable)
    
    Artifact a4_recoverable;
    
    void doTestSetup() throws Exception {
        // create
        StorageMetadata sm1 = adapter.put(new NewArtifact(URI.create("test:FOO/a1")), getInputStreamOfRandomBytes(1024L), null);
        final Artifact a1 = create(sm1);
        
        StorageMetadata sm2 = adapter.put(new NewArtifact(URI.create("test:FOO/a2")), getInputStreamOfRandomBytes(1024L), null);
        final Artifact a2 = create(sm2);
        
        StorageMetadata sm3 = adapter.put(new NewArtifact(URI.create("test:FOO/a3")), getInputStreamOfRandomBytes(1024L), null);
        final Artifact a3 = create(sm3);
        
        StorageMetadata sm4 = adapter.put(new NewArtifact(URI.create("test:FOO/a4")), getInputStreamOfRandomBytes(1024L), null);
        final Artifact a4 = create(sm4);
        
        StorageMetadata sm5 = adapter.put(new NewArtifact(URI.create("test:FOO/a5")), getInputStreamOfRandomBytes(1024L), null);
        final Artifact a5 = create(sm5);
        
        StorageMetadata sm6 = adapter.put(new NewArtifact(URI.create("test:FOO/a6")), getInputStreamOfRandomBytes(1024L), null);
        final Artifact a6 = create(sm6);

        StorageMetadata sm7 = adapter.put(new NewArtifact(URI.create("test:FOO/a7")), getInputStreamOfRandomBytes(1024L), null);
        final Artifact a7 = create(sm7);
        
        StorageMetadata sm8 = adapter.put(new NewArtifact(URI.create("test:FOO/a8")), getInputStreamOfRandomBytes(1024L), null);
        final Artifact a8 = create(sm8);
        
        // setup
        a1.storageLocation = sm1.getStorageLocation();
        artifactDAO.put(a1);
        
        a2.storageLocation = sm2.getStorageLocation();
        artifactDAO.put(a2);
        adapter.delete(sm2.getStorageLocation());
        
        a3.storageLocation = sm3.getStorageLocation();
        artifactDAO.put(a3);
        
        // no storageLocation
        artifactDAO.put(a4);
        this.a4_recoverable = a4;
        
        a5.storageLocation = sm5.getStorageLocation();
        artifactDAO.put(a5);
        
        // a6: no artifact
        
        // metadata conflict
        adapter.delete(sm7.getStorageLocation());
        sm7 = adapter.put(new NewArtifact(URI.create("test:FOO/a7")), getInputStreamOfRandomBytes(1024L), null);
        a7.storageLocation = sm7.getStorageLocation();
        artifactDAO.put(a7);
        
        // different storage location
        a8.storageLocation = sm8.getStorageLocation();
        artifactDAO.put(a8);
        adapter.delete(sm8.getStorageLocation());
        adapter.put(new NewArtifact(URI.create("test:FOO/a8")), getInputStreamOfRandomBytes(1024L), null);
    }
    
    public static InputStream getInputStreamOfRandomBytes(long numBytes) {
        
        Random rnd = new Random();
        
        return new InputStream() {
            long tot = 0L;
            
            @Override
            public int read() throws IOException {
                if (tot == numBytes) {
                    return -1;
                }
                tot++;
                return rnd.nextInt(255);
            }
            
            @Override
            public int read(byte[] bytes) throws IOException {
                return read(bytes, 0, bytes.length);
            }

            @Override
            public int read(byte[] bytes, int off, int len) throws IOException {
                if (tot == numBytes) {
                    return -1;
                }
                int num = len;
                if (tot + len > numBytes) {
                    num = (int) (numBytes - tot);
                }
                byte val = (byte) rnd.nextInt(255);
                Arrays.fill(bytes, off, off + num - 1, val);
                tot += num;
                return num;
            }
        };
    }
}

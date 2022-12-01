
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

package org.opencadc.tantar.policy;

import org.opencadc.tantar.policy.InventoryIsAlwaysRight;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;

public class InventoryIsAlwaysRightTest extends AbstractResolutionPolicyTest<InventoryIsAlwaysRight> {
    
    @Test
    public void resolveArtifactAndStorageMetadata() throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final Artifact artifact = new Artifact(URI.create("cadc:bucket/file.fits"),
                                               URI.create("md5:" + random16Bytes()), new Date(),
                                               88L);

        final StorageMetadata storageMetadata = new StorageMetadata(new StorageLocation(URI.create("s3:989877")), URI.create("test:989877"),
                                                        URI.create("md5:" + random16Bytes()), 1001L, new Date());
        
        artifact.storageLocation = storageMetadata.getStorageLocation();
        
        final TestValidateActions testEventListener = new TestValidateActions();

        testSubject = new InventoryIsAlwaysRight();
        testSubject.setValidateActions(testEventListener);
        testSubject.validate(artifact, storageMetadata);

        final List<String> outputLines = Arrays.asList(new String(output.toByteArray()).split("\n"));
        System.out.println(String.format("Message lines are \n\n%s\n\n", outputLines));

        //assertListContainsMessage(outputLines, "Replacing File StorageLocation[s3:989877] as per policy.");
        Assert.assertTrue("Should have called clearStorageLocation.",
                          !testEventListener.deleteArtifactCalled
                          && !testEventListener.createArtifactCalled
                          && testEventListener.clearStorageLocationCalled
                          && testEventListener.deleteStorageMetadataCalled
                          && !testEventListener.replaceArtifactCalled);
    }

    @Test
    public void resolveNullAndStorageMetadata() throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final StorageMetadata storageMetadata = new StorageMetadata(new StorageLocation(URI.create("s3:989877")), URI.create("test:989877"),
                                                        URI.create("md5:" + random16Bytes()), 1001L, new Date());
        final TestValidateActions testEventListener = new TestValidateActions();

        testSubject = new InventoryIsAlwaysRight();
        testSubject.setValidateActions(testEventListener);
        testSubject.validate(null, storageMetadata);

        final List<String> outputLines = Arrays.asList(new String(output.toByteArray()).split("\n"));
        System.out.println(String.format("Message lines are \n\n%s\n\n", outputLines));

        //assertListContainsMessage(outputLines, "Removing Unknown File StorageLocation[s3:989877] as per policy.");
        Assert.assertTrue("Should have called deleteStorageMetadata.",
                          !testEventListener.deleteArtifactCalled
                          && !testEventListener.createArtifactCalled
                          && !testEventListener.clearStorageLocationCalled
                          && testEventListener.deleteStorageMetadataCalled
                          && !testEventListener.replaceArtifactCalled);
    }
    
    @Test
    public void resolveNullAndStorageMetadataDelayed() throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final TestValidateActions testEventListener = new TestValidateActions();
        
        // simulate new stored object after validation started
        
        testSubject = new InventoryIsAlwaysRight();
        testSubject.setValidateActions(testEventListener);
        Thread.sleep(100L);
        
        final StorageMetadata storageMetadata = new StorageMetadata(new StorageLocation(URI.create("s3:989877")), URI.create("test:989877"),
                                                        URI.create("md5:" + random16Bytes()), 1001L, new Date());

        
        testSubject.validate(null, storageMetadata);

        final List<String> outputLines = Arrays.asList(new String(output.toByteArray()).split("\n"));
        System.out.println(String.format("Message lines are \n\n%s\n\n", outputLines));

        //assertListContainsMessage(outputLines, "Removing Unknown File StorageLocation[s3:989877] as per policy.");
        Assert.assertTrue("Should have delayed/skipped deleteStorageMetadata.",
                          !testEventListener.deleteArtifactCalled
                          && !testEventListener.createArtifactCalled
                          && !testEventListener.clearStorageLocationCalled
                          && !testEventListener.deleteStorageMetadataCalled
                          && !testEventListener.replaceArtifactCalled);
    }

    @Test
    public void resolveNullAndInvalidStorageMetadata() throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();

        // StorageMetadata nas no other metadata than the StorageLocation.
        final StorageMetadata storageMetadata = new StorageMetadata(new StorageLocation(URI.create("s3:989877")));

        final Artifact artifact = new Artifact(URI.create("cadc:bucket/file.fits"),
                                               URI.create("md5:" + random16Bytes()), new Date(),
                                               88L);

        artifact.storageLocation = new StorageLocation(URI.create("s3:989877"));

        final TestValidateActions testEventListener = new TestValidateActions();

        testSubject = new InventoryIsAlwaysRight();
        testSubject.setValidateActions(testEventListener);
        testSubject.validate(artifact, storageMetadata);

        final List<String> outputLines = Arrays.asList(new String(output.toByteArray()).split("\n"));
        System.out.println(String.format("Message lines are \n\n%s\n\n", outputLines));

        //assertListContainsMessage(outputLines,
        //                          "Invalid Storage Metadata (StorageLocation[s3:989877]).  "
        //                          + "Replacing as per policy.");
        Assert.assertTrue("Should only have called clearStorageLocation and deleteStorageMetadata.",
                          !testEventListener.deleteArtifactCalled
                          && !testEventListener.createArtifactCalled
                          && testEventListener.clearStorageLocationCalled
                          && testEventListener.deleteStorageMetadataCalled
                          && !testEventListener.replaceArtifactCalled);
    }

    @Test
    public void resolveArtifactAndNull() throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final Artifact artifact = new Artifact(URI.create("cadc:bucket/file.fits"),
                                               URI.create("md5:" + random16Bytes()), new Date(), 88L);
        artifact.storageLocation = new StorageLocation(URI.create("s3:101010"));

        final TestValidateActions testEventListener = new TestValidateActions();

        testSubject = new InventoryIsAlwaysRight();
        testSubject.setValidateActions(testEventListener);
        testSubject.validate(artifact, null);

        final List<String> outputLines = Arrays.asList(new String(output.toByteArray()).split("\n"));
        System.out.println(String.format("Message lines are \n\n%s\n\n", outputLines));

        //assertListContainsMessage(outputLines, "Resetting Artifact StorageLocation[s3:101010] as per policy.");
        Assert.assertTrue("Should have called clearStorageLocation.",
                          !testEventListener.deleteArtifactCalled
                          && !testEventListener.createArtifactCalled
                          && testEventListener.clearStorageLocationCalled
                          && !testEventListener.deleteStorageMetadataCalled
                          && !testEventListener.replaceArtifactCalled);
    }
}

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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.ratik;

import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.Calendar;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;

public class InventoryValidatorTest {
    private static final Logger log = Logger.getLogger(InventoryValidatorTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.ratik", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.opencadc.inventory.db", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
    }

    private final InventoryEnvironment localEnvironment = new InventoryEnvironment();
    private final LuskanEnvironment remoteEnvironment = new LuskanEnvironment();

    public InventoryValidatorTest() throws Exception {

    }

    @Before
    public void beforeTest() throws Exception {
        this.localEnvironment.cleanTestEnvironment();
        this.remoteEnvironment.cleanTestEnvironment();
    }

    /**
     * discrepancy: none
     * before: Artifact in L & R
     * after: Artifact in L & R
     */
    @Test
    public void noDiscrepancy() throws Exception {
        Calendar calendar = Calendar.getInstance();
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact);
        this.remoteEnvironment.artifactDAO.put(artifact);

        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, artifact.getBucket());
        testSubject.run();

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertTrue("local artifact changed", localArtifact.equals(artifact));
        // check other artifact attributes?
    }

    // discrepancy: artifact in L && artifact not in R //

    /** discrepancy: artifact in L && artifact not in R
     * explanation0: filter policy at L changed to exclude artifact in R
     * evidence: Artifact in R without filter
     * action: delete Artifact, if (L==storage) create DeletedStorageLocationEvent
     *
     * changed filter policy, global doesn't filter, means L == storage only?
     * before: Artifact in L & R, filter policy to exclude Artifact in R
     * after: Artifact not in L, DeletedStorageLocationEvent in L
     */
    @Test
    public void explanation0_inL() throws Exception {
        Calendar calendar = Calendar.getInstance();
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact);
        this.remoteEnvironment.artifactDAO.put(artifact);

        //.necessary to carefully generate a uri bucket, or will an out of range bucket work?
        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, "g");
        testSubject.run();

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNull("local artifact found", localArtifact);

        DeletedStorageLocationEvent dsle = this.localEnvironment.deletedStorageLocationEventDAO.get(artifact.getID());
        Assert.assertNotNull("DeletedStorageLocationEvent not found", dsle);
    }

    /** discrepancy: artifact in L && artifact not in R
     * explanation1: deleted from R, pending/missed DeletedArtifactEvent in L
     * evidence: DeletedArtifactEvent in R
     * action: put DAE, delete artifact
     *
     * before: Artifact in L, not in R, DeletedArtifactEvent in R
     * after: Artifact not in L, DeletedArtifactEvent in L
     */
    @Test
    public void explanation1_inL() throws Exception {
        Calendar calendar = Calendar.getInstance();
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact);

        DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(artifact.getID());
        this.remoteEnvironment.deletedArtifactEventDAO.put(deletedArtifactEvent);

        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, artifact.getBucket());
        testSubject.run();

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNull("local artifact found", localArtifact);

        DeletedStorageLocationEvent dsle = this.localEnvironment.deletedStorageLocationEventDAO.get(artifact.getID());
        Assert.assertNotNull("DeletedStorageLocationEvent not found", dsle);
    }

    /** discrepancy: artifact in L && artifact not in R
     * explanation2: L==global, deleted from R, pending/missed DeletedStorageLocationEvent in L
     * evidence: DeletedStorageLocationEvent in R
     * action: remove siteID from Artifact.storageLocations (see note)
     *
     * note: when removing siteID from Artifact.storageLocations in global, if the Artifact.siteLocations becomes empty
     * the artifact should be deleted (metadata-sync needs to also do this in response to a DeletedStorageLocationEvent)
     * TBD: must this also create a DeletedArtifactEvent?
     *
     * L == global
     * case 1
     * before: Artifact in L with R siteID, plus others, in Artifact.siteLocations, not in R, DeletedStorageLocationEvent in R
     * after: Artifact in L, R siteID not in Artifact.siteLocations
     *
     * case 2
     * before: Artifact in L with R siteID in Artifact.siteLocations, not in R, DeletedStorageLocationEvent in R
     * after: Artifact in not in L
     */
    @Test
    public void explanation2_inL() throws Exception {
        // case 1
        UUID remoteSiteID = UUID.randomUUID();
        UUID otherSiteID = UUID.randomUUID();

        Calendar calendar = Calendar.getInstance();
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        artifact.siteLocations.add(new SiteLocation(remoteSiteID));
        artifact.siteLocations.add(new SiteLocation(otherSiteID));
        this.localEnvironment.artifactDAO.put(artifact);

        DeletedStorageLocationEvent deletedStorageLocationEvent = new DeletedStorageLocationEvent(artifact.getID());
        this.remoteEnvironment.deletedStorageLocationEventDAO.put(deletedStorageLocationEvent);

        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, artifact.getBucket());
        testSubject.run();

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertFalse("artifact contains remote site location", localArtifact.siteLocations.contains(new SiteLocation(remoteSiteID)));
        Assert.assertTrue("artifact missing other site location", localArtifact.siteLocations.contains(new SiteLocation(otherSiteID)));

        // cleanup
        this.localEnvironment.cleanTestEnvironment();
        this.remoteEnvironment.cleanTestEnvironment();

        // case 2
        artifact = new Artifact(URI.create("cadc:INTTEST/two.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        artifact.siteLocations.add(new SiteLocation(remoteSiteID));
        this.localEnvironment.artifactDAO.put(artifact);

        deletedStorageLocationEvent = new DeletedStorageLocationEvent(artifact.getID());
        this.remoteEnvironment.deletedStorageLocationEventDAO.put(deletedStorageLocationEvent);

        testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, artifact.getBucket());
        testSubject.run();

        localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNull("local artifact found", localArtifact);
    }

    /** discrepancy: artifact in L && artifact not in R
     * explanation3: L==global, new Artifact in L, pending/missed Artifact or sync in R
     * evidence: ?
     * action: remove siteID from Artifact.storageLocations (see below)
     *
     * note: when removing siteID from Artifact.storageLocations in global, if the Artifact.siteLocations becomes empty
     * the artifact should be deleted (metadata-sync needs to also do this in response to a DeletedStorageLocationEvent)
     * TBD: must this also create a DeletedArtifactEvent?
     *
     * L == global
     * test1
     * before: Artifact in L with multiple Artifact.siteLocations, not in R
     * after: Artifact in L, siteID not in Artifact.siteLocations
     *
     * test2
     * before: Artifact in L with R siteID in Artifact.siteLocations, not in R
     * after: Artifact in not in L
     */
    @Test
    public void explanation3_inL() throws Exception {
        // case 1
        UUID remoteSiteID = UUID.randomUUID();
        UUID otherSiteID = UUID.randomUUID();

        Calendar calendar = Calendar.getInstance();
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        artifact.siteLocations.add(new SiteLocation(remoteSiteID));
        artifact.siteLocations.add(new SiteLocation(otherSiteID));
        this.localEnvironment.artifactDAO.put(artifact);

        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, artifact.getBucket());
        testSubject.run();

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertFalse("artifact contains remote site location", localArtifact.siteLocations.contains(new SiteLocation(remoteSiteID)));
        Assert.assertTrue("artifact missing other site location", localArtifact.siteLocations.contains(new SiteLocation(otherSiteID)));

        // cleanup
        this.localEnvironment.cleanTestEnvironment();
        this.remoteEnvironment.cleanTestEnvironment();

        // case 2
        artifact = new Artifact(URI.create("cadc:INTTEST/two.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        artifact.siteLocations.add(new SiteLocation(remoteSiteID));
        this.localEnvironment.artifactDAO.put(artifact);

        testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, artifact.getBucket());
        testSubject.run();

        localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNull("local artifact found", localArtifact);
    }

    /** discrepancy: artifact in L && artifact not in R
     * explanation4: L==storage, new Artifact in L, pending/missed new Artifact event in R
     * evidence: ?
     * action: none
     *
     * L == storage
     * before: Artifact in L, not in R
     * after: Artifact in L
     */
    @Test
    public void explanation4_inL() throws Exception {
        Calendar calendar = Calendar.getInstance();
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        this.localEnvironment.artifactDAO.put(artifact);

        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, artifact.getBucket());
        testSubject.run();

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNull("local artifact found", localArtifact);
    }

    /** discrepancy: artifact in L && artifact not in R
     * explanation6: deleted from R, lost DeletedArtifactEvent
     * evidence: ?
     * action: assume explanation3
     *
     * if L == global then explanation3, L == storage then explanation1?
     */
    @Test
    public void explanation6_inL() throws Exception {
        // recreate other tests here?
        // add comments to referenced tests and delete this test?
        // L == global
        // explanation3_inL();
        // L == storage
        // explanation1_inL();
    }

    /**
     * discrepancy: artifact in L && artifact not in R
     * explanation7: L==global, lost DeletedStorageLocationEvent
     * evidence: ?
     * action: assume explanation3
     */
    @Test
    public void explanation7_inL() throws Exception {
          // explanation3_inL();
    }

    // discrepancy: artifact not in L && artifact in R //

    /**
     * discrepancy: artifact not in L && artifact in R
     * explanation0: filter policy at L changed to include artifact in R
     * evidence: ?
     * action: equivalent to missed Artifact event (explanation3 below)
     *
     * before: L == storage only, Artifact in R, not in L
     * after: Artifact in L
     */
    @Test
    public void explanation0_notInL() throws Exception {
        // explanation3_notInL();
    }

    /** discrepancy: artifact not in L && artifact in R
     * explanation1: deleted from L, pending/missed DeletedArtifactEvent in R
     * evidence: DeletedArtifactEvent in L
     * action: none
     *
     *  before: Artifact not in L, in R
     *  after: Artifact not in L
     */
    @Test
    public void explanation1_notInL() throws Exception {
        Calendar calendar = Calendar.getInstance();
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        this.remoteEnvironment.artifactDAO.put(artifact);

        DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(artifact.getID());
        this.localEnvironment.deletedArtifactEventDAO.put(deletedArtifactEvent);

        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, artifact.getBucket());
        testSubject.run();

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNull("local artifact found", localArtifact);
    }

    /** discrepancy: artifact not in L && artifact in R
     * explanation2: L==storage, deleted from L, pending/missed DeletedStorageLocationEvent in R
     * evidence: DeletedStorageLocationEvent in L
     * action: none
     *
     * L == storage
     * before: Artifact not in L, in R, DeletedStorageLocationEvent in L
     * after: Artifact not in L, DeletedStorageLocationEvent in L
     */
    @Test
    public void explanation2_notInL() throws Exception {
        Calendar calendar = Calendar.getInstance();
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        this.remoteEnvironment.artifactDAO.put(artifact);

        DeletedStorageLocationEvent remopteDeletedStorageLocationEvent = new DeletedStorageLocationEvent(artifact.getID());
        this.remoteEnvironment.deletedStorageLocationEventDAO.put(remopteDeletedStorageLocationEvent);

        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, artifact.getBucket());
        testSubject.run();

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNotNull("local artifact not found", localArtifact);

        DeletedStorageLocationEvent localDeletedStorageLocationEvent =
            this.localEnvironment.deletedStorageLocationEventDAO.get(remopteDeletedStorageLocationEvent.getID());
        Assert.assertNotNull("DeletedStorageLocationEvent not found", localDeletedStorageLocationEvent);
    }

    /** discrepancy: artifact not in L && artifact in R
     * explanation3: L==storage, new Artifact in R, pending/missed new Artifact event in L
     * evidence: ?
     * action: insert Artifact
     *
     * L == storage
     * before: Artifact not in L, in R
     * after: Artifact in L
     */
    @Test
    public void explanation3_notInL() throws Exception {
        Calendar calendar = Calendar.getInstance();
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        this.remoteEnvironment.artifactDAO.put(artifact);

        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, artifact.getBucket());
        testSubject.run();

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNotNull("local artifact not found", localArtifact);
    }

    /** discrepancy: artifact not in L && artifact in R
     * explanation4: L==global, new Artifact in R, pending/missed changed Artifact event in L
     * evidence: Artifact in local db but siteLocations does not include remote siteID
     * action: add siteID to Artifact.siteLocations
     *
     * L == global
     * before: Artifact not in L, in R, R siteID not in L Artifact.siteLocations
     * after:  Artifact in L and R siteID in Artifact.siteLocations
     */
    @Test
    public void explanation4_notInL() throws Exception {
        Calendar calendar = Calendar.getInstance();
        Artifact artifact = new Artifact(URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        this.remoteEnvironment.artifactDAO.put(artifact);
        this.localEnvironment.artifactDAO.put(artifact);

        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, artifact.getBucket());
        testSubject.run();

        Set<StorageSite> storageSites = this.remoteEnvironment.storageSiteDAO.list();
        Assert.assertEquals("StorageSite should have one entry", 1, storageSites.size());
        UUID remoteSiteID = storageSites.iterator().next().getID();

        Artifact localArtifact = this.localEnvironment.artifactDAO.get(artifact.getURI());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertTrue("artifact does not contains remote site location", localArtifact.siteLocations.contains(new SiteLocation(remoteSiteID)));
    }

    /** discrepancy: artifact not in L && artifact in R
     * explanation6: deleted from L, lost DeletedArtifactEvent
     * evidence: ?
     * action: assume explanation3
     */
    @Test
    public void explanation6_notInL() throws Exception {
        //explanation3_notInL();
    }

    /** discrepancy: artifact not in L && artifact in R
     * explanation7: L==storage, deleted from L, lost DeletedStorageLocationEvent
     * evidence: ?
     * action: assume explanation3
     */
    @Test
    public void explanation7_notInL() throws Exception {
        //explanation3_notInL();
    }

    /**
     * discrepancy: artifact.uri in both && artifact.id mismatch
     *
     * explanation1: same ID collision due to race condition that metadata-sync has to handle
     * evidence: no more evidence needed
     * action: pick winner, create DeletedArtifactEvent for loser, delete loser if it is in L
     *
     * loser always in L?
     * before: Artifact.uri in L && R, L Artifact.id != R Artifact.id
     * after:  Artifact not in L, DeletedArtifactEvent in L
     */
    @Test
    public void artifactUriCollision() throws Exception {
        Calendar calendar = Calendar.getInstance();

        UUID localID = UUID.randomUUID();
        Artifact localArtifact = new Artifact(localID, URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        this.localEnvironment.artifactDAO.put(localArtifact);

        UUID remoteID = UUID.randomUUID();
        Artifact remoteArtifact = new Artifact(localID, URI.create("cadc:INTTEST/one.ext"), TestUtil.getRandomMD5(), calendar.getTime(), 1024L);
        this.remoteEnvironment.artifactDAO.put(remoteArtifact);

        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, remoteArtifact.getBucket());
        testSubject.run();

        localArtifact = this.localEnvironment.artifactDAO.get(localArtifact.getURI());
        Assert.assertNull("local artifact found", localArtifact);

        DeletedArtifactEvent deletedArtifactEvent = this.localEnvironment.deletedArtifactEventDAO.get(localID);
        Assert.assertNotNull("DeletedArtifactEvent not found", deletedArtifactEvent);
    }

    /**
     * discrepancy: artifact in both && valid metaChecksum mismatch
     *
     * explanation1: pending/missed artifact update in L
     * evidence: ??
     * action: put Artifact
     *
     * explanation2: pending/missed artifact update in R
     * evidence: ??
     * action: do nothing
     *
     * before: Artifact in L & R, L metaChecksum != R metaChecksum
     * after: Artifact in L with R metaChecksum
     */
    @Test
    public void artifactChecksumMismatch() throws Exception {
        Calendar calendar = Calendar.getInstance();

        UUID artifactID = UUID.randomUUID();
        URI artifactURI = URI.create("cadc:INTTEST/one.ext");
        URI localChecksum = TestUtil.getRandomMD5();
        Artifact localArtifact = new Artifact(artifactID, artifactURI, localChecksum, calendar.getTime(), 1024L);
        this.localEnvironment.artifactDAO.put(localArtifact);

        URI remoteChecksum = TestUtil.getRandomMD5();
        Artifact remoteArtifact = new Artifact(artifactID, artifactURI, remoteChecksum, calendar.getTime(), 1024L);
        this.remoteEnvironment.artifactDAO.put(remoteArtifact);

        InventoryValidator testSubject = new InventoryValidator(this.localEnvironment.daoConfig, TestUtil.LUSKAN_URI, remoteArtifact.getBucket());
        testSubject.run();

        localArtifact = this.localEnvironment.artifactDAO.get(localArtifact.getURI());
        Assert.assertNotNull("local artifact not found", localArtifact);
        Assert.assertSame("local artifact checksum does not match", localArtifact.getContentChecksum(), remoteChecksum);
    }

}

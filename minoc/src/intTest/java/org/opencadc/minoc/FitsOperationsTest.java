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
*
************************************************************************
*/

package org.opencadc.minoc;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import nom.tam.fits.Fits;
import nom.tam.util.RandomAccessDataObject;
import nom.tam.util.RandomAccessFileExt;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

/**
 * Integration test to pull existing test FITS files from VOSpace (Vault) into a local directory, then PUT them into
 * the local test Minoc and run cutouts from the service.  This test depends on:
 * <ul>
 *   <li>A local Minoc</li>
 *   <li>A local inventory Postgres database for Minoc</li>
 *   <li>A Registry to point to this local Minoc that is accessible from this test</li>
 *   <li>A proxy for this Minoc</li>
 * </ul>
 */
public class FitsOperationsTest extends MinocTest {
    private final static Logger LOGGER = Logger.getLogger(FitsOperationsTest.class);
    private final static URI VOSPACE_URI = URI.create("ivo://cadc.nrc.ca/vault");
    private final static Path DEFAULT_DATA_PATH = new File(System.getProperty("user.home")
                                                           + "/.config/test-data").toPath();

    protected URL filesVaultURL;

    static {
        Log4jInit.setLevel("org.opencadc.minoc", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.net", Level.INFO);
    }

    public FitsOperationsTest() throws Exception {
        super();

        final RegistryClient regClient = new RegistryClient();
        filesVaultURL = new URL(regClient.getServiceURL(VOSPACE_URI, Standards.VOSPACE_FILES_20, AuthMethod.ANON)
                                + "/CADC/test-data/cutouts");
        DEFAULT_DATA_PATH.toFile().mkdirs();
    }

    @Test
    public void testCompressed() throws Exception {
        final String testFilePrefix = "test-compressed";
        final String testFileExtension = "fz";
        final URI artifactURI = URI.create("cadc:TEST/" + testFilePrefix + "." + testFileExtension);
        final String[] cutoutSpecs = new String[] {
                "[0][*,100:400]"
        };

        uploadAndCompare(artifactURI, cutoutSpecs, testFilePrefix, testFileExtension);
    }

    @Test
    public void testSimple() throws Exception {
        final String testFilePrefix = "test-simple";
        final String testFileExtension = "fits";
        final URI artifactURI = URI.create("cadc:TEST/" + testFilePrefix + "." + testFileExtension);
        final String[] cutoutSpecs = new String[] {
                "[0][200:350,100:300]"
        };

        uploadAndCompare(artifactURI, cutoutSpecs, testFilePrefix, testFileExtension);
    }

    @Test
    public void testMEF() throws Exception {
        final String testFilePrefix = "test-mef";
        final String testFileExtension = "fits";
        final URI artifactURI = URI.create("cadc:TEST/" + testFilePrefix + "." + testFileExtension);
        final String[] cutoutSpecs = new String[]{
                "[116][100:250,*]",
                "[SCI,14][100:125,100:175]",
                "[91][*,90:255]"
        };

        uploadAndCompare(artifactURI, cutoutSpecs, testFilePrefix, testFileExtension);
    }

    @Test
    public void testFITSCompliance() throws Exception {
        final String testFilePrefix = "test-fits-compliance";
        final String testFileExtension = "fits";
        final URI artifactURI = URI.create("cadc:TEST/" + testFilePrefix + "." + testFileExtension);
        final String[] cutoutSpecs = new String[] {
                "[3][*,1:100]",
                "[4][50:90,*]"
        };

        uploadAndCompare(artifactURI, cutoutSpecs, testFilePrefix, testFileExtension);
    }

    private void uploadAndCompare(final URI artifactURI, final String[] cutoutSpecs, final String testFilePrefix,
                                  final String testFileExtension) throws Exception {
        ensureFile(artifactURI);
        final StringBuilder queryStringBuilder = new StringBuilder("?");
        Arrays.stream(cutoutSpecs).
                forEach(cut -> queryStringBuilder.append("SUB=").append(NetUtil.encode(cut)).append("&"));

        queryStringBuilder.deleteCharAt(queryStringBuilder.lastIndexOf("&"));

        final URL artifactSUBURL = new URL(filesURL + "/" + artifactURI.toString() + queryStringBuilder.toString());
        final File outputFile = Files.createTempFile(testFilePrefix + "-", "." + testFileExtension).toFile();
        LOGGER.debug("Writing cutout to " + outputFile);

        // Perform the cutout.
        Subject.doAs(userSubject, (PrivilegedExceptionAction<Boolean>) () -> {
            LOGGER.debug("Testing cutout with " + artifactSUBURL);
            try (final FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
                final HttpGet cutoutClient = new HttpGet(artifactSUBURL, fileOutputStream);
                cutoutClient.setFollowRedirects(true);
                cutoutClient.run();
                fileOutputStream.flush();

                Assert.assertEquals("Wrong content type.",
                                    cutoutClient.getResponseHeader(HttpTransfer.CONTENT_TYPE), "application/fits");
                Assert.assertNotNull("Should include Content-Disposition ("
                                     + cutoutClient.getResponseHeader("Content-Disposition") + ")",
                                     cutoutClient.getResponseHeader("Content-Disposition"));

                Assert.assertEquals("Should NOT contain " + HttpTransfer.CONTENT_LENGTH, -1L,
                                  cutoutClient.getContentLength());
                Assert.assertFalse("Should NOT contain " + HttpTransfer.CONTENT_MD5,
                                   StringUtil.hasText(cutoutClient.getContentMD5()));
                Assert.assertFalse("Should NOT contain " + HttpTransfer.CONTENT_ENCODING,
                                   StringUtil.hasText(cutoutClient.getContentEncoding()));
            }

            LOGGER.debug("Cutout complete -> " + artifactURI);
            return Boolean.TRUE;
        });

        // setup
        final File expectedFile = new File(DEFAULT_DATA_PATH.toFile(), testFilePrefix + "-cutout.fits");
        ensureLocalFile(expectedFile);

        final RandomAccessDataObject expectedCutout = new RandomAccessFileExt(expectedFile, "r");
        final Fits expectedFits = new Fits(expectedCutout);

        final RandomAccessDataObject resultCutout = new RandomAccessFileExt(outputFile, "r");
        final Fits resultFits = new Fits(resultCutout);

        FitsTest.assertFitsEqual(expectedFits, resultFits);
    }

    /**
     * Perform an upload to the local Minoc, then perform the cutout against it in the service.
     * @param artifactURI   The URI to ensure.
     * @throws Exception    Any errors.
     */
    private void ensureFile(final URI artifactURI) throws Exception {
        LOGGER.info("ensureLocalFile(" + artifactURI + ")");
        final URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());

        Subject.doAs(userSubject, (PrivilegedExceptionAction<Boolean>) () -> {
            // cleanup
            final HttpDelete del = new HttpDelete(artifactURL, false);
            del.run();
            final Throwable throwable = del.getThrowable();
            if (throwable != null && !(throwable instanceof ResourceNotFoundException)) {
                Assert.fail(throwable.getMessage());
            }

            return Boolean.TRUE;
        });

        // verify
        final HttpGet head = new HttpGet(artifactURL, false);
        head.setHeadOnly(true);
        try {
            head.prepare();
            Assert.fail("cleanup failed -- file exists: " + artifactURI);
        } catch (ResourceNotFoundException ok) {
            LOGGER.info("verify not found: " + artifactURL);
        }

        final String schemePath = artifactURI.getSchemeSpecificPart();
        final String fileName = schemePath.substring(schemePath.lastIndexOf("/") + 1);
        final File localFile = new File(DEFAULT_DATA_PATH.toFile(), fileName);

        ensureLocalFile(localFile);

        try (final FileInputStream fileInputStream = new FileInputStream(localFile)) {
            Subject.doAs(userSubject, (PrivilegedExceptionAction<Boolean>) () -> {
                final HttpUpload upload = new HttpUpload(fileInputStream, artifactURL);
                upload.setRequestProperty("X-Test-Method", fileName);
                upload.setRequestProperty(HttpTransfer.CONTENT_LENGTH, Long.toString(localFile.length()));
                upload.run();
                LOGGER.info("response code: " + upload.getResponseCode() + " " + upload.getThrowable());
                Assert.assertNull("Upload contains error.", upload.getThrowable());

                return Boolean.TRUE;
            });
        }
        LOGGER.info("cleanFilePut(" + artifactURI + "): OK");
    }

    private void ensureLocalFile(final File localFile) throws Exception {
        if (!localFile.exists() || (localFile.length() == 0)) {
            localFile.delete();
            final URL downloadURL = new URL(filesVaultURL + "/" + localFile.getName());
            LOGGER.info("File " + localFile + " does not exist.  Downloading from " + downloadURL);
            try (final FileOutputStream fileOutputStream = new FileOutputStream(localFile)) {
                final HttpGet download = new HttpGet(downloadURL, fileOutputStream);
                download.setFollowRedirects(true);
                download.run();

                fileOutputStream.flush();
                Assert.assertNull("Should be no error.", download.getThrowable());
            }
        }
    }
}

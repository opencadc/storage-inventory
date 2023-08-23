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

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.FileUtil;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;


public class MetaTest extends MinocTest {

    private static final Logger LOGGER = Logger.getLogger(MetaTest.class);

    /**
     * Test the META keyword.
     * @throws Exception Any unknown errors.
     */
    @Test
    public void testMEFHeaders() throws Exception {
        try {
            final URI artifactURI = URI.create("cadc:TEST/testMEFHeaders");
            final URL artifactURL = new URL(filesURL + "/" + artifactURI);
            final File sampleFITSFile = FileUtil.getFileFromResource("sample-mef.fits", MetaTest.class);
            final File sampleHeaderFile = FileUtil.getFileFromResource("sample-mef.txt", MetaTest.class);
            final long byteCount = sampleFITSFile.length();
            final byte[] buffer = new byte[(int) byteCount];

            Subject.doAs(userSubject, (PrivilegedExceptionAction<Object>) () -> {
                try (final FileInputStream fileInputStream = new FileInputStream(sampleFITSFile)) {
                    Assert.assertEquals("Wrong length.", byteCount, fileInputStream.read(buffer));
                }

                try (final InputStream inputStream = new ByteArrayInputStream(buffer)) {
                    final HttpUpload put = new HttpUpload(inputStream, artifactURL);
                    put.setDigest(computeChecksumURI(buffer));
                    put.setRequestProperty(HttpTransfer.CONTENT_TYPE, "application/fits");
                    put.run();
                    Assert.assertNull(put.getThrowable());
                }

                final List<String> expectedHeaderLines = new ArrayList<>();
                try (final FileInputStream fileInputStream = new FileInputStream(sampleHeaderFile);
                     final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream))) {
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        expectedHeaderLines.add(line);
                    }
                }

                final URL artifactHeadersURL = new URL(artifactURL.toExternalForm() + "?META=true");
                final HttpGet headersGet = new HttpGet(artifactHeadersURL, true);
                headersGet.run();

                final List<String> resultHeaderLines = new ArrayList<>();
                final BufferedReader bufferedReader =
                        new BufferedReader(new InputStreamReader(headersGet.getInputStream()));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    resultHeaderLines.add(line);
                }

                for (int i = 0; i < expectedHeaderLines.size(); i++) {
                    final String expectedHeaderLine = expectedHeaderLines.get(i);
                    final String resultHeaderLine = resultHeaderLines.get(i);

                    if (expectedHeaderLine == null) {
                        Assert.assertNull("Should be null at " + i, resultHeaderLine);
                    } else {
                        Assert.assertEquals("Header Line at " + i + " is incorrect.",
                                            expectedHeaderLine.trim(), resultHeaderLine.trim());
                    }
                }

                return null;
            });
        } catch (Exception t) {
            LOGGER.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
            throw t;
        }
    }
    
    @Test
    public void testNotFITS() throws Exception {
        try {
            URI artifactURI = URI.create("cadc:TEST/testNotFITS");
            URL artifactURL = new URL(filesURL + "/" + artifactURI.toString());

            String content = "abcdefghijklmnopqrstuvwxyz";
            byte[] data = content.getBytes();

            // put: no length or checksum
            InputStream in = new ByteArrayInputStream(data);
            HttpUpload put = new HttpUpload(in, artifactURL);
            put.setDigest(computeChecksumURI(data));

            Subject.doAs(userSubject, new RunnableAction(put));
            Assert.assertNull(put.getThrowable());
            

            URL artifactHeadersURL = new URL(artifactURL.toExternalForm() + "?META=true");
            HttpGet headersGet = new HttpGet(artifactHeadersURL, true);
            headersGet.run();

            LOGGER.info("response: " + headersGet.getResponseCode() + " " + headersGet.getThrowable());
            Assert.assertEquals(400, headersGet.getResponseCode());

        } catch (Exception t) {
            LOGGER.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
            throw t;
        }
    }

    @Test
    public void testCheckHeaderNotFound() throws Exception {
        try {
            Subject.doAs(userSubject, (PrivilegedExceptionAction<Object>) () -> {
                final URI artifactURI = URI.create("cadc:TEST/testMEFHeadersNotFound");
                final URL artifactURL = new URL(filesURL + "/" + artifactURI + "?META=true");

                // delete
                final HttpGet headersGet = new HttpGet(artifactURL, true);
                headersGet.run();
                Assert.assertNotNull(headersGet.getThrowable());
                Assert.assertEquals("should be 404, not found", 404, headersGet.getResponseCode());
                System.out.println(headersGet.getThrowable());
                Assert.assertTrue(headersGet.getThrowable() instanceof ResourceNotFoundException);

                return null;
            });

        } catch (Exception t) {
            LOGGER.error("unexpected throwable", t);
            Assert.fail("unexpected throwable: " + t);
            throw t;
        }
    }
}

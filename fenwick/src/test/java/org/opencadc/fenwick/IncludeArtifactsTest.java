
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
 *
 ************************************************************************
 */

package org.opencadc.fenwick;

import org.apache.log4j.Level;
import org.junit.Test;
import org.junit.Assert;

import ca.nrc.cadc.util.Log4jInit;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;


public class IncludeArtifactsTest {

    static {
        Log4jInit.setLevel("org.opencadc.fenwick", Level.DEBUG);
    }

    @Test
    public void iterator() throws Exception {
        final String currUserHome = System.getProperty("user.home");
        try {
            final Path tmpConfDirPath = Files.createTempDirectory(IncludeArtifactsTest.class.getName());
            // Create the expected layout.
            final File includeDir = new File(tmpConfDirPath.toFile(), "config/include");
            includeDir.mkdirs();
            final Path includePath = includeDir.toPath();

            System.setProperty("user.home", tmpConfDirPath.toFile().toString());

            final String clauseOne = "WHERE a = b AND c < 8";
            Files.write(Files.createTempFile(includePath, "clauseone_", ".sql"), clauseOne.getBytes());

            final String clauseTwo =
                    "--Authored by jenkisnd\n--2020.04.28\nWHERE column between (45, 60) AND column_type = " +
                    "'PROJECT'\nAND year = 2020";
            Files.write(Files.createTempFile(includePath, "clausetwo_", ".sql"), clauseTwo.getBytes());

            final String clauseThree = "WHERE lastModified >= 1977-11-25";
            Files.write(Files.createTempFile(includePath, "clausethree_", ".sql"), clauseThree.getBytes());

            // TXT files are excluded.
            final String clauseFour = "WHERE name != 'BADARCHIVE'";
            Files.write(Files.createTempFile(includePath, "clausefour_", ".txt"), clauseFour.getBytes());

            final String clauseFive = "      where thiscol = 'thisvalue'";
            Files.write(Files.createTempFile(includePath, "clausefive_", ".sql"), clauseFive.getBytes());

            // Should be excluded
            final String clauseSix = "      where ";
            Files.write(Files.createTempFile(includePath, "clausesix_", ".sql"), clauseSix.getBytes());

            final String clauseSeven = "      \nwhere\n number = 88\n  and speed = 'racer' --Only speed racer";
            Files.write(Files.createTempFile(includePath, "clauseseven_", ".sql"), clauseSeven.getBytes());

            final IncludeArtifacts includeArtifacts = new IncludeArtifacts();
            final Iterator<String> clauses = includeArtifacts.iterator();
            final List<String> clauseList = new ArrayList<>();
            clauses.forEachRemaining(clauseList::add);

            final List<String> expectedClauses =
                    Arrays.asList("column between (45, 60) AND column_type = 'PROJECT' AND year = 2020",
                                  "a = b AND c < 8", "thiscol = 'thisvalue'", "number = 88 and speed = 'racer'",
                                  "lastModified >= 1977-11-25");

            // Sort to allow comparisons.
            expectedClauses.sort(Comparator.naturalOrder());
            clauseList.sort(Comparator.naturalOrder());

            Assert.assertArrayEquals("Wrong clauses", expectedClauses.toArray(), clauseList.toArray());
        } finally {
            System.setProperty("user.home", currUserHome);
        }
    }

    @Test
    public void iteratorMissingWhereException() throws Exception {
        final String currUserHome = System.getProperty("user.home");
        try {
            final Path tmpConfDirPath = Files.createTempDirectory(IncludeArtifactsTest.class.getName());
            // Create the expected layout.
            final File includeDir = new File(tmpConfDirPath.toFile(), "config/include");
            includeDir.mkdirs();
            final Path includePath = includeDir.toPath();

            System.setProperty("user.home", tmpConfDirPath.toFile().toString());

            final String clause = "OR B < 9";
            final Path tmpFilePath =
                    Files.write(Files.createTempFile(includePath, "clause_", ".sql"), clause.getBytes());

            final IncludeArtifacts includeArtifacts = new IncludeArtifacts();
            try {
                includeArtifacts.iterator();
                Assert.fail("Should throw IllegalStateException for missing where.");
            } catch (IllegalStateException e) {
                Assert.assertEquals("Wrong message.",
                                    String.format("The first clause found in %s (line 1) MUST be start with the "
                                                  + "WHERE keyword.", tmpFilePath.toFile().getName()),
                                    e.getMessage());
                // Good.
            }
        } finally {
            System.setProperty("user.home", currUserHome);
        }
    }

    @Test
    public void iteratorMoreThanOneWhereException() throws Exception {
        final String currUserHome = System.getProperty("user.home");
        try {
            final Path tmpConfDirPath = Files.createTempDirectory(IncludeArtifactsTest.class.getName());
            // Create the expected layout.
            final File includeDir = new File(tmpConfDirPath.toFile(), "config/include");
            includeDir.mkdirs();
            final Path includePath = includeDir.toPath();

            System.setProperty("user.home", tmpConfDirPath.toFile().toString());

            final String clause = "WHERE B < 9 -- first where\nwhere z == 9 -- should fail here";
            Files.write(Files.createTempFile(includePath, "clause_", ".sql"), clause.getBytes());

            final IncludeArtifacts includeArtifacts = new IncludeArtifacts();
            try {
                includeArtifacts.iterator();
                Assert.fail("Should throw IllegalStateException for too many wheres.");
            } catch (IllegalStateException e) {
                Assert.assertEquals("Wrong message.", "A valid WHERE clause is already present (line 2).",
                                    e.getMessage());
                // Good.
            }
        } finally {
            System.setProperty("user.home", currUserHome);
        }
    }
}

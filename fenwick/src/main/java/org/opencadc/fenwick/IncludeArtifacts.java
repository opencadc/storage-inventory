
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

import ca.nrc.cadc.util.StringUtil;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.log4j.Logger;


public class IncludeArtifacts implements ArtifactSelector {
    private static final Logger log = Logger.getLogger(IncludeArtifacts.class);
    static final String COMMENT_PREFIX = "--";

    private final Path selectorConfigDir;

    /**
     * Empty constructor.
     */
    public IncludeArtifacts() {
        // Read in the configuration file, and create a usable Map from it.
        final File homeConfigDirectory = new File(String.format("%s/config/include", System.getProperty("user.home")));

        // Allow some flexibility to override.  This is useful for local testing as one would normally require root
        // access to create /config.  Those Selectors that do not use the include clauses may safely ignore this.
        selectorConfigDir = homeConfigDirectory.isDirectory()
                            ? homeConfigDirectory.toPath()
                            : new File("/config/include").toPath();
    }

    /**
     * Obtain the iterator of clauses used to build a query to filter the Artifacts being merged.  This method will read
     * in the files each time, so use it sparingly.
     *
     * @return Iterator of String clauses, or empty list.  Never null.
     *
     * @throws Exception Any issues with establishing the list, or reading from configuration
     */
    @Override
    public Iterator<String> iterator() throws Exception {
        final List<String> whereClauses = new ArrayList<>();
        loadClauses(whereClauses);
        if (whereClauses.isEmpty()) {
            throw new IllegalStateException(
                    String.format("No usable SQL filter files located in %s.  Ensure there is at least one file "
                                  + "with the .sql extension whose content begins with the keyword 'WHERE'.",
                                  selectorConfigDir));
        }
        return whereClauses.iterator();
    }

    /**
     * Iterate the files in the filters folder and read in the appropriately named files.  This method will verify that
     * the file names end with an SQL extension, and the clause declared within begins with WHERE.  That clause is
     * then loaded into the given list with the WHERE keyword stripped.
     * <p/>
     * This method is how the SQL files are read in and loaded as clauses.  Any lines that are not comments are
     * considered part of the clause.  Comments begin with <code>--</code> and can be within the line, but anything
     * after the <code>--</code> will be ignored.
     *
     * @param whereClauses A list of clauses to load into.
     * @throws IOException If anything goes awry while reading the files.
     */
    void loadClauses(final List<String> whereClauses) throws IOException {
        final File configurationDirFile = selectorConfigDir.toFile();
        if (configurationDirFile.isDirectory() && configurationDirFile.canRead()) {
            final File[] fileListing = Objects.requireNonNull(
                    configurationDirFile.listFiles(pathname -> pathname.getName().toLowerCase().endsWith(".sql")));
            for (final File f : fileListing) {
                boolean validWhereClauseFound = false;
                final StringBuilder clauseBuilder = new StringBuilder();

                try (final LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(f))) {
                    String line;
                    while ((line = lineNumberReader.readLine()) != null) {
                        line = line.trim();

                        if (line.contains(COMMENT_PREFIX)) {
                            line = line.substring(0, line.indexOf(COMMENT_PREFIX)).trim();
                        }

                        // Skip empty lines
                        if (StringUtil.hasText(line)) {
                            // SQL comment syntax
                            if (line.regionMatches(true, 0, "WHERE", 0, "WHERE".length())) {
                                if (validWhereClauseFound) {
                                    throw new IllegalStateException(
                                            String.format("A valid WHERE clause is already present (line %d).",
                                                          lineNumberReader.getLineNumber()));
                                }

                                validWhereClauseFound = true;
                                line = line.replaceFirst("(?i)\\bwhere\\b", "").trim();

                                // It is acceptable to have the WHERE keyword on its own line, too.
                                if (StringUtil.hasText(line)) {
                                    clauseBuilder.append(line);
                                }
                            } else {
                                // This is assumed to be another part of the clause, so ensure we've already passed the
                                // WHERE portion.
                                if (validWhereClauseFound) {
                                    clauseBuilder.append(" ").append(line);
                                } else {
                                    throw new IllegalStateException(
                                            String.format("The first clause found in %s (line %d) MUST be start with "
                                                          + "the WHERE keyword.", f.getName(),
                                                          lineNumberReader.getLineNumber()));
                                }
                            }
                        }
                    }
                }

                if ((clauseBuilder.length() > 0) && StringUtil.hasText(clauseBuilder.toString().trim())) {
                    whereClauses.add(clauseBuilder.toString().trim());
                }
            }
        } else {
            throw new IOException(String.format("Directory %s is not found or not readable.", selectorConfigDir));
        }
    }
}


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

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.StringUtil;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.file.Path;

import org.apache.log4j.Logger;

/**
 * Implementation of ArtifactSelector that includes artifacts via selective queries.
 * This class requires one or more fragments of SQL (a WHERE clause), each in a separate
 * file located in {user.home}/config/include and named {something}.sql -- see the
 * iterator() method for details.
 *
 * @author pdowler
 */
public class IncludeArtifacts implements ArtifactSelector {
    private static final Logger log = Logger.getLogger(IncludeArtifacts.class);
    static final String COMMENT_PREFIX = "--";
    static final String SQL_FILE_NAME = "artifact-filter.sql";

    private final Path selectorConfigDir;

    /**
     * Empty constructor.
     */
    public IncludeArtifacts() {
        // Read in the configuration file, and create a usable Map from it.
        final File homeConfigDirectory = new File(System.getProperty("user.home") + "/config");
        selectorConfigDir = homeConfigDirectory.toPath();
    }

    /**
     * Obtain the iterator of clauses used to build a query to include the Artifacts being merged.
     *
     * <p>This method will load the expected file, and verify that the clause declared within begins with WHERE.  That
     * clause is then parsed and returned with the WHERE keyword stripped.
     *
     * <p>This method is how the SQL file is read in and loaded as a clause.  Any lines that are not comments are
     * considered part of the clause.  Comments begin with <code>--</code> and can be within the line, but anything
     * after the <code>--</code> will be ignored.
     *
     * <p>This class will read in the SQL (*.sql) file located in the configuration directory and parse the SQL
     * fragments.  Each fragment MUST begin with the WHERE keyword and be followed by one or more SQL conditions:
     *
     * <p>artifact-filter.sql
     * <code>
     * -- Comments are OK and ignored!
     * WHERE lastModified &gt; 2020-03-03 AND lastModified &lt; 2020-03-30
     * </code>
     *
     * <p>The *.sql file is parsed and ANDed together to form the WHERE clause for the query to select desired
     * Artifacts:
     * <code>
     * WHERE (lastModified &gt; 2020-03-03 AND lastModified &lt; 2020-03-30)
     * AND (uri like 'ad:TEST%' OR uri like 'ad:CADC%')
     * </code>
     *
     * @return String condition.
     * @throws ResourceNotFoundException For any missing required configuration.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     */
    @Override
    public String getConstraint() throws ResourceNotFoundException, IOException, IllegalStateException {
        final File configurationDirFile = selectorConfigDir.toFile();
        if (configurationDirFile.isDirectory() && configurationDirFile.canRead()) {
            final File[] fileListing = configurationDirFile.listFiles((dir, name) -> name.equals(SQL_FILE_NAME));
            if (fileListing == null || fileListing.length != 1) {
                throw new IllegalStateException("There should exist a single file called " + SQL_FILE_NAME + " in the "
                        + selectorConfigDir + " folder.");
            } else {
                final File f = fileListing[0];
                boolean validWhereClauseFound = false;
                final StringBuilder clauseBuilder = new StringBuilder();

                try (final LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(f))) {
                    String line;
                    while ((line = lineNumberReader.readLine()) != null) {
                        line = line.trim();
                        log.debug("Next line is " + line);

                        if (line.contains(COMMENT_PREFIX)) {
                            line = line.substring(0, line.indexOf(COMMENT_PREFIX)).trim();
                        }

                        // Skip empty lines
                        if (StringUtil.hasText(line)) {
                            // SQL comment syntax
                            if (line.regionMatches(true, 0, "WHERE", 0, "WHERE".length())) {
                                if (validWhereClauseFound) {
                                    throw new IllegalStateException(
                                            "A valid WHERE clause is already present (line "
                                                    + lineNumberReader.getLineNumber() + ").");
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
                                    throw new IllegalStateException("The first clause found in " + f.getName()
                                            + " (line " + lineNumberReader.getLineNumber()
                                            + ") MUST be start with the WHERE keyword.");
                                }
                            }
                        }
                    }
                }

                if ((clauseBuilder.length() > 0) && StringUtil.hasText(clauseBuilder.toString().trim())) {
                    return clauseBuilder.toString();
                } else {
                    throw new IllegalStateException("No usable SQL in " + selectorConfigDir + "/" + SQL_FILE_NAME);
                }
            }
        } else {
            throw new IOException("Directory " + selectorConfigDir + " is not found or not readable.");
        }
    }
}

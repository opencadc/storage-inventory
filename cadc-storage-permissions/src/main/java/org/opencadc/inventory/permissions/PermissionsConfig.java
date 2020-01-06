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
 *  $Revision: 5 $
 *
 ************************************************************************
 */

package org.opencadc.inventory.permissions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;

public class PermissionsConfig {
    private static final Logger log = Logger.getLogger(PermissionsConfig.class);

    private List<String> config;

    public PermissionsConfig(File propertiesFile)
        throws IOException {
        this.config = loadConfig(propertiesFile);
    }

    /**
     * Get the Permissions for the given Artifact URI. If permissions are not
     * found for the given Artifact URI, returns an empty Permissions object
     * with no permissions.
     *
     * @param artifactURI The Artifact URI.
     * @return A Permissions object.
     */
    public Permissions getPermissions(URI artifactURI) {
        if (artifactURI == null) {
            throw new IllegalArgumentException("artifactURI is null");
        }
        Permissions permissions = null;

        for (String line : getConfig()) {
            String[] keyValues = line.split("=");
            if (keyValues.length == 2) {
                String key = keyValues[0].trim();
                String value = keyValues[1].trim();
                Pattern pattern = getPattern(key);
                if (pattern.matcher(artifactURI.toASCIIString()).matches()) {
                    permissions = parsePermissions(value);
                    break;
                }
            } else {
                log.error("invalid config entry, expected key = value: " + line);
            }
        }
        if (permissions == null) {
            permissions = new Permissions(false, new ArrayList<GroupURI>(), new ArrayList<GroupURI>());
        }
        return permissions;
    }

    /**
     * Reads the properties file and adds each line in the properties file as a String in a List.
     *
     * @param propertiesFile The properties file.
     * @return A List of strings containing the properties.
     * @throws IOException for errors reading the properties file.
     */
    List<String> loadConfig(File propertiesFile)
        throws IOException {
        if (!propertiesFile.exists()) {
            throw new FileNotFoundException("File not found or cannot read: " + propertiesFile.getAbsolutePath());
        }
        Stream<String> lines = Files.lines(Paths.get(propertiesFile.getAbsolutePath()));
        return lines.filter(s -> !s.startsWith("#") && !s.isEmpty()).collect(Collectors.toList());
    }

    /**
     * For the given value create a Pattern for matching against the permissions file.
     *
     * @param value
     * @return regex pattern.
     */
    Pattern getPattern(String value) {
        // ad:FOO/* -> ^ad:FOO\\/.*
        StringBuilder sb = new StringBuilder();
        sb.append("^");
        sb.append(value.replace("/", "\\/").replace("*", ".*"));
        return Pattern.compile(sb.toString());
    }

    /**
     * Parse the permissions string into a Permissions object. If the permissions string
     * cannot be parsed, return an empty Permissions object with no permissions.
     *
     * @param value The string to parse.
     * @return A Permissions object.
     */
    Permissions parsePermissions(String value) {

        StringBuilder message = new StringBuilder();
        boolean isAnonymous = false;
        List<GroupURI> readOnlyGroups = new ArrayList<GroupURI>();
        List<GroupURI> readWriteGroups = new ArrayList<GroupURI>();

        // Split the line into key value pair
        String[] values = value.trim().split("\\s+");
        if (values.length == 3) {
            if (values[0].trim().matches("^(T|F)$")) {
                isAnonymous = values[0].equals("T");
            } else {
                message.append("invalid anonymous flag ");
                message.append(values[0]);
                message.append(", expected T or n");
            }
            loadGroupURIS(readOnlyGroups, message, values[1].trim(), "read-only");
            loadGroupURIS(readWriteGroups, message, values[2].trim(), "read-write");
        } else {
            message.append("expected 3 values, found ");
            message.append(values.length);
            message.append(": ");
            message.append(value);
            message.append("\n");
        }

        if (message.length() > 0) {
            isAnonymous = false;
            readOnlyGroups.clear();
            readWriteGroups.clear();
            log.error(message.toString());
        }
        return new Permissions(isAnonymous, readOnlyGroups, readWriteGroups);
    }

    /**
     * Parses a string into GroupURI's and adds them to a list.
     *
     * @param groupURIS List of GroupURI's.
     * @param message   StringBuilder to store errors encountered.
     * @param value The string to parse for GroupURI's.
     * @param groupType The type of group permissions.
     */
    void loadGroupURIS(List<GroupURI> groupURIS, StringBuilder message, String value,  String groupType) {
        String[] groups = value.split(",");
        for (String groupUri : groups) {
            try {
                groupURIS.add(new GroupURI(groupUri));
            } catch (IllegalArgumentException e) {
                message.append("invalid ");
                message.append(groupType);
                message.append(" group URI: ");
                message.append(groupUri);
                message.append("\n");
            }
        }
    }

    /**
     * Get the List of permissions. Useful for unit testing.
     * @return the permissions as a List of String.
     */
    List<String> getConfig() {
        return this.config;
    }

}

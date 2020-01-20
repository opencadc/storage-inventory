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

package org.opencadc.baldur;

import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.permissions.Grant;
import org.opencadc.inventory.permissions.ReadGrant;
import org.opencadc.inventory.permissions.WriteGrant;

/**
 * Class that provides access to the permissions described
 * in the permissions property file.
 * 
 * @author majorb
 *
 */
public class PermissionsConfig {
    
    private static final Logger log = Logger.getLogger(PermissionsConfig.class);
    private static final String PERMISSIONS_CONFIG_PROPERTIES = "baldur-permissions-config.properties";

    private static final String KEY_ENTRY = "entry";
    private static final String KEY_ANON_READ = ".anon";
    private static final String KEY_READONLY_GROUPS = ".readOnlyGroups";
    private static final String KEY_READWRITE_GROUPS = ".readWriteGroups";
    
    private static List<PermissionEntry> entries = null;
    
    public PermissionsConfig() {
        init();
    }

    /**
     * Get the read grant for the given Artifact URI. 
     *
     * @param artifactURI The Artifact URI.
     * @return The read grant information object for the artifact URI.
     */
    public ReadGrant getReadGrant(URI artifactURI) {
        InventoryUtil.assertNotNull(PermissionsConfig.class, "artifactURI", artifactURI);
        boolean anonymousRead = false;
        List<GroupURI> groups = new ArrayList<GroupURI>();
        List<PermissionEntry> matchingEntries = getMatchingEntries(artifactURI);
        log.debug("compiling read grant from " + matchingEntries.size() + " matching entries");
        for (PermissionEntry next : matchingEntries) {
            if (!anonymousRead) {
                anonymousRead = next.anonRead;
            }
            for (GroupURI groupURI : next.readOnlyGroups) {
                if (!groups.contains(groupURI)) {
                    groups.add(groupURI);
                }
            }
        }
        ReadGrant readGrant = new ReadGrant(artifactURI, getExpiryDate(), anonymousRead);
        readGrant.getGroups().addAll(groups);
        return readGrant;
    }
    
    /**
     * Get the write grant for the given Artifact URI. 
     *
     * @param artifactURI The Artifact URI.
     * @return The write grant information object for the artifact URI.
     */
    public WriteGrant getWriteGrant(URI artifactURI) {
        InventoryUtil.assertNotNull(PermissionsConfig.class, "artifactURI", artifactURI);
        List<GroupURI> groups = new ArrayList<GroupURI>();
        List<PermissionEntry> matchingEntries = getMatchingEntries(artifactURI);
        log.debug("compiling write grant from " + matchingEntries.size() + " matching entries");
        for (PermissionEntry next : matchingEntries) {
            for (GroupURI groupURI : next.readWriteGroups) {
                if (!groups.contains(groupURI)) {
                    groups.add(groupURI);
                }
            }
        }
        WriteGrant writeGrant = new WriteGrant(artifactURI, getExpiryDate());
        writeGrant.getGroups().addAll(groups);
        return writeGrant;
    }
    
    List<PermissionEntry> getMatchingEntries(URI artifactURI) {
        List<PermissionEntry> list = new ArrayList<PermissionEntry>();
        for (PermissionEntry next : entries) {
            Matcher matcher = next.pattern.matcher(artifactURI.toString());
            if (matcher.matches()) {
                list.add(next);
            }
        }
        return list;
    }

    /**
     * Read the permissions config.
     * 
     * @return The (possibly) cached permissions.
     */
    private void init() {
        if (entries == null) {
            log.debug("initializing permissions config");
            PropertiesReader pr = new PropertiesReader(PERMISSIONS_CONFIG_PROPERTIES);
            MultiValuedProperties allProps = pr.getAllProperties();
            if (allProps == null) {
                throw new IllegalStateException("failed to read permissions config from " + PERMISSIONS_CONFIG_PROPERTIES);
            }
            List<String> entryConfig = allProps.getProperty(KEY_ENTRY);
            if (entryConfig == null) {
                throw new IllegalStateException("no entries found in " + PERMISSIONS_CONFIG_PROPERTIES);
            }
            log.debug("reading permissions config with " + entryConfig.size() + " entries.");
            List<PermissionEntry> tmp = new ArrayList<PermissionEntry>();
            PermissionEntry next = null;
            String name = null;
            Pattern pattern = null;
            for (String entry : entryConfig) {
                // entry has format:  entry = name pattern
                log.debug("reading permission entry: " + entry);
                String[] namePattern = entry.split(" ");
                if (namePattern.length != 2) {
                    throw new IllegalStateException("invalid config line in " + PERMISSIONS_CONFIG_PROPERTIES
                        + ": " + entry);
                }
                name = namePattern[0];
                // compile the pattern
                try {
                    pattern = Pattern.compile(namePattern[1]);
                } catch (Exception e) {
                    throw new IllegalStateException("invalid uri matching pattern in " + PERMISSIONS_CONFIG_PROPERTIES
                        + ": " + namePattern[1] + "(" + e.getMessage() + ")");
                }
                next = new PermissionEntry(name, pattern);
                if (tmp.contains(next)) {
                    throw new IllegalStateException("duplicate entry name [" + name + "] in " + PERMISSIONS_CONFIG_PROPERTIES);
                }
                
                // get other properties for this entry
                List<String> anonRead = allProps.getProperty(next.name + KEY_ANON_READ);
                if (anonRead != null && !anonRead.isEmpty()) {
                    if (anonRead.size() > 1) {
                        throw new IllegalStateException("too many entries for " + next.name + KEY_ANON_READ);
                    }
                    next.anonRead = Boolean.parseBoolean(anonRead.get(0));
                }
                List<String> readOnlyGroups = allProps.getProperty(next.name + KEY_READONLY_GROUPS);
                initAddGroups(readOnlyGroups, next.readOnlyGroups, next.name + KEY_READONLY_GROUPS);
                List<String> readWriteGroups = allProps.getProperty(next.name + KEY_READWRITE_GROUPS);
                initAddGroups(readWriteGroups, next.readWriteGroups, next.name + KEY_READWRITE_GROUPS);
                tmp.add(next);
                log.debug("Added permission entry: " + next);
            }
            log.debug("permissions initialization complete.");
            entries = tmp;
        }
    }
    
    private void initAddGroups(List<String> configList, List<GroupURI> targetList, String key) {
        if (configList != null && !configList.isEmpty()) {
            if (configList.size() > 1) {
                throw new IllegalStateException("too many entries for " + key);
            }
            String[] groups = configList.get(0).split(" ");
            for (String group : groups) {
                try {
                    targetList.add(new GroupURI(group));
                } catch (Exception e) {
                    throw new IllegalStateException("failed reading group uri: " + group
                        + "(" + e.getMessage() + ")");
                }
            }
        }
    }
    
    private Date getExpiryDate() {
        // grants expire immediately
        return new Date();
    }
    
    /**
     * This method can be used if the cache needs to be cleared so new 
     * config can be read.  Since initialization is done on construction
     * a new PermissionsConfig instance is needed after this method is
     * called.
     */
    static void clearCache() {
        entries = null;
    }
    
    class PermissionEntry {
        private String name;
        private Pattern pattern;
        boolean anonRead = false;
        List<GroupURI> readOnlyGroups = new ArrayList<GroupURI>();
        List<GroupURI> readWriteGroups = new ArrayList<GroupURI>();
        
        PermissionEntry(String name, Pattern pattern) {
            this.name = name;
            this.pattern = pattern;
        }
        
        public String getName() {
            return name;
        }

        public Pattern getPattern() {
            return pattern;
        }

        @Override
        public boolean equals(Object o) {
            return this.name.equals(((PermissionEntry) o).getName());
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("PermissionEntry=[");
            sb.append("name=[").append(name).append("],");
            sb.append("pattern=[").append(pattern).append("],");
            sb.append("anonRead=[").append(anonRead).append("],");
            sb.append("readOnlyGroups=[");
            sb.append(Arrays.toString(readOnlyGroups.toArray()));
            sb.append("],");
            sb.append("readWriteGroups=[");
            sb.append(Arrays.toString(readWriteGroups.toArray()));
            sb.append("]");
            return sb.toString();
        }
    }

}

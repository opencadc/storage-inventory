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
import java.net.URI;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;

/**
 * Class that provides access to the permissions described
 * in the permissions property file.
 * 
 * @author majorb
 *
 */
public class PermissionsConfig {
    
    private static final Logger log = Logger.getLogger(PermissionsConfig.class);

    private static final String PERMISSIONS_PROPERTIES = "baldur.properties";
    private static final String KEY_USERS = "users";
    private static final String KEY_ENTRY = "entry";
    private static final String KEY_ANON_READ = ".anon";
    private static final String KEY_READONLY_GROUPS = ".readOnlyGroups";
    private static final String KEY_READWRITE_GROUPS = ".readWriteGroups";
    private static final String KEY_EXPIRY_TIME = "expiryTime";
    
    private static Set<Principal> authPrincipals = new HashSet<Principal>();
    private static List<PermissionEntry> entries = null;
    private static Date expiryDate;
    
    PermissionsConfig() {
        init();
    }
    
    Set<Principal> getAuthorizedPrincipals() {
        return authPrincipals;
    }
    
    Iterator<PermissionEntry> getMatchingEntries(URI artifactURI) {
        return new PermissionIterator(artifactURI);
    }

    Date getExpiryDate() {
        return expiryDate;
    }

    /**
     * Read the permissions config.
     * 
     * @return The (possibly) cached permissions.
     */
    private void init() {
        if (entries == null) {
            log.debug("initializing permissions config");
            PropertiesReader pr = new PropertiesReader(PERMISSIONS_PROPERTIES);
            MultiValuedProperties allProps = pr.getAllProperties();
            if (allProps == null) {
                throw new IllegalStateException("failed to read permissions config from " + PERMISSIONS_PROPERTIES);
            }
            
            // get the authorized users
            // (TODO: Issue 41: https://github.com/opencadc/storage-inventory/issues/41)
            List<String> authUsersConfig = allProps.getProperty(KEY_USERS);
            if (authUsersConfig == null) {
                throw new IllegalStateException("missing configurations for key " + KEY_USERS + " in: "
                    + PERMISSIONS_PROPERTIES);
            }
            for (String dn : authUsersConfig) {
                log.debug("authorized dn: " + dn);
                authPrincipals.add(new X500Principal(dn));
            }
            if (authPrincipals.size() == 0) {
                throw new IllegalStateException("no values for key " + KEY_USERS + " in " + PERMISSIONS_PROPERTIES);
            }
            
            // get the permission entries
            List<String> entryConfig = allProps.getProperty(KEY_ENTRY);
            if (entryConfig == null || (entryConfig.size() == 0)) {
                throw new IllegalStateException("no entries found in " + PERMISSIONS_PROPERTIES);
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
                    throw new IllegalStateException("invalid config line in " + PERMISSIONS_PROPERTIES
                        + ": " + entry);
                }
                name = namePattern[0];
                // compile the pattern
                try {
                    pattern = Pattern.compile(namePattern[1]);
                } catch (Exception e) {
                    throw new IllegalStateException("invalid uri matching pattern in " + PERMISSIONS_PROPERTIES
                        + ": " + namePattern[1] + "(" + e.getMessage() + ")");
                }
                next = new PermissionEntry(name, pattern);
                if (tmp.contains(next)) {
                    throw new IllegalStateException("duplicate entry name [" + name + "] in " + PERMISSIONS_PROPERTIES);
                }
                
                // get other properties for this entry
                List<String> anonRead = allProps.getProperty(next.getName() + KEY_ANON_READ);
                if (anonRead != null && !anonRead.isEmpty()) {
                    if (anonRead.size() > 1) {
                        throw new IllegalStateException("too many entries for " + next.getName() + KEY_ANON_READ);
                    }
                    next.anonRead = Boolean.parseBoolean(anonRead.get(0));
                }
                List<String> readOnlyGroups = allProps.getProperty(next.getName() + KEY_READONLY_GROUPS);
                initAddGroups(readOnlyGroups, next.readOnlyGroups, next.getName() + KEY_READONLY_GROUPS);
                List<String> readWriteGroups = allProps.getProperty(next.getName() + KEY_READWRITE_GROUPS);
                initAddGroups(readWriteGroups, next.readWriteGroups, next.getName() + KEY_READWRITE_GROUPS);
                tmp.add(next);
                log.debug("Added permission entry: " + next);
            }

            // get the Grant timeout
            List<String> timeout = allProps.getProperty(KEY_EXPIRY_TIME);
            if (timeout.size() == 0) {
                throw new IllegalStateException("no values for key " + KEY_EXPIRY_TIME + " in " + PERMISSIONS_PROPERTIES);
            }
            if (timeout.size() > 1) {
                throw new IllegalStateException("multiple values for key " + KEY_EXPIRY_TIME + " in "
                    + PERMISSIONS_PROPERTIES + " where single value expected");
            }
            try {
                expiryDate = calcExpiryDate(Integer.parseInt(timeout.get(0)));
            } catch (NumberFormatException nfe) {
                throw new IllegalStateException("invalid  number value for " + KEY_EXPIRY_TIME + " in "
                    + PERMISSIONS_PROPERTIES);
            }

            log.debug("permissions initialization complete.");
            entries = tmp;
        }
    }

    Date calcExpiryDate(int expiryTime) {
        Calendar expiryDate = Calendar.getInstance();
        expiryDate.add(Calendar.SECOND, expiryTime);
        return expiryDate.getTime();
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
    
    /**
     * This method can be used if the cache needs to be cleared so new 
     * config can be read.  Since initialization is done on construction
     * a new PermissionsConfig instance is needed after this method is
     * called.
     */
    static void clearCache() {
        entries = null;
    }
    
    /**
     * Class that iterates over permission entries that match the
     * given artifact URI.
     */
    class PermissionIterator implements Iterator<PermissionEntry> {
        
        URI artifactURI;
        Iterator<PermissionEntry> entryIterator;
        PermissionEntry next = null;
        
        PermissionIterator(URI artifactURI) {
            this.artifactURI = artifactURI;
            entryIterator = entries.iterator();
            advance();
        }
        
        private void advance() {
            if (entryIterator.hasNext()) {
                PermissionEntry nextEntry = entryIterator.next();
                Matcher matcher = nextEntry.getPattern().matcher(artifactURI.toString());
                if (matcher.matches()) {
                    next = nextEntry;
                } else {
                    advance();
                }
            } else {
                next = null;
            }
        }
        
        public boolean hasNext() {
            return next != null;
        }
        
        public PermissionEntry next() {
            if (next == null) {
                throw new NoSuchElementException("no more matching entries");
            }
            PermissionEntry ret = next;
            advance();
            return ret;
        }
        
    }

}

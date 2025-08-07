/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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
************************************************************************
*/

package org.opencadc.inventory;

import ca.nrc.cadc.util.HexUtil;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.persist.Entity;

/**
 * Static utility methods.
 *
 * @author pdowler
 */
public abstract class InventoryUtil {
    private static final Logger log = Logger.getLogger(InventoryUtil.class);

    public static final String BUCKET_CHARS = "0123456789abcdef";
    
    private InventoryUtil() {
    }
    
    /**
     * Chose between two artifacts to resolve a collision: two artifacts with same Artifact.uri
     * but different Entity.id values. The winning artifact should be retained/used and the losing 
     * artifact should be removed/ignored.
     * 
     * @param local the local artifact
     * @param remote the remote artifact
     * @return true if remote artifact wins, false if local artifact wins, null if tied
     */
    public static Boolean isRemoteWinner(Artifact local, Artifact remote) {
        if (local.getID().equals(remote.getID())) {
            throw new IllegalArgumentException("method called with same instances: "
                    + local.getID() + " vs " + remote.getID());
        }
        if (local.getContentLastModified().after(remote.getContentLastModified())) {
            return false;
        }
        if (remote.getContentLastModified().after(local.getContentLastModified())) {
            return true;
        }
        
        // tie
        return null;
    }
    
    /**
     * Compute a short code based on the URI argument. The returned code is a hex
     * string of the specified length generated from the given URI.
     *
     * @param uri the URI to compute from
     * @param length length of hex string
     * @return short code
     */
    public static String computeBucket(URI uri, int length) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] bytes = uri.toASCIIString().trim().getBytes("UTF-8");
            md.update(bytes);
            byte[] sha = md.digest();
            String hex = HexUtil.toHex(sha);
            return hex.substring(0, length);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("BUG: failed to get instance of SHA-1", ex);
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("BUG: failed to encode String in UTF-8", ex);
        }
    }
    
    /**
     * Compute the filename of an artifact URI.
     * @param uri The uri to parse
     * @return The filename
     */
    public static String computeArtifactFilename(URI uri) {
        validateArtifactURI(InventoryUtil.class, uri);
        String ssp = uri.getSchemeSpecificPart();
        int lastSlash = ssp.lastIndexOf("/");
        if (lastSlash > 0) {
            return ssp.substring(lastSlash + 1);
        }
        return ssp;
    }

    /**
     * Load and instantiate an instance of the specified Java interface. This method uses
     * the fully-qualified class name as a system property key; the value of the system property
     * is the fully qualified class name of an implementation of that interface.
     *
     * @param <T>   Class type of the instantiated class
     * @param clazz an interface class
     * @return configured implementation of the interface
     * @throws IllegalStateException if an instance cannot be created
     */
    public static <T> T loadPlugin(Class<T> clazz) throws IllegalStateException {
        String cnameProp = clazz.getName();
        String cname = System.getProperty(cnameProp);
        if (cname == null) {
            throw new IllegalStateException("CONFIG: " + cnameProp + " not set");
        }
        try {
            Class<?> c = Class.forName(cname);
            return (T) c.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("CONFIG: " + cnameProp + " implementation not found in classpath: " + cname, ex);
        } catch (InstantiationException | NoSuchMethodException ex) {
            throw new IllegalStateException("CONFIG: " + cnameProp + " implementation " + cname + " does not have a no-arg constructor", ex);
        } catch (InvocationTargetException ex) {
            Throwable cause = ex.getCause();
            if (cause != null) { // it has to be, but just to be safe
                throw new IllegalStateException("CONFIG: " + cnameProp + " init failed: " + cause.getMessage(), cause);
            }
            throw new IllegalStateException("CONFIG: " + cnameProp + " init failed: " + ex.getMessage(), ex);
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException("CONFIG: failed to instantiate " + cname, ex);
        }
    }

    /**
     * Load and instantiate an instance of the specified Java interface.  This method uses
     * the fully-qualified class name as a system property key; the value of the system property
     * is the fully qualified class name of an implementation of that interface.
     *
     * <p>This overloaded method will use a series of constructor arguments to build a new instance.  It assumes that
     * the requested Class contains a constructor with the given arguments.
     *
     * @param <T>             Class type of the instantiated class
     * @param implementationClassName   Class name to create
     * @param constructorArgs The constructor arguments
     * @return configured implementation of the interface
     *
     * @throws IllegalStateException if an instance cannot be created
     */
    public static <T> T loadPlugin(final String implementationClassName, final Object... constructorArgs)
            throws IllegalStateException {
        if (implementationClassName == null) {
            throw new IllegalStateException("Implementation class name cannot be null.");
        }
        try {
            Class<?> c = Class.forName(implementationClassName);
            for (final Constructor<?> constructor : c.getDeclaredConstructors()) {
                if (constructor.getParameterCount() == constructorArgs.length) {
                    return (T) constructor.newInstance(constructorArgs);
                }
            }
            throw new IllegalStateException("No matching constructor found.");
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("CONFIG: " + implementationClassName + " implementation not found in classpath: " + implementationClassName,
                                            ex);
        } catch (InstantiationException ex) {
            throw new IllegalStateException(
                    "CONFIG: " + implementationClassName + " implementation " + implementationClassName + " does not have a matching constructor", ex);
        } catch (InvocationTargetException ex) {
            Throwable cause = ex.getCause();
            if (cause != null) { // it has to be, but just to be safe
                throw new IllegalStateException("CONFIG: " + implementationClassName + " init failed: " + cause.getMessage(), cause);
            }
            throw new IllegalStateException("CONFIG: " + implementationClassName + " init failed: " + ex.getMessage(), ex);
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException("CONFIG: failed to instantiate " + implementationClassName, ex);
        }
    }

    /**
     * Validates that a URI conforms to the {scheme}:{scheme-specific-part}
     * pattern and that {scheme-specific-part} is a relative path with each
     * forward-slash (/) separated component being a valid path component.
     *
     * @param caller class performing the test
     * @param uri artifact URI to check
     * @throws IllegalArgumentException if the uri does not conform
     */
    public static void validateArtifactURI(Class caller, URI uri) {
        if (uri.getFragment() != null || uri.getQuery() != null
            || uri.getUserInfo() != null
            || uri.getAuthority() != null || uri.getHost() != null || uri.getPort() != -1) {
            throw new IllegalArgumentException(caller.getSimpleName()
                + ": invalid Artifact.uri: " + uri + " -- authority|query|fragment|host|port not permitted");
        }

        String scheme = uri.getScheme();
        String ssp = uri.getSchemeSpecificPart();
        if (scheme == null || ssp == null || ssp.isEmpty()) {
            throw new IllegalArgumentException(caller.getSimpleName()
                + ": invalid Artifact.uri: " + uri 
                    + " -- expected {scheme}:{scheme-specific-part} where {scheme-specific-part} is a relative path ending with a filename");
        }
        if (ssp.charAt(0) == '/') {
            throw new IllegalArgumentException(caller.getSimpleName()
                + ": invalid Artifact.uri: " + uri 
                    + " -- expected {scheme}:{scheme-specific-part} where {scheme-specific-part} is a relative path ending with a filename");
        }
        char lastChar = ssp.charAt(ssp.length() - 1);
        if (lastChar == ':' || lastChar == '/') {
            throw new IllegalArgumentException(caller.getSimpleName()
                + ": invalid Artifact.uri: " + uri 
                    + " -- expected {scheme}:{scheme-specific-part} where {scheme-specific-part} is a relative path ending with a filename");
        }
        String[] comps = ssp.split("/");
        for (String c : comps) {
            assertValidPathComponent(null, "scheme-specific-part", c);
        }
    }
    
    /**
     * Find storage site by unique id.
     *
     * @param id entity ID
     * @param sites list of known sites
     * @return matching site or null if not found
     */
    public static StorageSite findSite(UUID id, Collection<StorageSite> sites) {
        for (StorageSite s : sites) {
            if (s.getID().equals(id)) {
                return s;
            }
        }
        return null;
    }

    /**
     * Find storage site by resourceID.
     *
     * @param resourceID service identifier
     * @param sites list of known sites
     * @return matching site or null if not found
     */
    public static StorageSite findSite(URI resourceID, Collection<StorageSite> sites) {
        for (StorageSite s : sites) {
            if (s.getResourceID().equals(resourceID)) {
                return s;
            }
        }
        return null;
    }

    /**
     * Assign last modified timestamp to an entity. This method is to support
     * persisting/serialising and reconstructing/deserialising an entity.
     *
     * @param ce the entity
     * @param d the timestamp
     */
    public static void assignLastModified(Entity ce, Date d) {
        try {
            Field f = Entity.class.getDeclaredField("lastModified");
            f.setAccessible(true);
            f.set(ce, d);
        } catch (NoSuchFieldException | IllegalAccessException oops) {
            throw new RuntimeException("BUG", oops);
        }
    }

    /**
     * Assign metaChecksum URI to an entity. This method is to support
     * persisting/serialising and reconstructing/deserialising an entity.
     *
     * @param ce the entity
     * @param u the URI
     */
    public static void assignMetaChecksum(Entity ce, URI u) {
        assertValidChecksumURI(InventoryUtil.class, "metaChecksum", u);
        try {
            Field f = Entity.class.getDeclaredField("metaChecksum");
            f.setAccessible(true);
            f.set(ce, u);
        } catch (NoSuchFieldException | IllegalAccessException oops) {
            throw new RuntimeException("BUG", oops);
        }
    }

    /**
     * Utility method so constructors can validate arguments.
     *
     * @param caller class doing test
     * @param name field name being checked
     * @param test object to test
     * @throws IllegalArgumentException if the value is invalid
     */
    public static void assertNotNull(Class caller, String name, Object test)
            throws IllegalArgumentException {
        if (test == null) {
            throw new IllegalArgumentException("invalid " + caller.getSimpleName() + "." + name + ": null");
        }
    }

    /**
     * A valid path component cannot have: space ( ), slash (/), escape (\), percent (%),
     * semi-colon (;), ampersand (&amp;), dollar ($), or question (?) characters.
     *
     * @param caller class doing test
     * @param name field name being checked
     * @param test object to test
     * @throws IllegalArgumentException if the value is invalid
     */
    public static void assertValidPathComponent(Class caller, String name, String test) {
        assertNotNull(caller, name, test);
        log.debug("assertValidPathComponent: " + test);
        boolean space = (test.indexOf(' ') >= 0);
        boolean slash = (test.indexOf('/') >= 0);
        boolean escape = (test.indexOf('\\') >= 0);
        boolean percent = (test.indexOf('%') >= 0);
        //boolean colon = (test.indexOf(':') >= 0); // reverted in 1.0.1
        boolean semic = (test.indexOf(';') >= 0);
        boolean amp = (test.indexOf('&') >= 0);
        boolean dollar = (test.indexOf('$') >= 0);
        boolean question = (test.indexOf('?') >= 0);
        boolean sqopen = (test.indexOf('[') >= 0);
        boolean sqclose = (test.indexOf(']') >= 0);

        if (space || slash || escape || percent || semic || amp || dollar || question || sqopen || sqclose) {
            String s = "invalid ";
            if (caller != null) {
                s += caller.getSimpleName() + ".";
            }
            throw new IllegalArgumentException(s + name + ": " + test
                    + " reason: path component may not contain space ( ), slash (/), escape (\\), percent (%),"
                    + " semi-colon (;), ampersand (&), dollar ($), question (?), or square brackets ([])");
        }
    }

    /**
     * Checksum URI validation.
     *
     * @param caller class doing test
     * @param name field name being checked
     * @param uri URI to test
     * @throws IllegalArgumentException if the value is invalid
     */
    public static void assertValidChecksumURI(Class caller, String name, URI uri) {
        String alg = uri.getScheme();
        String sval = uri.getSchemeSpecificPart();
        if (alg == null || sval == null) {
            throw new IllegalArgumentException("invalid " + caller.getSimpleName() + "." + name + ": "
                + uri + "reason: expected <algorithm>:<hex value>");
        }
        byte[] b;
        try {
            b = HexUtil.toBytes(sval);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("invalid checksum URI: " 
                + uri + " contains invalid hex value, expected <algorithm>:<hex value>");
        }
        
        if ("md5".equals(alg)) {
            if (b.length != 16) {
                throw new IllegalArgumentException("invalid checksum URI: " + uri 
                        + " found " + b.length + " bytes, expected 16");
            }
            return;
        }
        
        if ("sha-1".equals(alg)) {
            if (b.length != 20) {
                throw new IllegalArgumentException("invalid checksum URI: " + uri 
                        + " found " + b.length + " bytes, expected 20");
            }
            return;
        } 
        
        if ("sha-256".equals(alg)) {
            if (b.length != 32) {
                throw new IllegalArgumentException("invalid checksum URI: " + uri 
                        + " found " + b.length + " bytes, expected 32");
            }
            return;
        }
        
        if ("sha-384".equals(alg)) {
            if (b.length != 48) {
                throw new IllegalArgumentException("invalid checksum URI: " + uri 
                        + " found " + b.length + " bytes, expected 48");
            }
            return;
        }
        
        if ("sha-512".equals(alg)) {
            if (b.length != 64) {
                throw new IllegalArgumentException("invalid checksum URI: " + uri 
                        + " found " + b.length + " bytes, expected 64");
            }
            return;
        }
        
        if ("adler".equals(alg)) {
            if (b.length != 4) {
                throw new IllegalArgumentException("invalid checksum URI: " + uri 
                        + " found " + b.length + " bytes, expected 4");
            }
            return;
        }
        
        throw new IllegalArgumentException("invalid checksum URI: " + uri + " unsupported algorithm");
    }
}

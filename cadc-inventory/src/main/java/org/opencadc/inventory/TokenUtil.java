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
************************************************************************
 */

package org.opencadc.inventory;

import ca.nrc.cadc.util.Base64;
import ca.nrc.cadc.util.RsaSignatureGenerator;
import ca.nrc.cadc.util.RsaSignatureVerifier;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.AccessControlException;
import java.security.InvalidKeyException;

import org.apache.log4j.Logger;

/**
 * Utilities the generation and validation of pre-authorized tokens for artifact
 * download and upload.
 * 
 * @author majorb
 *
 */
public class TokenUtil {

    private static final Logger log = Logger.getLogger(TokenUtil.class);

    private static final String KEY_META_URI = "uri";
    private static final String KEY_META_METHOD = "met";
    private static final String KEY_META_SUBJECT = "sub";
    
    private static final String PUB_KEY_FILENAME = "InventoryPub.key";
    private static final String PRIV_KEY_FILENAME = "InventoryPriv.key";
    
    private static final String TOKEN_DELIM = "~";
    
    /**
     * Valid methods that apply to authorization.
     */
    public static enum HttpMethod {
        GET, PUT
    }

    /**
     * Generate an artifact token given the input parameters.
     * @param uri The artifact URI
     * @param method The method applied to the artifact.
     * @param user The user initiating the action on the artifact.
     * @return A pre-authorized signed token.
     */
    public static String generateToken(URI uri, HttpMethod method, String user) {

        // create the metadata and signature segments
        StringBuilder metaSb = new StringBuilder();
        metaSb.append(KEY_META_URI).append("=").append(uri.toString());
        metaSb.append("&");
        metaSb.append(KEY_META_METHOD).append("=").append(method.toString());
        metaSb.append("&");
        metaSb.append(KEY_META_SUBJECT).append("=").append(user);
        byte[] metaBytes = metaSb.toString().getBytes();

        RsaSignatureGenerator sg = new RsaSignatureGenerator(PRIV_KEY_FILENAME);
        String sig;
        try {
            byte[] sigBytes = sg.sign(new ByteArrayInputStream(metaBytes));
            sig = new String(Base64.encode(sigBytes));
            log.debug("Created signature: " + sig + " for meta: " + metaSb.toString());
        } catch (InvalidKeyException | IOException | RuntimeException e) {
            throw new IllegalStateException("Could not sign token", e);
        }
        String meta = new String(Base64.encode(metaBytes));
        log.debug("meta: " + meta);
        log.debug("sig: " + sig);

        // build the token
        StringBuilder token = new StringBuilder();
        String metaURLEncoded = base64URLEncode(meta);
        String sigURLEncoded = base64URLEncode(sig);

        log.debug("metaURLEncoded: " + metaURLEncoded);
        log.debug("sigURLEncoded: " + sigURLEncoded);

        token.append(metaURLEncoded);
        token.append(TOKEN_DELIM);
        token.append(sigURLEncoded);
        log.debug("Created token path: " + token.toString());

        return token.toString();
    }
    
    /**
     * Validate the given token with the expectations expressed in the parameters.
     * 
     * @param token The token to validate.
     * @param expectedURI The expected artifact URI.
     * @param expectedMethod The expected method to be applied to the artifact.
     * @return The user contained in the token.
     * @throws AccessControlException If any of the expectations are not met or if the token is invalid.
     * @throws IOException If a processing error occurs.
     */
    public static String validateToken(String token, URI expectedURI, HttpMethod expectedMethod) throws AccessControlException, IOException {

        log.debug("validating token: " + token);
        String[] parts = token.split(TOKEN_DELIM);
        if (parts.length != 2) {
            log.debug("invalid format, not two parts");
            throw new AccessControlException("Invalid auth token");
        }

        byte[] metaBytes = Base64.decode(base64URLDecode(parts[0]));
        byte[] sigBytes = Base64.decode(base64URLDecode(parts[1]));

        RsaSignatureVerifier sv = new RsaSignatureVerifier(PUB_KEY_FILENAME);
        boolean verified;
        try {
            verified = sv.verify(new ByteArrayInputStream(metaBytes), sigBytes);
        } catch (InvalidKeyException | RuntimeException e) {
            log.debug("Recieved invalid signature", e);
            throw new AccessControlException("Invalid auth token");
        }
        if (!verified) {
            log.debug("verified==false");
            throw new AccessControlException("Invalid auth token");
        }

        String[] metaParams = new String(metaBytes).split("&");
        String uri = null;
        String method = null;
        String user = null;
        for (String metaParam : metaParams) {
            log.debug("Processing param: " + metaParam);
            int eqIndex = metaParam.indexOf("=");
            if (eqIndex < 2) {
                log.debug("invalid param key/value pair");
                throw new AccessControlException("Invalid auth token");
            }
            String key = metaParam.substring(0, eqIndex);
            String value = metaParam.substring(eqIndex + 1);
            if (KEY_META_URI.equals(key)) {
                uri = value;
            }
            if (KEY_META_METHOD.equals(key)) {
                method = value;
            }
            if (KEY_META_SUBJECT.equals(key)) {
                user = value;
            }
        }
        log.debug("uri: " + uri);
        log.debug("method: " + method);
        log.debug("subject: " + user);
        
        if (!expectedURI.toString().equals(uri)) {
            log.debug("wrong target uri");
            throw new AccessControlException("Invalid auth token");
        }
        if (!expectedMethod.toString().equals(method)) {
            log.debug("wrong http method");
            throw new AccessControlException("Invalid auth token");
        }
        
        // validation passed, return the user for logging
        return user;

    }

    /**
     * Make a base 64 string safe for URLs.
     * @param s The string to encode.
     * @return The encoded string.
     */
    static String base64URLEncode(String s) {
        if (s == null) {
            return null;
        }
        return s.replace("/", "-").replace("+", "_");
    }

    /**
     * Decode a URL encoded base 64 string.
     * @param s The string to decode.
     * @return The decoded string.
     */
    static String base64URLDecode(String s) {
        if (s == null) {
            return null;
        }
        return s.replace("_", "+").replace("-", "/");
    }

}

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
************************************************************************
*/

package org.opencadc.inventory.storage;

import ca.nrc.cadc.util.HexUtil;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.zip.Adler32;
import org.apache.log4j.Logger;
import org.apache.xerces.impl.dv.util.Base64;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.EncodableDigest;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.digests.SHA224Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.digests.SHA512Digest;

/**
 *
 * @author pdowler
 */
public class MessageDigestAPI {
    private static final Logger log = Logger.getLogger(MessageDigestAPI.class);

    private final Digest impl;
    private static final List<String> ALG_NAMES = Arrays.asList(
            "md5", "sha1", "sha224", "sha256", "sha512"
    );
    // hack to work with EOS
    private Adler32 adler32;
   
    
    private MessageDigestAPI(EncodableDigest impl) { 
        this.impl = (Digest) impl;
    }
    
    private MessageDigestAPI(Adler32 impl) {
        this.impl = null;
        this.adler32 = impl;
    }

    public static List<String> getAlgorithmNames() {
        return ALG_NAMES;
    }
    
    /**
     * Create a new bouncy castle message digest that supports state encoding.
     * 
     * @param algorithm case-insensitive digest algorithm
     * @return message digest instance
     * @throws NoSuchAlgorithmException if the specified algorithm is not supported
     */
    public static MessageDigestAPI getInstance(String algorithm) throws NoSuchAlgorithmException {
        if ("md5".equalsIgnoreCase(algorithm)) {
            return new MessageDigestAPI(new MD5Digest());
        }
        if ("sha1".equalsIgnoreCase(algorithm)) {
            return new MessageDigestAPI(new SHA1Digest());
        }
        if ("sha224".equalsIgnoreCase(algorithm)) {
            return new MessageDigestAPI(new SHA224Digest());
        }
        if ("sha256".equalsIgnoreCase(algorithm)) {
            return new MessageDigestAPI(new SHA256Digest());
        }
        if ("sha512".equalsIgnoreCase(algorithm)) {
            return new MessageDigestAPI(new SHA512Digest());
        }
        if ("adler".equalsIgnoreCase(algorithm)) {
            return new MessageDigestAPI(new Adler32());
        }
        throw new NoSuchAlgorithmException("not found: " + algorithm);
    }
    
    /**
     * Get the current internal state of the specified digest.
     * 
     * @param d an EncodableDigest instance
     * @return string form of internal state
     */
    public static String getEncodedState(MessageDigestAPI d) {
        if (d.impl == null && d.adler32 != null) {
            throw new UnsupportedOperationException("cannot encode adler digect");
        }
        String alg = d.getAlgorithmName();
        byte[] ret = ((EncodableDigest)d.impl).getEncodedState();
        String base64 = Base64.encode(ret);
        //log.info("encoded state: " + ret.length + " bytes"); 
        //log.info("encoded base64: " + base64.length() + " " + base64);
        return alg + ":" + base64;
    }
    
    /**
     * Restore a digest from string form of internal state.
     * @param encodedState string form of internal state
     * @return restored digest
     * @throws java.security.NoSuchAlgorithmException if the encoded state references an unsupported algorithm
     */
    public static MessageDigestAPI getDigest(String encodedState) throws NoSuchAlgorithmException {
        URI u = URI.create(encodedState);
        String algorithm = u.getScheme();
        byte[] encodedBytes = Base64.decode(u.getSchemeSpecificPart());
        if ("md5".equalsIgnoreCase(algorithm)) {
            return new MessageDigestAPI(new MD5Digest(encodedBytes));
        }
        if ("sha1".equalsIgnoreCase(algorithm)) {
            return new MessageDigestAPI(new SHA1Digest(encodedBytes));
        }
        if ("sha224".equalsIgnoreCase(algorithm)) {
            return new MessageDigestAPI(new SHA224Digest(encodedBytes));
        }
        if ("sha256".equalsIgnoreCase(algorithm)) {
            return new MessageDigestAPI(new SHA256Digest(encodedBytes));
        }
        if ("sha512".equalsIgnoreCase(algorithm)) {
            return new MessageDigestAPI(new SHA512Digest(encodedBytes));
        }
        throw new NoSuchAlgorithmException("not found: " + algorithm);
    }
    
    public String getAlgorithmName() {
        if (adler32 != null) {
            return "adler";
        }
        return impl.getAlgorithmName().toLowerCase();
    }
    
    public void update(byte[] bytes, int offset, int length) {
        if (adler32 != null) {
            adler32.update(bytes, offset, length);
        } else {
            impl.update(bytes, offset, length);
        }
    }
    
    public void update(byte[] bytes) {
        if (adler32 != null) {
            adler32.update(bytes);
        } else {
            impl.update(bytes, 0, bytes.length);
        }
    }
    
    public byte[] digest() {
        if (adler32 != null) {
            int val = (int) adler32.getValue();
            return HexUtil.toBytes(val);
        }
        byte[] ret = new byte[impl.getDigestSize()];
        impl.doFinal(ret, 0);
        return ret;
    }
    
    public void reset() {
        if (adler32 != null) {
            adler32.reset();
        } else {
            impl.reset();
        }
    }
}

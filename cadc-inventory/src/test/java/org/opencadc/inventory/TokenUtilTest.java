package org.opencadc.inventory;

import java.io.File;
import java.net.URI;
import java.security.AccessControlException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.xerces.impl.dv.util.Base64;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencadc.inventory.TokenUtil.HttpMethod;

import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.RsaSignatureGenerator;

public class TokenUtilTest {
    
    private static final Logger log = Logger.getLogger(TokenUtilTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.minoc", Level.DEBUG);
    }
    
    static File pubFile, privFile;
    
    @BeforeClass
    public static void initKeys() throws Exception {
        log.info("Creating test key pair");
        String keysDir = "build/resources/test";
        File pub = new File(keysDir + "/InventoryPub.key");
        File priv = new File(keysDir + "/InventoryPriv.key");
        RsaSignatureGenerator.genKeyPair(pub, priv, 1024);
        privFile = new File(keysDir, RsaSignatureGenerator.PRIV_KEY_FILE_NAME);
        pubFile = new File(keysDir, RsaSignatureGenerator.PUB_KEY_FILE_NAME);
        log.info("Created pub key: " + pubFile.getAbsolutePath());
    }
    
    @AfterClass
    public static void cleanupKeys() throws Exception {
        if (pubFile != null) {
            pubFile.delete();
        }
        if (privFile != null) {
            privFile.delete();
        }
    }
    
    @Test
    public void testRoundTripToken() {
        try {
            
            String[] uris = new String[] {
                "cadc:TEST/file.fits",
                "cadc:TEST/file.fits",
                "mast:HST/long/file/path/preview.png",
                "mast:HST/long/file/path/preview.png",
                "cadc:TEST/file.fits",
                "cadc:TEST/file.fits",
            };
            HttpMethod[] methods = new HttpMethod[] {
                HttpMethod.GET,
                HttpMethod.PUT,
                HttpMethod.GET,
                HttpMethod.PUT,
                HttpMethod.GET,
                HttpMethod.PUT,
            };
            String[] users = new String[] {
                "user",
                "user",
                "user",
                "user",
                "C=CA, O=Grid, OU=nrc-cnrc.gc.ca, CN=Brian Major",
                "C=CA, O=Grid, OU=nrc-cnrc.gc.ca, CN=Brian Major",
            };
            
            for (int i=0; i<uris.length; i++) {
                String uri = uris[i];
                HttpMethod method = methods[i];
                String user = users[i];
                String token = TokenUtil.generateToken(URI.create(uri), method, user);
                String actUser = TokenUtil.validateToken(token, URI.create(uri), method);
                Assert.assertEquals("user", user, actUser);
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testWrongURI() {
        try {

            String uri = "cadc:TEST/file.fits";
            HttpMethod method = HttpMethod.GET;
            String user = "user";
            String token = TokenUtil.generateToken(URI.create(uri), method, user);
            try {
                TokenUtil.validateToken(token, URI.create("cadc:TEST/file2.fits"), HttpMethod.GET);
                Assert.fail("Should have failed with wrong uri");
            } catch (AccessControlException e) {
                // expected
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testWrongMethod() {
        try {

            String uri = "cadc:TEST/file.fits";
            HttpMethod method = HttpMethod.GET;
            String user = "user";
            String token = TokenUtil.generateToken(URI.create(uri), method, user);
            try {
                TokenUtil.validateToken(token, URI.create(uri), HttpMethod.PUT);
                Assert.fail("Should have failed with wrong method");
            } catch (AccessControlException e) {
                // expected
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testTamperMetadata() {
        try {

            String uri = "cadc:TEST/file.fits";
            HttpMethod method = HttpMethod.GET;
            String user = "user";
            String token = TokenUtil.generateToken(URI.create(uri), method, user);
            String[] parts = token.split("~");
            String newToken = TokenUtil.base64URLEncode(Base64.encode("junk".getBytes())) + "~" + parts[1];
            try {
                TokenUtil.validateToken(newToken, URI.create(uri), HttpMethod.PUT);
                Assert.fail("Should have failed with invalid metadata");
            } catch (AccessControlException e) {
                // expected
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testTamperSignature() {
        try {

            String uri = "cadc:TEST/file.fits";
            HttpMethod method = HttpMethod.GET;
            String user = "user";
            String token = TokenUtil.generateToken(URI.create(uri), method, user);
            String[] parts = token.split("~");
            String newToken = parts[0] + "~" + TokenUtil.base64URLEncode(Base64.encode("junk".getBytes()));
            try {
                TokenUtil.validateToken(newToken, URI.create(uri), HttpMethod.PUT);
                Assert.fail("Should have failed with invalid signature");
            } catch (AccessControlException e) {
                // expected
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}

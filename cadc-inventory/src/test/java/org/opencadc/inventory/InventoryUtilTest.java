/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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
import ca.nrc.cadc.util.Log4jInit;

import java.net.URI;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class InventoryUtilTest {
    private static final Logger log = Logger.getLogger(InventoryUtilTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
    }

    public InventoryUtilTest() {
    }
    
    /*
    @Test
    public void testTemplate() {
        try {
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    */

    @Test
    public void testCollisionResolver() {
        URI md5 = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
        long asize = 2014L;
        long t = System.currentTimeMillis();
        try {
            final Artifact aold = new Artifact(URI.create("cadc:TEST/a1"), md5, new Date(t - 100), asize);
            final Artifact anew = new Artifact(URI.create("cadc:TEST/a1"), md5, new Date(t + 100), asize);
            final Artifact asame = new Artifact(URI.create("cadc:TEST/a1"), md5, new Date(t + 100), asize);
            
            Boolean win;
            
            // remote newer
            win = InventoryUtil.isRemoteWinner(aold, anew);
            Assert.assertTrue(win);
            win = InventoryUtil.isRemoteWinner(aold, anew);
            Assert.assertTrue(win);
            
            // local newer
            win = InventoryUtil.isRemoteWinner(anew, aold);
            Assert.assertFalse(win);
            win = InventoryUtil.isRemoteWinner(anew, aold);
            Assert.assertFalse(win);
            
            // ties
            win = InventoryUtil.isRemoteWinner(anew, asame);
            Assert.assertNull(win);
            win = InventoryUtil.isRemoteWinner(asame, anew);
            Assert.assertNull(win);
            
            final Artifact sameInstance = new Artifact(aold.getID(), URI.create("cadc:TEST/a1"), md5, new Date(t), asize);
            try {
                InventoryUtil.isRemoteWinner(aold, sameInstance);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            try {
                InventoryUtil.isRemoteWinner(aold, sameInstance);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testValidateArtifactURI() {
        try {
            InventoryUtil.validateArtifactURI(InventoryUtilTest.class, URI.create("cadc:FOO/bar.fits"));

            InventoryUtil.validateArtifactURI(InventoryUtilTest.class, URI.create("cadc:FOO/organised/into/directories/bar.fits"));

            InventoryUtil.validateArtifactURI(InventoryUtilTest.class, URI.create("vault:" + UUID.randomUUID().toString()));

            InventoryUtil.validateArtifactURI(InventoryUtilTest.class, URI.create("cadc:FOO/bar+baz.fits"));

            try {
                URI u = URI.create("cadc:");
                log.info("check scheme: " + u);
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            try {
                URI u = URI.create("cadc:foo/");
                log.info("check directory: " + u);
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            try {
                URI u = URI.create("cadc:/foo/bar");
                log.info("check absolute: " + u);
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            try {
                URI u = URI.create("cadc:foo?bar");
                log.info("check query: " + u.getQuery());
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }

            try {
                URI u = URI.create("cadc:foo#bar");
                log.info("check fragment: " + u.getFragment());
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }

            try {
                URI u = URI.create("cadc:foo bar");
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }

            try {
                URI u = URI.create("cadc:foo\tbar");
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }

            try {
                URI u = URI.create("cadc:foo\\bar");
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }

            try {
                URI u = URI.create("cadc:foo%25bar"); // %25 == % sign since URI class decodes
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }

            try {
                URI u = URI.create("cadc:foo;bar");
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }

            try {
                URI u = URI.create("cadc:foo&bar");
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }

            try {
                URI u = URI.create("cadc:foo$bar");
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            try {
                URI u = URI.create("cadc:foo[bar");
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            try {
                URI u = URI.create("cadc:foo]bar");
                InventoryUtil.validateArtifactURI(InventoryUtilTest.class, u);
                Assert.fail("expected invalid: " + u);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testValidateChecksumURI() {
        try {
            Random rnd = new Random();
            byte[] buf = new byte[1024];
            
            MessageDigest md = MessageDigest.getInstance("md5");
            byte[] cs = md.digest(buf);
            String hex = HexUtil.toHex(cs);
            
            URI valid = URI.create("md5:" + hex);
            InventoryUtil.assertValidChecksumURI(InventoryUtilTest.class, "test", valid);
            log.info("valid: " + valid);
            
            int n = valid.toASCIIString().length() - 2; // drop one byte
            URI invalid = URI.create(valid.toASCIIString().substring(0, n));
            try {
                InventoryUtil.assertValidChecksumURI(InventoryUtilTest.class, "test", invalid);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            md = MessageDigest.getInstance("sha-1");
            cs = md.digest(buf);
            hex = HexUtil.toHex(cs);
            valid = URI.create("sha-1:" + hex);
            InventoryUtil.assertValidChecksumURI(InventoryUtilTest.class, "test", valid);
            log.info("valid: " + valid);
            
            n = valid.toASCIIString().length() - 2; // drop one byte
            invalid = URI.create(valid.toASCIIString().substring(0, n));
            try {
                InventoryUtil.assertValidChecksumURI(InventoryUtilTest.class, "test", invalid);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            md = MessageDigest.getInstance("sha-256");
            cs = md.digest(buf);
            hex = HexUtil.toHex(cs);
            valid = URI.create("sha-256:" + hex);
            InventoryUtil.assertValidChecksumURI(InventoryUtilTest.class, "test", valid);
            log.info("valid: " + valid);
            
            n = valid.toASCIIString().length() - 2; // drop one byte
            invalid = URI.create(valid.toASCIIString().substring(0, n));
            try {
                InventoryUtil.assertValidChecksumURI(InventoryUtilTest.class, "test", invalid);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            md = MessageDigest.getInstance("sha-384");
            cs = md.digest(buf);
            hex = HexUtil.toHex(cs);
            valid = URI.create("sha-384:" + hex);
            InventoryUtil.assertValidChecksumURI(InventoryUtilTest.class, "test", valid);
            log.info("valid: " + valid);
            
            n = valid.toASCIIString().length() - 2; // drop one byte
            invalid = URI.create(valid.toASCIIString().substring(0, n));
            try {
                InventoryUtil.assertValidChecksumURI(InventoryUtilTest.class, "test", invalid);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            md = MessageDigest.getInstance("sha-512");
            cs = md.digest(buf);
            hex = HexUtil.toHex(cs);
            valid = URI.create("sha-512:" + hex);
            InventoryUtil.assertValidChecksumURI(InventoryUtilTest.class, "test", valid);
            log.info("valid: " + valid);
            
            n = valid.toASCIIString().length() - 2; // drop one byte
            invalid = URI.create(valid.toASCIIString().substring(0, n));
            try {
                InventoryUtil.assertValidChecksumURI(InventoryUtilTest.class, "test", invalid);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
            invalid = URI.create("foo:" + hex);
            try {
                InventoryUtil.assertValidChecksumURI(InventoryUtilTest.class, "test", invalid);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeArtifactFilename() {
        try {
            String randomUUID = UUID.randomUUID().toString();
            Assert.assertEquals("bar.fits",
                InventoryUtil.computeArtifactFilename(URI.create("cadc:bar.fits")));
            Assert.assertEquals("bar.fits",
                InventoryUtil.computeArtifactFilename(URI.create("cadc:FOO/bar.fits")));
            Assert.assertEquals("bar.fits",
                InventoryUtil.computeArtifactFilename(URI.create("cadc:FOO/organised/into/directories/bar.fits")));
            Assert.assertEquals(randomUUID,
                InventoryUtil.computeArtifactFilename(URI.create("vault:" + randomUUID)));
            Assert.assertEquals("bar+baz.fits",
                InventoryUtil.computeArtifactFilename(URI.create("cadc:FOO/bar+baz.fits")));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testLoadFail() {
        try {
            System.clearProperty("org.opencadc.inventory.storage.StorageAdapter");
            InventoryUtil.loadPlugin(Comparator.class);
            Assert.fail("missing config should fail");
        } catch (IllegalStateException expected) {
            log.info("missing config - caught: " + expected + " cause: " + expected.getCause());
        }

        try {
            System.setProperty(Comparator.class.getName(), "org.opencadc.inventory.storage.InventoryUtilTest$NoImpl");
            InventoryUtil.loadPlugin(Comparator.class);
            Assert.fail("missing class should fail");
        } catch (IllegalStateException expected) {
            log.info("missing class - caught: " + expected + " cause: " + expected.getCause());
            Assert.assertTrue(expected.getCause() instanceof ClassNotFoundException);
        } finally {
            System.clearProperty(Comparator.class.getName());
        }

        try {
            System.setProperty(Comparator.class.getName(), "org.opencadc.inventory.storage.InventoryUtilTest$InvalidCtorImpl");
            System.setProperty(Comparator.class.getName(), InvalidCtorImpl.class.getName());
            InventoryUtil.loadPlugin(Comparator.class);
            Assert.fail("missing no-arg ctor should fail");
        } catch (IllegalStateException expected) {
            log.info("missing no-arg ctor - caught: " + expected + " cause: " + expected.getCause());
            Assert.assertTrue(expected.getCause() instanceof NoSuchMethodException);
        } finally {
            System.clearProperty(Comparator.class.getName());
        }

        try {
            System.setProperty(Comparator.class.getName(), "org.opencadc.inventory.storage.InventoryUtilTest$NoInterfaceImpl");
            InventoryUtil.loadPlugin(Comparator.class);
            Assert.fail("missing interface should fail");
        } catch (IllegalStateException expected) {
            log.info("missing interface - caught: " + expected + " cause: " + expected.getCause());
            Assert.assertTrue(expected.getCause() instanceof ClassNotFoundException);
        } finally {
            System.clearProperty(Comparator.class.getName());
        }

        try {
            System.setProperty(Comparator.class.getName(), "org.opencadc.inventory.storage.InventoryUtilTest$CtorThrowsImpl");
            InventoryUtil.loadPlugin(Comparator.class);
            Assert.fail("ctor throws should fail");
        } catch (IllegalStateException expected) {
            log.info("ctor throws - caught: " + expected);
        } finally {
            System.clearProperty(Comparator.class.getName());
        }

    }

    @Test
    public void testLoadFailConstructorArgs() {
        try {
            InventoryUtil.loadPlugin("org.opencadc.inventory.InventoryUtilTest$ValidImpl", 88);
            Assert.fail("missing class should fail");
        } catch (IllegalStateException expected) {
            log.info("missing class - caught: " + expected + " cause: " + expected.getCause());
            Assert.assertEquals("Wrong message.", "No matching constructor found.", expected.getMessage());
        }

        try {
            InventoryUtil.loadPlugin("org.opencadc.inventory.InventoryUtilTest$ValidImpl", 88, "STRING", false);
            Assert.fail("missing class should fail");
        } catch (IllegalStateException expected) {
            log.info("missing class - caught: " + expected + " cause: " + expected.getCause());
            Assert.assertEquals("Wrong message.", "No matching constructor found.", expected.getMessage());
        }
    }

    @Test
    public void testLoadOK() {
        try {
            System.setProperty(Comparator.class.getName(), "org.opencadc.inventory.InventoryUtilTest$ValidImpl");
            Comparator c = InventoryUtil.loadPlugin(Comparator.class);
            log.info("loaded: " + c.getClass().getName());
        } finally {
            System.clearProperty(Comparator.class.getName());
        }
    }

    @Test
    public void testLoadOKConstructorArgs() {
        try {
            Comparator c = InventoryUtil.loadPlugin("org.opencadc.inventory.InventoryUtilTest$ValidImpl", false, "GOOD");
            log.info("loaded: " + c.getClass().getName());
        } finally {
            System.clearProperty(Comparator.class.getName());
        }
    }


    public static class InvalidCtorImpl implements Comparator<Object> {
        public InvalidCtorImpl(boolean b) {
        }

        @Override
        public int compare(Object a, Object b) {
            throw new UnsupportedOperationException();
        }
    }

    public static class CtorThrowsImpl implements Comparator<Object> {
        public CtorThrowsImpl() {
            throw new IllegalStateException("CtorThrowsImpl: setup fail");
        }

        @Override
        public int compare(Object a, Object b) {
            throw new UnsupportedOperationException();
        }
    }

    public static class ValidImpl implements Comparator<Object> {
        public ValidImpl() {
        }

        public ValidImpl(boolean b, String s) {

        }

        @Override
        public int compare(Object a, Object b) {
            throw new UnsupportedOperationException();
        }
    }
}

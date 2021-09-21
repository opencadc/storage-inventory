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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.raven;

import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.PropertiesReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class RavenInitActionTest {
    private static final Logger log = Logger.getLogger(RavenInitActionTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.raven", Level.INFO);
    }

    static String USER_HOME = System.getProperty("user.home");

    @Test
    public void testValidRavenProperties() {
        System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "src/test/resources/testValidConfig");

        try {
            System.setProperty("user.home", "src/test/resources/testValidConfig");
            RavenInitAction testSubject = new RavenInitAction() {
                @Override
                public void doInit() {
                    initConfig();
                    initStorageSiteRules();
                }
            };
            testSubject.doInit();
        } catch (Exception e) {
            Assert.fail("exception not expected; " + e.getMessage());
        } finally {
            System.setProperty("user.home", USER_HOME);
        }
    }

    @Test
    public void testInvalidConfig() {
        System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "src/test/resources/testInvalidConfig");

        try {
            System.setProperty("user.home", "src/test/resources/testInvalidConfig");
            RavenInitAction testSubject = new RavenInitAction() {
                @Override
                public void doInit() {
                    initConfig();
                }
            };
            testSubject.doInit();
            Assert.fail("exception expected to be thrown");
        } catch (Exception e) {
            String message = e.getMessage();
            log.debug(message);
            Assert.assertTrue(message.contains(String.format("%s: MISSING", RavenInitAction.SCHEMA_KEY)));
            Assert.assertTrue(message.contains(String.format("%s: MISSING", RavenInitAction.PUBKEYFILE_KEY)));
            Assert.assertTrue(message.contains(String.format("%s: MISSING", RavenInitAction.PRIVKEYFILE_KEY)));
        } finally {
            System.setProperty("user.home", USER_HOME);
        }
    }

    @Test
    public void testInvalidStorageSiteRules() {
        System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, "src/test/resources/testInvalidStorageSiteRules");

        try {
            System.setProperty("user.home", "src/test/resources/testInvalidConfig");
            RavenInitAction testSubject = new RavenInitAction() {
                @Override
                public void doInit() {
                    initStorageSiteRules();
                }
            };
            testSubject.doInit();
            Assert.fail("exception expected to be thrown");
        } catch (Exception e) {
            String message = e.getMessage();
            log.debug(message);
            Assert.assertTrue(message.contains("resourceID: MISSING"));
            Assert.assertTrue(message.contains("resourceID: EMPTY VALUE"));
            Assert.assertTrue(message.contains("resourceID: MULTIPLE ENTRIES"));
            Assert.assertTrue(message.contains("resourceID: INVALID URI"));
            Assert.assertTrue(message.contains("namespaces: MISSING"));
            Assert.assertTrue(message.contains("namespaces: EMPTY VALUE"));
            Assert.assertTrue(message.contains("namespaces: MULTIPLE ENTRIES"));
        } finally {
            System.setProperty("user.home", USER_HOME);
        }
    }

}

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


package org.opencadc.raven;

import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.PropertiesReader;
import java.io.File;
import java.io.FileOutputStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class StorageResolverTest {
    private static final Logger log = Logger.getLogger(StorageResolverTest.class);

    private ArtifactAction artifactAction;

    private static final String DEFAULT_RAVEN_CONFIG =
            "org.opencadc.raven.inventory.schema=inventory\n" +
            "org.opencadc.raven.publicKeyFile=raven-pub.key\n" +
            "org.opencadc.raven.privateKeyFile=raven-priv.key\n" +
            "org.opencadc.raven.consistency.preventNotFound=true";
    static {
        Log4jInit.setLevel("org.opencadc.raven", Level.DEBUG);
    }

    String getTestConfigDir() {
        return System.getProperty("user.dir") + "/build/tmp";
    }

    public StorageResolverTest() {
         artifactAction = new ArtifactAction(false) {
            @Override
            void parseRequest() throws Exception { }
            @Override
            protected InlineContentHandler getInlineContentHandler() {
                return null;
            }
            @Override
            public void doAction() throws Exception { }
        };
    }

    @Test
    public void testParsePath() throws Exception {
        File propFile = new File(getTestConfigDir(), "raven.properties");
        try {
            System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, getTestConfigDir());
            if (!propFile.exists())
                propFile.createNewFile();

            FileOutputStream out = new FileOutputStream(propFile);
            String test_properties = DEFAULT_RAVEN_CONFIG;
            out.write(test_properties.getBytes("UTF-8"));
            out.close();
            artifactAction.initResolvers();
            Assert.assertTrue(artifactAction.storageResolvers.isEmpty());

            out = new FileOutputStream(propFile);
            test_properties = DEFAULT_RAVEN_CONFIG + "\norg.opencadc.raven.storageResolver.entry=scheme1 org.opencadc.raven.MockStorageResolver";
            out.write(test_properties.getBytes("UTF-8"));
            out.close();
            artifactAction.initResolvers();
            Assert.assertEquals(1, artifactAction.storageResolvers.size());
            Assert.assertTrue(artifactAction.storageResolvers.get("scheme1") instanceof MockStorageResolver);

            out = new FileOutputStream(propFile);
            test_properties = DEFAULT_RAVEN_CONFIG +
                    "\norg.opencadc.raven.storageResolver.entry=scheme2 org.opencadc.raven.MockStorageResolver" +
                    "\norg.opencadc.raven.storageResolver.entry=scheme3 org.opencadc.raven.MockStorageResolver";
            out.write(test_properties.getBytes("UTF-8"));
            out.close();
            artifactAction.initResolvers();
            Assert.assertEquals(2, artifactAction.storageResolvers.size());
            Assert.assertTrue(artifactAction.storageResolvers.get("scheme2") instanceof MockStorageResolver);
            Assert.assertTrue(artifactAction.storageResolvers.get("scheme3") instanceof MockStorageResolver);

            // invalid key
            Assert.assertFalse(artifactAction.storageResolvers.containsKey("scheme1"));
        } finally {
            System.clearProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY);
            // cleanup
            if (propFile.exists())
                propFile.delete();
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testInvalidResolverClass() throws Exception{
        // invalid resolver class
        File propFile = new File(getTestConfigDir(), "raven.properties");
        try {
            System.setProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY, getTestConfigDir());
            if (!propFile.exists())
                propFile.createNewFile();
            FileOutputStream out = new FileOutputStream(propFile);
            String test_properties = DEFAULT_RAVEN_CONFIG + "\norg.opencadc.raven.storageResolver.entry=scheme1 org.opencadc.not.Exist";
            out.write(test_properties.getBytes("UTF-8"));
            out.close();
            boolean pass = false;
            artifactAction.initResolvers();
        } finally {
            System.clearProperty(PropertiesReader.CONFIG_DIR_SYSTEM_PROPERTY);
            // cleanup
            if (propFile.exists())
                propFile.delete();
        }


    }
}


/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2020.                            (c) 2020.
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
 *
 ************************************************************************
 */

package org.opencadc.tantar;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.StringUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * Main application entry.  This class expects a tantar.properties file to be available and readable.
 */
public class Main {
    private static final String LOGGING_KEY = Main.class.getPackage().getName() + ".logging";
    
    public static final String APPLICATION_NAME = "tantar";

    public static void main(final String[] args) {
        Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
        Log4jInit.setLevel("org.opencadc", Level.WARN);

        final String certificateFileLocation = String.format("%s/%s/cadcproxy.pem", System.getProperty("user.home"),
                                                             ".ssl");
        final Logger logger = Logger.getLogger(Main.class);
        final Reporter reporter = new Reporter(logger);

        reporter.start();

        try {
            final PropertiesReader propertiesReader = new PropertiesReader("tantar.properties");
            final MultiValuedProperties applicationProperties = propertiesReader.getAllProperties();

            // The PropertiesReader won't throw a FileNotFoundException if the given file doesn't exist at all.  We'll
            // need to throw it here as an appropriate way to notify users.
            if (applicationProperties == null) {
                throw new FileNotFoundException("Unable to locate configuration file.");
            }

            final String configuredLogging = applicationProperties.getFirstPropertyValue(LOGGING_KEY);

            Log4jInit.setLevel("org.opencadc.tantar", StringUtil.hasText(configuredLogging)
                                                       ? Level.toLevel(configuredLogging.toUpperCase()) : Level.INFO);

            final BucketValidator bucketValidator = new BucketValidator(applicationProperties, reporter,
                                                                        SSLUtil.createSubject(
                                                                                new File(certificateFileLocation)));

            bucketValidator.validate();
        } catch (IllegalStateException e) {
            // IllegalStateExceptions are thrown for missing but required configuration.
            logger.fatal(e.getMessage(), e);
            System.exit(3);
        } catch (FileNotFoundException e) {
            logger.fatal(e.getMessage(), e);
            System.exit(1);
        } catch (IOException e) {
            logger.fatal(e.getMessage(), e);
            System.exit(2);
        } catch (Exception e) {
            // Used to catch everything else, such as a RuntimeException when a file cannot be obtained and put.
            logger.fatal(e.getMessage(), e);
            System.exit(4);
        } finally {
            reporter.end();
        }
    }
}

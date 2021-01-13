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
************************************************************************
*/

package org.opencadc.ratik;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.HexUtil;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class TestUtil {
    private static final Logger log = Logger.getLogger(TestUtil.class);
    static String INVENTORY_SERVER = "RATIK_TEST";
    static String INVENTORY_DATABASE = "cadctest";
    static String INVENTORY_SCHEMA = "inventory";
    static String LUSKAN_SERVER = "LUSKAN_TEST";
    static String LUSKAN_SCHEMA = "inventory";
    static String LUSKAN_DATABASE = "cadctest";
    static URI LUSKAN_URI = URI.create("ivo://cadc.nrc.ca/luskan");
    
    static String ZERO_BYTES_MD5 = "md5:d41d8cd98f00b204e9800998ecf8427e";

    static {
        try {
            File opt = FileUtil.getFileFromResource("intTest.properties", TestUtil.class);
            if (opt.exists()) {
                Properties props = new Properties();
                props.load(new FileReader(opt));

                if (props.containsKey("inventoryServer")) {
                    INVENTORY_SERVER = props.getProperty("inventoryServer").trim();
                }
                if (props.containsKey("inventoryDatabase")) {
                    INVENTORY_DATABASE = props.getProperty("inventoryDatabase").trim();
                }
                if (props.containsKey("inventorySchema")) {
                    INVENTORY_SCHEMA = props.getProperty("inventorySchema").trim();
                }
                if (props.containsKey("luskanServer")) {
                    LUSKAN_SERVER = props.getProperty("luskanServer").trim();
                }
                if (props.containsKey("luskanSchema")) {
                    LUSKAN_SCHEMA = props.getProperty("luskanSchema").trim();
                }
                if (props.containsKey("luskanDatabase")) {
                    LUSKAN_DATABASE = props.getProperty("luskanDatabase").trim();
                }
                if (props.containsKey("luskanURI")) {
                    LUSKAN_URI = URI.create(props.getProperty("luskanURI").trim());
                }
            }
        } catch (MissingResourceException | FileNotFoundException noFileException) {
            log.debug("No intTest.properties supplied.  Using defaults.");
        } catch (IOException oops) {
            throw new RuntimeException(oops.getMessage(), oops);
        }

        log.debug("intTest database config: " + INVENTORY_SERVER + " " + INVENTORY_DATABASE + " " + INVENTORY_SCHEMA);
    }
    
    private TestUtil() { 
    }

    static Subject getConfiguredSubject() {
        return SSLUtil.createSubject(new File(System.getProperty("user.home") + "/.ssl/cadcproxy.pem"));
    }
    
    static URI getRandomMD5() {
        UUID uuid = UUID.randomUUID();
        return URI.create("md5:" + HexUtil.toHex(uuid.getMostSignificantBits()) + HexUtil.toHex(uuid.getLeastSignificantBits()));
    }
}

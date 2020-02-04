/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020.                            (c) 2030.
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

package org.opencadc.inventory.db.version;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import java.io.IOException;
import java.util.List;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Place holder for manual call to force InitDatase.
 * 
 * @author pdowler
 */
public class Main implements Runnable {
    private static final Logger log = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            ArgumentMap am = new ArgumentMap(args);
            Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
            Log4jInit.setLevel("org.opencadc", Level.WARN);
            if (am.isSet("d") || am.isSet("debug")) {
                Log4jInit.setLevel("org.opencadc.inventory", Level.DEBUG);
            } else if (am.isSet("v") || am.isSet("verbose")) {
                Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
            }

            if (am.isSet("h") || am.isSet("help")) {
                usage();
                return;
            }
            
            List<String> ss = am.getPositionalArgs();
            if (ss.size() != 3) {
                log.error("missing required commandline args");
                usage();
                System.exit(1);
            }
            
            Main m = new Main(ss.get(0), ss.get(1), ss.get(2));
            m.run();
            
        } catch (Throwable unexpected) {
            log.error("unexpected error", unexpected);
            System.exit(2);
        }
    }
    
    private static void usage() {
        System.out.println("usage: cadc-inventory-db [-v|--verbose|-d|--debug] <server> <database> <schema>");
    }
    
    private final String server;
    private final String database;
    private final String schema;
    
    private Main(String server, String database, String schema) {
        this.server = server;
        this.database = database;
        this.schema = schema;
    }
    
    public void run() {
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(server, database);
            String driver = cc.getDriver();
            if (driver == null) {
                throw new RuntimeException("failed to find JDBC driver for " + server + "," + database);
            }
            DataSource ds = DBUtil.getDataSource(cc);
            log.info("target: " + server + " " + database + " " + schema);
            
            InitDatabase init = new InitDatabase(ds, database, schema);
            boolean result = init.doInit();
            if (result) {
                log.info("init: complete");
            } else {
                log.info("init: no-op");
            }
        } catch (IOException ex) {
            throw new RuntimeException("failed to read connection info from $HOME/.dbrc", ex);
        }
    }
}

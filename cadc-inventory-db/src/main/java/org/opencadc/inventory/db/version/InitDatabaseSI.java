/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

import ca.nrc.cadc.db.version.InitDatabase;
import java.net.URL;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class InitDatabaseSI extends InitDatabase {
    private static final Logger log = Logger.getLogger(InitDatabaseSI.class);
    
    public static final String MODEL_NAME = "storage-inventory";
    public static final String MODEL_VERSION = "1.0.5";
    public static final String PREV_MODEL_VERSION = "1.0.0";
    //public static final String PREV_MODEL_VERSION = "DO-NOT_UPGRADE-BY-ACCIDENT";

    static String[] CREATE_SQL = new String[] {
        "generic.ModelVersion.sql",
        "inventory.Artifact.sql",
        "inventory.StorageSite.sql",
        "inventory.ObsoleteStorageLocation.sql",
        "inventory.DeletedArtifactEvent.sql",
        "inventory.DeletedStorageLocationEvent.sql",
        "inventory.StorageLocationEvent.sql",
        "generic.HarvestState.sql",
        "generic.PreauthKeyPair.sql",
        "generic.permissions.sql"
    };
    
    static String[] UPGRADE_SQL = new String[] {
        "inventory.upgrade-1.0.5.sql",
        "generic.permissions.sql"
    };
    
    public InitDatabaseSI(DataSource ds, String database, String schema) { 
        super(ds, database, schema, MODEL_NAME, MODEL_VERSION, PREV_MODEL_VERSION);
        for (String s : CREATE_SQL) {
            createSQL.add(s);
        }
        for (String s : UPGRADE_SQL) {
            upgradeSQL.add(s);
        }
    }

    @Override
    protected URL findSQL(String fname) {
        // SQL files are stored inside the jar file
        return InitDatabaseSI.class.getClassLoader().getResource(fname);
    }
}

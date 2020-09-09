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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.luskan;

import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.KeyColumnDesc;
import ca.nrc.cadc.tap.schema.KeyDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.uws.Job;

public class TestUtil {

    /**
     * load a TAP Schema for test purpose.
     *
     */
    public static TapSchema loadTapSchema() {
        return mockTapSchema();
    }

    /**
     * @return a mocked TAP schema
     */
    public static TapSchema mockTapSchema() {

        TapSchema tapSchema = new TapSchema();

        String schemaName;
        SchemaDesc schemaDesc;

        // tap_schema schema
        schemaName = "tap_schema";
        schemaDesc = new SchemaDesc(schemaName);
        tapSchema.getSchemaDescs().add(schemaDesc);

        String tableName;
        TableDesc tableDesc;

        // tap_schema.schemas11
        tableName = schemaName + ".schemas11";
        tableDesc = new TableDesc(schemaName, tableName);
        schemaDesc.getTableDescs().add(tableDesc);
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "schema_index", TapDataType.INTEGER));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "description", new TapDataType("char", "512*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "schema_name", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "utype", new TapDataType("char", "512*", null)));

        // tap_schema.tables11
        tableName = schemaName + ".tables11";
        tableDesc = new TableDesc(schemaName, tableName);
        schemaDesc.getTableDescs().add(tableDesc);
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "table_index", TapDataType.INTEGER));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "description", new TapDataType("char", "512*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "schema_name", new TapDataType("char", "512*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "table_name", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "table_type", new TapDataType("char", "8*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "utype", new TapDataType("char", "512*", null)));
        KeyDesc k = new KeyDesc("k1", "TAP_SCHEMA.tables11", "TAP_SCHEMA.schemas11");
        k.getKeyColumnDescs().add(new KeyColumnDesc("k1", "schema_name", "schema_name"));
        tableDesc.getKeyDescs().add(k);

        // tap_schema.columns11
        tableName = schemaName + ".columns11";
        tableDesc = new TableDesc(schemaName, tableName);
        schemaDesc.getTableDescs().add(tableDesc);
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "\"size\"", TapDataType.INTEGER));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "principal", TapDataType.INTEGER));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "indexed", TapDataType.INTEGER));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "std", TapDataType.INTEGER));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "column_index", TapDataType.INTEGER));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "arraysize", new TapDataType("char", "16*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "column_name", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "datatype", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "description", new TapDataType("char", "512*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "table_name", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "ucd", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "unit", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "utype", new TapDataType("char", "512*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "xtype", new TapDataType("char", "64*", null)));
        k = new KeyDesc("k2", "TAP_SCHEMA.columns11", "TAP_SCHEMA.tables11");
        k.getKeyColumnDescs().add(new KeyColumnDesc("k2", "table_name", "table_name"));
        tableDesc.getKeyDescs().add(k);

        // tap_schema.keys11
        tableName = schemaName + ".keys11";
        tableDesc = new TableDesc(schemaName, tableName);
        schemaDesc.getTableDescs().add(tableDesc);
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "description", new TapDataType("char", "512*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "from_table", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "key_id", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "target_table", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "utype", new TapDataType("char", "512*", null)));
        k = new KeyDesc("k3", "TAP_SCHEMA.keys11", "TAP_SCHEMA.tables11");
        k.getKeyColumnDescs().add(new KeyColumnDesc("k3", "from_table", "table_name"));
        tableDesc.getKeyDescs().add(k);
        k = new KeyDesc("k4", "TAP_SCHEMA.keys11", "TAP_SCHEMA.tables11");
        k.getKeyColumnDescs().add(new KeyColumnDesc("k4", "target_table", "table_name"));
        tableDesc.getKeyDescs().add(k);

        // tap_schema.key_columns11
        tableName = schemaName + ".key_columns11";
        tableDesc = new TableDesc(schemaName, tableName);
        schemaDesc.getTableDescs().add(tableDesc);
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "from_column", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "key_id", new TapDataType("char", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "target_column", new TapDataType("char", "64*", null)));
        k = new KeyDesc("k5", "TAP_SCHEMA.key_columns11", "TAP_SCHEMA.keys11");
        k.getKeyColumnDescs().add(new KeyColumnDesc("k5", "key_id", "key_id"));
        tableDesc.getKeyDescs().add(k);

        // inventory schema
        schemaName = "inventory";
        schemaDesc = new SchemaDesc(schemaName);
        tapSchema.getSchemaDescs().add(schemaDesc);

        // inventory.Artifact
        tableName = schemaName + ".Artifact";
        tableDesc = new TableDesc(schemaName, tableName);
        schemaDesc.getTableDescs().add(tableDesc);
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "uri", new TapDataType("char", "512*", "uri")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "uriBucket", new TapDataType("char", "5", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "contentChecksum", new TapDataType("char", "136*", "uri")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "contentLastModified", new TapDataType("char", "23*", "timestamp")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "contentLength", TapDataType.LONG));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "contentType", new TapDataType("char", "128*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "contentEncoding", new TapDataType("char", "128*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "lastModified", new TapDataType("char", "23*", "timestamp")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "metaChecksum", new TapDataType("char", "136*", "uri")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "id", new TapDataType("char", "36", "uuid")));

        // inventory.StorageSite
        tableName = schemaName + ".StorageSite";
        tableDesc = new TableDesc(schemaName, tableName);
        schemaDesc.getTableDescs().add(tableDesc);
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "resourceID", new TapDataType("char", "512*", "uri")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "name", new TapDataType("char", "32*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "id", new TapDataType("char", "36", "uuid")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "lastModified", new TapDataType("char", "23*", "timestamp")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "metaChecksum", new TapDataType("char", "136*", "uri")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "allowRead", TapDataType.BOOLEAN));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "allowWrite", TapDataType.BOOLEAN));

        // inventory.DeletedArtifactEvent
        tableName = schemaName + ".DeletedArtifactEvent";
        tableDesc = new TableDesc(schemaName, tableName);
        schemaDesc.getTableDescs().add(tableDesc);
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "id", new TapDataType("char", "36", "uuid")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "lastModified", new TapDataType("char", "23*", "timestamp")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "metaChecksum", new TapDataType("char", "136*", "uri")));

        // inventory.DeletedStorageLocationEvent
        tableName = schemaName + ".DeletedStorageLocationEvent";
        tableDesc = new TableDesc(schemaName, tableName);
        schemaDesc.getTableDescs().add(tableDesc);
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "id", new TapDataType("char", "36", "uuid")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "lastModified", new TapDataType("char", "23*", "timestamp")));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "metaChecksum", new TapDataType("char", "136*", "uri")));

        return tapSchema;
    }

    static Job job = new Job() {
        @Override
        public String getID() {
            return "internal-test-jobID";
        }
    };

}

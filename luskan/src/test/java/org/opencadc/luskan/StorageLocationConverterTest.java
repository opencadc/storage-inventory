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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.luskan;

import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.uws.Parameter;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.luskan.tap.AdqlQueryImpl;

public class StorageLocationConverterTest {
    private static final Logger log = Logger.getLogger(StorageLocationConverterTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.luskan", Level.INFO);
    }

    private static final TapSchema tapSchema = TestUtil.mockTapSchema();

    // global luskan
    // HACK: this currently applies the inverse so queries can use the index on unstored artifacts
    @Test
    public void testSelectArtifactInGlobal() {
        String query = "select id from inventory.artifact";
        String expected = "SELECT id FROM inventory.artifact WHERE (inventory.artifact.storagelocation_storageid IS NULL)";
        doTest(query, expected, "false");
    }

    // Not the inventory.artifact table, should not append the is null constraint
    @Test
    public void testSchemaAndTableName() {
        String query = "select id from inventory.storagesite";
        String expected = "SELECT id FROM inventory.storagesite";
        doTest(query, expected, "true");
    }
    
    @Test
    public void testUnfilteredArtifactMetadata() {
        String query = "select id from inventory.ArtifactMetadata";
        String expected = "SELECT id FROM inventory.Artifact";
        doTest(query, expected, "true");
    }
    
    @Test
    public void testPendingArtifact() {
        String query = "select id from inventory.PendingArtifact";
        String expected = "SELECT id FROM inventory.Artifact WHERE (inventory.artifact.storagelocation_storageid IS NULL)";
        doTest(query, expected, "true");
    }
    
    @Test
    public void testSelectWithStorageLocation() {
        String query = "select id from inventory.artifact";
        String expected = "SELECT id FROM inventory.artifact WHERE (inventory.artifact.storagelocation_storageid IS NOT NULL)";
        doTest(query, expected, "true");
    }

    @Test
    public void testSelectWithTableAlias() {
        String query = "select id from inventory.artifact a";
        String expected = "SELECT id from inventory.artifact AS a WHERE (a.storagelocation_storageid IS NOT NULL)";
        doTest(query, expected, "true");
    }

    @Test
    public void testSelectWithTableJoin() {
        String query = "select a.contentLength from inventory.artifact a join inventory.artifact b on a.id = b.id";
        String expected = "SELECT a.contentLength from inventory.artifact AS a JOIN inventory.artifact AS b on a.id = b.id"
                + " WHERE (a.storagelocation_storageid IS NOT NULL) AND (b.storagelocation_storageid IS NOT NULL)";
        doTest(query, expected, "true");
    }

    @Test
    public void testSelectWithWhere() {
        String query = "select id from inventory.artifact where contentLength <= 1024";
        String expected = "SELECT id from inventory.artifact WHERE (contentLength <= 1024) and"
                + " (inventory.artifact.storagelocation_storageid IS NOT NULL)";
        doTest(query, expected, "true");
    }

    private void doTest(final String query, final String expected, final String isStorageLocation) {
        try {
            TestUtil.job.getParameterList().clear();
            List<Parameter> params = new ArrayList<Parameter>();
            params.add(new Parameter("QUERY", query));
            log.info("query: " + query);
            TapQuery tq = new TestQuery(isStorageLocation);
            tq.setTapSchema(tapSchema);
            TestUtil.job.getParameterList().addAll(params);
            tq.setJob(TestUtil.job);
            log.info("expected: " + expected);
            String sql = tq.getSQL();
            log.info("actual: " + sql);

            sql = sql.toLowerCase();
            Assert.assertTrue(sql.equalsIgnoreCase(expected));
        } catch (Exception e) {
            log.error("unexpected exception", e);
            Assert.fail();
        } finally {
            TestUtil.job.getParameterList().clear();
        }
    }

    private static class TestQuery extends AdqlQueryImpl {

        private final String isStorageLocation;
        
        public TestQuery(String isStorageLocation) {
            this.isStorageLocation = isStorageLocation;
        }

        @Override
        protected MultiValuedProperties getProperties() {
            return new MultiValuedProperties() {
                @Override
                public String getFirstPropertyValue(String value) {
                    if (LuskanConfig.STORAGE_SITE_KEY.equals(value)) {
                        return isStorageLocation;
                    }
                    return null;
                }
            };
        }
    }

}


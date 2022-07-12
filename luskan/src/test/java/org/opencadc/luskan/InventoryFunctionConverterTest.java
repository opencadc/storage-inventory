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

package org.opencadc.luskan;

import org.opencadc.luskan.tap.AdqlQueryImpl;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.uws.Parameter;

public class InventoryFunctionConverterTest {
    private static final Logger log = Logger.getLogger(InventoryFunctionConverterTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.luskan", Level.INFO);
        Log4jInit.setLevel("net.sf.jsqlparser", Level.INFO);
    }

    private static final TapSchema tapSchema = TestUtil.mockTapSchema();

    @Test
    public void testSelect() {
        String query = "select *, num_copies() from inventory.Artifact where id = '{some artifact id}'";
        String expected = "SELECT inventory.Artifact.id, inventory.Artifact.contentLength, cardinality(inventory.Artifact.siteLocations) FROM inventory.Artifact WHERE id = '{some artifact id}'";
        doTest(query, expected, "cardinality(inventory.Artifact.siteLocations)");
    }

    @Test
    public void testWhere() {
        String query = "select count(*) from inventory.Artifact where num_copies() = 3";
        String expected = "select count(*) from inventory.Artifact where cardinality(inventory.Artifact.siteLocations) = 3";
        doTest(query, expected, "cardinality(inventory.Artifact.siteLocations)");
    }

    @Test
    public void testGroupByWithAlias() {
        String query = "select count(*), num_copies() as num from inventory.Artifact group by num";
        String expected = "select count(*), cardinality(inventory.Artifact.siteLocations) as num from inventory.Artifact group by num";
        doTest(query, expected, "cardinality(inventory.Artifact.siteLocations)");
    }

    @Test
    public void testAlias() {
        String query = "select count(*), num_copies() from inventory.Artifact as a";
        String expected = "select count(*), cardinality(a.siteLocations) from inventory.Artifact as a";
        doTest(query, expected, "cardinality(a.siteLocations)");
    }

    @Test
    public void testMultipleArtifactTables() {
        String query = "select count(*), num_copies() from inventory.Artifact as a, temp.Artifact as b where a.id = b.id";
        String expected = "select count(*), cardinality(a.siteLocations) from inventory.Artifact as a, temp.Artifact as b where a.id = b.id";
        doTest(query, expected, "cardinality(a.siteLocations)");
    }

    @Test
    public void testNoFromTables() {
        String query = "select num_copies()";
        doTestFailure(query);
    }

    @Test
    public void testNotInventorySchema() {
        String query = "select count(*), num_copies() from temp.Artifact";
        doTestFailure(query);
    }

    @Test
    public void testMultipleInventoryArtifactTables() {
        String query = "select count(*), num_copies() from inventory.Artifact as a, inventory.Artifact as b";
        doTestFailure(query);
    }

    @Test
    public void testNoInventoryArtifactTable() {
        String query = "select num_copies() from DeletedArtifactEvent";
        doTestFailure(query);
    }

    private void doTest(final String query, final String origFunction, String expectedFunction) {
        try {
            TestUtil.job.getParameterList().clear();
            List<Parameter> params = new ArrayList<Parameter>();
            params.add(new Parameter("QUERY", query));
            log.info("query: " + query);
            TapQuery tq = new TestQuery();
            tq.setTapSchema(tapSchema);
            TestUtil.job.getParameterList().addAll(params);
            tq.setJob(TestUtil.job);
            String sql = tq.getSQL();
            log.info("actual: " + sql);
            
            Assert.assertFalse(sql.contains(origFunction));
            Assert.assertTrue(sql.contains(expectedFunction));
            
        } catch (Exception e) {
            log.error("unexpected exception", e);
            Assert.fail();
        } finally {
            TestUtil.job.getParameterList().clear();
        }
    }

    private void doTestFailure(final String query) {
        try {
            TestUtil.job.getParameterList().clear();
            List<Parameter> params = new ArrayList<Parameter>();
            params.add(new Parameter("QUERY", query));
            log.info("query: " + query);
            TapQuery tq = new TestQuery();
            tq.setTapSchema(tapSchema);
            TestUtil.job.getParameterList().addAll(params);
            tq.setJob(TestUtil.job);
            String sql = tq.getSQL();
            Assert.fail("Exception expected");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    private static class TestQuery extends AdqlQueryImpl {

        public TestQuery() {}

        @Override
        protected MultiValuedProperties getProperties() {
            return new MultiValuedProperties() {
                @Override
                public String getFirstPropertyValue(String value) {
                    return "false";
                }
            };
        }
    }
    
}

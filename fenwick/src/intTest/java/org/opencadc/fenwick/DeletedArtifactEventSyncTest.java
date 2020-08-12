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

package org.opencadc.fenwick;

import static org.opencadc.fenwick.TestUtil.INVENTORY_DATABASE;
import static org.opencadc.fenwick.TestUtil.INVENTORY_SCHEMA;
import static org.opencadc.fenwick.TestUtil.LUSKAN_DATABASE;
import static org.opencadc.fenwick.TestUtil.LUSKAN_SCHEMA;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.db.DeletedEventDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.tap.TapClient;
import org.opencadc.tap.TapRowMapper;

public class DeletedArtifactEventSyncTest {

    private static final Logger log = Logger.getLogger(DeletedArtifactEventSyncTest.class);

    private static final File PROXY_PEM = new File(System.getProperty("user.home") + "/.ssl/cadcproxy.pem");

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
        Log4jInit.setLevel("org.opencadc.fenwick", Level.INFO);
    }

    private final DeletedEventDAO<DeletedArtifactEvent> deletedEventDAO = new DeletedEventDAO<>();

    public DeletedArtifactEventSyncTest() throws Exception {
        final DBConfig dbConfig = new DBConfig();
        final ConnectionConfig cc = dbConfig.getConnectionConfig(TestUtil.LUSKAN_SERVER, LUSKAN_DATABASE);
        DBUtil.createJNDIDataSource("jdbc/DeletedEventSyncTest", cc);

        final Map<String, Object> config = new TreeMap<>();
        config.put(SQLGenerator.class.getName(), SQLGenerator.class);
        config.put("jndiDataSourceName", "jdbc/DeletedEventSyncTest");
        config.put("database", LUSKAN_DATABASE);
        config.put("schema", LUSKAN_SCHEMA);

        deletedEventDAO.setConfig(config);
    }

    @Before
    public void setup() throws SQLException {
        log.info("deleting events...");
        DataSource ds = deletedEventDAO.getDataSource();
        String sql = String.format("delete from %s.DeletedArtifactEvent", LUSKAN_SCHEMA);
        ds.getConnection().createStatement().execute(sql);
        log.info("deleting events... OK");
    }

    @Test
    public void testRowMapper() {
        try {
            log.info("testRowMapper");

            UUID uuid = UUID.randomUUID();
            Date lasModified = new Date();
            URI metaChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");

            List<Object> row = new ArrayList<>();
            row.add(uuid);
            row.add(lasModified);
            row.add(metaChecksum);

            TapRowMapper<DeletedArtifactEvent> mapper =
                new DeletedArtifactEventSync.DeletedArtifactEventRowMapper();
            DeletedArtifactEvent event = mapper.mapRow(row);

            Assert.assertNotNull(event);
            Assert.assertEquals(uuid, event.getID());
            Assert.assertEquals(lasModified, event.getLastModified());
            Assert.assertEquals(metaChecksum, event.getMetaChecksum());
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }

    @Test
    public void testGetEvents() {
        try {
            log.info("testGetEvents");
            Subject userSubject = SSLUtil.createSubject(PROXY_PEM);
            TapClient<DeletedArtifactEvent> tapClient = new TapClient<>(TestUtil.LUSKAN_URI);

            Calendar now = Calendar.getInstance();
            now.add(Calendar.DAY_OF_MONTH, -1);
            Date startTime = now.getTime();
            DeletedArtifactEventSync sync = new DeletedArtifactEventSync(tapClient);
            sync.startTime = startTime;

            Subject.doAs(userSubject, new PrivilegedExceptionAction<Object>() {

                public Object run() throws Exception {

                    // query with no results
                    ResourceIterator<DeletedArtifactEvent> emptyIterator = sync.getEvents();
                    Assert.assertNotNull(emptyIterator);
                    Assert.assertFalse(emptyIterator.hasNext());

                    DeletedArtifactEvent expected1 = new DeletedArtifactEvent(UUID.randomUUID());
                    DeletedArtifactEvent expected2 = new DeletedArtifactEvent(UUID.randomUUID());

                    deletedEventDAO.put(expected1);
                    deletedEventDAO.put(expected2);

                    // query with multiple results
                    ResourceIterator<DeletedArtifactEvent> iterator = sync.getEvents();
                    Assert.assertNotNull(iterator);

                    Assert.assertTrue(iterator.hasNext());
                    DeletedArtifactEvent actual1 = iterator.next();
                    Date lastModified1 = actual1.getLastModified();

                    Assert.assertTrue(iterator.hasNext());
                    DeletedArtifactEvent actual2 = iterator.next();
                    Date lastModified2 = actual2.getLastModified();

                    // newest date should be returned first.
                    Assert.assertTrue(lastModified1.before(lastModified2));
                    Assert.assertFalse(iterator.hasNext());

                    return null;
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }

}

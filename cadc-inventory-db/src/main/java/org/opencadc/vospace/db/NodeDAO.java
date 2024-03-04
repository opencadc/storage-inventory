/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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

package org.opencadc.vospace.db;

import ca.nrc.cadc.io.ResourceIterator;
import java.net.URI;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.db.AbstractDAO;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 * @author pdowler
 */
public class NodeDAO extends AbstractDAO<Node> {
    private static final Logger log = Logger.getLogger(NodeDAO.class);

    public NodeDAO() {
        super(true);
    }
    
    public NodeDAO(boolean origin) {
        super(origin);
    }

    @Override
    public void put(Node val) {
        super.put(val);
    }

    @Override
    public Node lock(Node n) {
        if (n == null) {
            throw new IllegalArgumentException("entity cannot be null");
        }
        // override because Node has subclasses: force base class here
        return super.lock(Node.class, n.getID());
    }
    
    public Node get(UUID id) {
        checkInit();
        return super.get(Node.class, id);
    }
    
    public Node get(ContainerNode parent, String name) {
        checkInit();
        log.debug("GET: " + parent.getID() + " + " + name);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            SQLGenerator.NodeGet get = (SQLGenerator.NodeGet) gen.getEntityGet(Node.class);
            get.setPath(parent, name);
            return get.execute(jdbc);
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + parent.getID() + " + " + name + " " + dt + "ms");
        }
        throw new RuntimeException("BUG: handleInternalFail did not throw");
    }
    
    public DataNode getDataNode(URI storageID) {
        checkInit();
        log.debug("GET: " + storageID);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            SQLGenerator.NodeGet get = (SQLGenerator.NodeGet) gen.getEntityGet(Node.class);
            get.setStorageID(storageID);
            return (DataNode) get.execute(jdbc);
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + storageID + " " + dt + "ms");
        }
        throw new RuntimeException("BUG: handleInternalFail did not throw");
    }
    
    public boolean isEmpty(ContainerNode parent) {
        checkInit();
        log.debug("isEmpty: " + parent.getID());
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            SQLGenerator.NodeCount count = (SQLGenerator.NodeCount) gen.getNodeCount();
            count.setID(parent.getID());
            int num = count.execute(jdbc);
            return (num == 0);
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("isEmpty: " + parent.getID() + " " + dt + "ms");
        }
        throw new RuntimeException("BUG: handleInternalFail did not throw");
    }
    
    public void delete(UUID id) {
        super.delete(Node.class, id);
    }
    
    /**
     * Get iterator of child nodes.
     * 
     * @param parent the container node to list
     * @param limit max number of nodes to return, or null
     * @param start list starting point, or null
     * @return iterator of child nodes matching the arguments
     */
    public ResourceIterator<Node> iterator(ContainerNode parent, Integer limit, String start) {
        if (parent == null) {
            throw new IllegalArgumentException("childIterator: parent cannot be null");
        }
        log.debug("iterator: " + parent.getID());
        
        checkInit();
        long t = System.currentTimeMillis();
        
        try {
            SQLGenerator.NodeIteratorQuery iter = (SQLGenerator.NodeIteratorQuery) gen.getEntityIteratorQuery(Node.class);
            iter.setParent(parent);
            iter.setStart(start);
            iter.setLimit(limit);
            return iter.query(dataSource);
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("iterator: " + parent.getID() + " " + dt + "ms");
        }
        throw new RuntimeException("BUG: should be unreachable");
    }
}

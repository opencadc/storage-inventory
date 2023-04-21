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

package org.opencadc.vault;

import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.TransientException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Logger;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.db.NodeDAO;

/**
 *
 * @author pdowler
 */
public class NodePersistenceImpl { //implements NodePersistence {
    private static final Logger log = Logger.getLogger(NodePersistenceImpl.class);

    private final Map<String,Object> nodeDaoConfig = new TreeMap<>();
    
    public NodePersistenceImpl(String dataSourceName, String inventorySchema, String vospaceSchema) {
        nodeDaoConfig.put(SQLGenerator.class.getName(), SQLGenerator.class);
        nodeDaoConfig.put("jndiDataSourceName", dataSourceName);
        nodeDaoConfig.put("schema", inventorySchema);
        nodeDaoConfig.put("vosSchema", vospaceSchema);
        
    }
    
    private NodeDAO getDAO() {
        NodeDAO instance = new NodeDAO();
        instance.setConfig(nodeDaoConfig);
        return instance;
    }

    /**
     * Get a node by name.
     * @param parent parent node, may be special root node but not null
     * @param name relative name of the child node
     * @return the child node or null if it does not exist
     * @throws TransientException 
     */
    public Node get(ContainerNode parent, String name) throws TransientException {
        throw new UnsupportedOperationException();
    }

    /**
     * Get an iterator over the children of a node. The output can optionally be 
     * limited to a specific number of children and can optionally start at a
     * specific child (usually the last one from a previous "batch") to resume
     * listing at a known position.
     * 
     * @param parent the container to iterate
     * @param limit max number of nodes to return, may be null
     * @param start first node in order to consider, may be null
     * @return iterator of matching child nodes, may be empty
     */
    public ResourceIterator<Node> iterator(ContainerNode parent, Integer limit, String start) {
        throw new UnsupportedOperationException();
    }

    /**
     * Load additional node properties for the specified node. Note: this may not be 
     * necessary and may be removed. TBD.
     * 
     * @param node
     * @throws TransientException 
     */
    public void getProperties(Node node) throws TransientException {
        throw new UnsupportedOperationException();
    }

    /**
     * Put the specified node. This can be an insert or update; to update, the argument
     * node must have been retrieved from persistence so it has the right Entity.id 
     * value. This method may modify the Entity.metaChecksum and the Entity.lastModified
     * values.
     * 
     * @param node the node to insert or update
     * @return the possibly modified node
     * @throws NodeNotSupportedException
     * @throws TransientException 
     */
    public Node put(Node node) throws NodeNotSupportedException, TransientException {
        // TODO: assign node.parentID here and remove from NodeDAO?
        // TODO: assign DataNode.storageID here -- only when null aka new DataNode?
        throw new UnsupportedOperationException();
    }

    /**
     * This can be done via put(Node) so probably obsolete. TBD.
     * @param node
     * @param list
     * @return
     * @throws TransientException 
     */
    public Node updateProperties(Node node, List<NodeProperty> list) throws TransientException {
        throw new UnsupportedOperationException();
    }

    /**
     * Delete the specified node.
     * 
     * @param node the node to delete
     * @throws TransientException 
     */
    public void delete(Node node) throws TransientException {
        throw new UnsupportedOperationException();
    }

}

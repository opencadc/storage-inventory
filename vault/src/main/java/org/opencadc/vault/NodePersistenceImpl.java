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
import ca.nrc.cadc.util.MultiValuedProperties;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.db.NodeDAO;

/**
 *
 * @author pdowler
 */
public class NodePersistenceImpl { //implements NodePersistence {
    private static final Logger log = Logger.getLogger(NodePersistenceImpl.class);

    private final Map<String,Object> nodeDaoConfig = new TreeMap<>();
    private final ContainerNode root;
    private final ContainerNode trash;
    private final Namespace storageNamespace;
    private final Set<URI> nonWritableProps = new TreeSet<>();
    private final Set<URI> rootAdminProps = new TreeSet<>();
    private final Set<URI> directNodeProps = new TreeSet<>();
    
    public NodePersistenceImpl() {
        MultiValuedProperties config = VaultInitAction.getConfig();
        String dataSourceName = VaultInitAction.JNDI_DATASOURCE;
        String inventorySchema = config.getFirstPropertyValue(VaultInitAction.INVENTORY_SCHEMA_KEY);
        String vospaceSchema = config.getFirstPropertyValue(VaultInitAction.VOSPACE_SCHEMA_KEY);
        nodeDaoConfig.put(SQLGenerator.class.getName(), SQLGenerator.class);
        nodeDaoConfig.put("jndiDataSourceName", dataSourceName);
        nodeDaoConfig.put("schema", inventorySchema);
        nodeDaoConfig.put("vosSchema", vospaceSchema);
        
        
        UUID rootID = new UUID(0L, 0L);
        this.root = new ContainerNode(rootID, "", false);
        String owner = config.getFirstPropertyValue(VaultInitAction.ROOT_OWNER);
        //root.owner = ?? // subject needed to authorize admin requests
        //root.ownerID not needed
        
        // TODO: do this setup in a txn with a lock on something
        NodeDAO dao = getDAO();
        ContainerNode tn = (ContainerNode) dao.get(root, ".trash");
        if (tn == null) {
            tn = new ContainerNode(".trash", false);
            tn.ownerID = root.ownerID;
            tn.owner = root.owner;
            tn.isPublic = false;
            dao.put(tn);
        }
        this.trash = tn;
        
        String ns = config.getFirstPropertyValue(VaultInitAction.STORAGE_NAMESPACE_KEY);
        this.storageNamespace = new Namespace(ns);

        // node properties that match immutable Artifact fields
        nonWritableProps.add(VOS.PROPERTY_URI_CONTENTLENGTH); // data nodes
        nonWritableProps.add(VOS.PROPERTY_URI_CONTENTMD5);    // data nodes
        nonWritableProps.add(VOS.PROPERTY_URI_CREATION_DATE); // no touch
        
        // computed properties
        nonWritableProps.add(VOS.PROPERTY_URI_WRITABLE);  // prediction for current caller 
        
        // props only the root admin can modify
        rootAdminProps.add(VOS.PROPERTY_URI_AVAILABLESPACE);
        rootAdminProps.add(VOS.PROPERTY_URI_CONTENTLENGTH); // container nodes
        rootAdminProps.add(VOS.PROPERTY_URI_QUOTA);
        rootAdminProps.add(VOS.PROPERTY_URI_CREATOR); // owner
        
        // props that are stored as Node fields
        directNodeProps.add(VOS.PROPERTY_URI_GROUPREAD);
        directNodeProps.add(VOS.PROPERTY_URI_GROUPWRITE);
        directNodeProps.add(VOS.PROPERTY_URI_INHERIT_PERMISSIONS);
        directNodeProps.add(VOS.PROPERTY_URI_ISLOCKED);
        directNodeProps.add(VOS.PROPERTY_URI_ISPUBLIC);
        
    }
    
    private NodeDAO getDAO() {
        NodeDAO instance = new NodeDAO();
        instance.setConfig(nodeDaoConfig);
        return instance;
    }
    
    private URI generateStorageID() {
        UUID id = UUID.randomUUID();
        URI ret = URI.create(storageNamespace.getNamespace() + id.toString());
        return ret;
    }

    /**
     * Get the container node that represents the root of all other nodes. 
     * This container node is used to navigate a path (from the root) using
     * <code>get(ContainerNode parent, String name)</code>.
     * 
     * @return the root container node
     */
    public ContainerNode getRootNode() {
        return root;
    }
    
    /**
     * Get a node by name. Concept: The caller uses this to navigate the path from the root
     * node to the target, checking permissions and deciding what to do about 
     * LinkNode(s) along the way.
     * 
     * @param parent parent node, may be special root node but not null
     * @param name relative name of the child node
     * @return the child node or null if it does not exist
     * @throws TransientException 
     */
    public Node get(ContainerNode parent, String name) throws TransientException {
        if (parent == null || name == null) {
            throw new IllegalArgumentException("args cannot be null: parent, name");
        }
        NodeDAO dao = getDAO();
        Node ret = dao.get(parent, name);
        return ret;
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
        if (parent == null) {
            throw new IllegalArgumentException("arg cannot be null: parent");
        }
        NodeDAO dao = getDAO();
        ResourceIterator<Node> ret = dao.iterator(parent, limit, start);
        return ret;
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
        if (node == null) {
            throw new IllegalArgumentException("arg cannot be null: node");
        }
        if (node.parentID == null) {
            if (node.parent == null) {
                throw new RuntimeException("BUG: cannot persist node without parent: " + node);
            }
            node.parentID = node.parent.getID();
        }
        if (node instanceof DataNode) {
            DataNode dn = (DataNode) node;
            if (dn.storageID == null) {
                // new data node? if lastModified is assigned, this looks sketchy
                if (dn.getLastModified() != null) {
                    throw new RuntimeException(
                        "BUG: attempt to put a previously stored DataNode without persistent storageID: "
                            + dn.getID() + " aka " + dn);
                }
                // concept: use a persistent storageID in the node that resolves to a a file
                // once someone puts the file to minoc, so Node.storageID == Artifact.uri
                // but the artifact may or may not exist
                dn.storageID = generateStorageID();
            }
        }
        NodeDAO dao = getDAO();
        dao.put(node);
        return node;
    }

    /**
     * Update properties of a node. This method is responsible for accepting/rejecting changes
     * based on whether a node property is read-only or writable by the caller (implementation
     * specific).
     * 
     * @param node the node to update
     * @param props new property values to set
     * @return the modified node
     * @throws TransientException 
     */
    public Node updateProperties(Node node, List<NodeProperty> props) throws TransientException {
        if (node == null || props == null) {
            throw new IllegalArgumentException("args cannot be null: node, props");
        }
        
        // merge props -> node and/or node.properties
        for (NodeProperty np : props) {
            if (nonWritableProps.contains(np.getKey())) {
                log.debug("updateProperties: skip " + np.getKey());
            } else {
                // some props only root admin can modify
                if (rootAdminProps.contains(np.getKey())) {
                    throw new UnsupportedOperationException("TODO: allow admin to modify prop: " + np.getKey());
                }
                
                // some props are directly in the node
                if (VOS.PROPERTY_URI_FORMAT.equals(np.getKey())
                        || VOS.PROPERTY_URI_CONTENTENCODING.equals(np.getKey())) {
                    throw new UnsupportedOperationException("TODO: set mutable artifact prop: " + np.getKey());
                }

                // some props are directly in the artifact
                if (directNodeProps.contains(np.getKey())) {
                    throw new UnsupportedOperationException("TODO: set mutable node prop: " + np.getKey());
                }
                
                // generic key-value props
                if (node.getProperties().contains(np)) {
                    log.debug("updateProperties: remove previous " + np.getKey());
                    node.getProperties().remove(np);
                }
                if (!np.isMarkedForDeletion()) {
                    node.getProperties().add(np);
                }
            }
        }
        
        NodeDAO dao = getDAO();
        dao.put(node);
        return node;
        
    }

    /**
     * Delete the specified node.
     * 
     * @param node the node to delete
     * @throws TransientException 
     */
    public void delete(Node node) throws TransientException {
        if (node == null) {
            throw new IllegalArgumentException("arg cannot be null: node");
        }
        
        NodeDAO dao = getDAO();
        boolean moveToTrash = false;
        if (node instanceof ContainerNode) {
            ContainerNode cn = (ContainerNode) node;
            try (ResourceIterator<Node> iter = dao.iterator(cn, 1, null)) {
                moveToTrash = !iter.hasNext();
            } catch (IOException ex) {
                throw new TransientException("database IO failure", ex);
            }
        }
        
        // TODO: DeletedNodeEvent
        if (moveToTrash) {
            node.parentID = trash.getID();
            dao.put(node);
        } else {
            dao.delete(node.getID());
        }
        
    }

}

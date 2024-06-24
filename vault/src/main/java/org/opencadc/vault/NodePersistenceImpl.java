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

package org.opencadc.vault;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.MultiValuedProperties;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.PreauthKeyPair;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.PreauthKeyPairDAO;
import org.opencadc.permissions.TokenTool;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.db.NodeDAO;
import org.opencadc.vospace.io.NodeWriter;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.Views;
import org.opencadc.vospace.server.transfers.TransferGenerator;

/**
 *
 * @author pdowler
 */
public class NodePersistenceImpl implements NodePersistence {
    private static final Logger log = Logger.getLogger(NodePersistenceImpl.class);

    private static final Set<URI> ADMIN_PROPS = new TreeSet<>(
        Arrays.asList(
            VOS.PROPERTY_URI_CREATOR,
            VOS.PROPERTY_URI_QUOTA
        )
    );
    
    private static final Set<URI> IMMUTABLE_PROPS = new TreeSet<>(
        Arrays.asList(
            VOS.PROPERTY_URI_AVAILABLESPACE,
            VOS.PROPERTY_URI_CONTENTLENGTH,
            VOS.PROPERTY_URI_CONTENTMD5,
            VOS.PROPERTY_URI_CONTENTDATE,
            VOS.PROPERTY_URI_CREATOR,
            VOS.PROPERTY_URI_DATE,
            VOS.PROPERTY_URI_QUOTA
        )
    );
    
    private static final Set<URI> ARTIFACT_PROPS = new TreeSet<>(
        Arrays.asList(
            // immutable
            VOS.PROPERTY_URI_CONTENTLENGTH,
            VOS.PROPERTY_URI_CONTENTMD5,
            VOS.PROPERTY_URI_CONTENTDATE,
            VOS.PROPERTY_URI_DATE,
            // mutable
            VOS.PROPERTY_URI_CONTENTENCODING,
            VOS.PROPERTY_URI_TYPE
        )
    );
    
    private final Map<String,Object> nodeDaoConfig;
    private final Map<String,Object> invDaoConfig;
    private final Map<String,Object> kpDaoConfig;
    private final boolean singlePool;
    
    private ContainerNode root;
    private final List<ContainerNode> allocationParents = new ArrayList<>();
    private final Namespace storageNamespace;
    
    private final boolean localGroupsOnly;
    private final URI resourceID;
    public final boolean preventNotFound; // GetAction needs to see this
    
    final String appName; // access by VaultTransferGenerator
    
    // possibly temporary hack so migration tool can set this to false and
    // preserve lastModified timestamps on nodes
    public boolean nodeOrigin = true;
    
    public NodePersistenceImpl(URI resourceID, String appName) {
        if (resourceID == null) {
            throw new IllegalArgumentException("resource ID required");
        }
        this.resourceID = resourceID;
        this.appName = appName;
        
        MultiValuedProperties config = VaultInitAction.getConfig();
        this.nodeDaoConfig = VaultInitAction.getDaoConfig(config);
        this.invDaoConfig = VaultInitAction.getInvConfig(config);
        this.kpDaoConfig = VaultInitAction.getKeyPairConfig(config);
        this.singlePool = nodeDaoConfig.get("jndiDataSourceName").equals(invDaoConfig.get("jndiDataSourceName"));
        this.localGroupsOnly = false;
        
        initRootNode();

        String ns = config.getFirstPropertyValue(VaultInitAction.STORAGE_NAMESPACE_KEY);
        this.storageNamespace = new Namespace(ns);

        String pnf = config.getFirstPropertyValue(VaultInitAction.PREVENT_NOT_FOUND_KEY);
        if (pnf != null) {
            this.preventNotFound = Boolean.valueOf(pnf);
            log.debug("Using consistency strategy: " + this.preventNotFound);
        } else {
            throw new IllegalStateException("invalid config: missing/invalid preventNotFound configuration");
        }
    }
    
    private Subject getRootOwner(MultiValuedProperties mvp, IdentityManager im) {
        final String owner = mvp.getFirstPropertyValue(VaultInitAction.ROOT_OWNER);
        if (owner == null) {
            throw new InvalidConfigException(VaultInitAction.ROOT_OWNER + " cannot be null");
        }
        Subject ret = new Subject();
        ret.getPrincipals().add(new HttpPrincipal(owner));
        return im.augment(ret);
    }

    private void initRootNode() {
        if (root != null) {
            return;
        }
        
        // if the init from VaultInitAction failed, this could be called by multiple threads in parallel
        // so let's not make a mess of the state
        synchronized (this) {
            // recheck
            if (root != null) {
                return;
            }
            try {
                MultiValuedProperties config = VaultInitAction.getConfig();
                IdentityManager identityManager = AuthenticationUtil.getIdentityManager();
                UUID rootID = new UUID(0L, 0L);
                ContainerNode rn = new ContainerNode(rootID, "");
                rn.owner = getRootOwner(config, identityManager);
                rn.ownerDisplay = identityManager.toDisplayString(rn.owner);
                log.info("ROOT owner: " + rn.owner);
                rn.ownerID = identityManager.toOwner(rn.owner);
                rn.isPublic = true;
                rn.inheritPermissions = false;

                // allocations
                List<ContainerNode> aps = new ArrayList<>();
                for (String ap : VaultInitAction.getAllocationParents(config)) {
                    if (ap.isEmpty()) {
                        // allocations are in root
                        aps.add(rn);
                        log.info("allocationParent: /");
                    } else {
                        try {

                            // simple top-level names only
                            ContainerNode cn = (ContainerNode) get(rn, ap);
                            String str = "";
                            if (cn == null) {
                                cn = new ContainerNode(ap);
                                cn.parent = rn;
                                cn.isPublic = true;
                                cn.inheritPermissions = false;
                                cn.owner = rn.owner;
                                str = "created/";
                                put(cn);
                            }
                            aps.add(cn);
                            log.info(str + "loaded allocationParent: /" + cn.getName());
                        } catch (NodeNotSupportedException bug) {
                            throw new RuntimeException("BUG: failed to update isPublic=true on allocationParent " + ap, bug);
                        }
                    }
                }

                // success
                this.root = rn;
                this.allocationParents.addAll(aps);
            } catch (Exception ex) {
                log.error("failed to init ROOT ContainerNode", ex);

            }
        }
    }

    @Override
    public Views getViews() {
        return new Views();
    }

    @Override
    public VaultTransferGenerator getTransferGenerator() {
        PreauthKeyPairDAO keyDAO = new PreauthKeyPairDAO();
        keyDAO.setConfig(kpDaoConfig);
        PreauthKeyPair kp = keyDAO.get(VaultInitAction.KEY_PAIR_NAME);
        TokenTool tt = new TokenTool(kp.getPublicKey(), kp.getPrivateKey());
        return new VaultTransferGenerator(this, appName, getArtifactDAO(), tt, preventNotFound);
    }
    
    private NodeDAO getDAO() {
        NodeDAO instance = new NodeDAO(nodeOrigin);
        instance.setConfig(nodeDaoConfig);
        return instance;
    }
    
    private ArtifactDAO getArtifactDAO() {
        ArtifactDAO instance = new ArtifactDAO(true); // origin==true?
        instance.setConfig(invDaoConfig);
        return instance;
    }
    
    private URI generateStorageID() {
        UUID id = UUID.randomUUID();
        URI ret = URI.create(storageNamespace.getNamespace() + id.toString());
        return ret;
    }

    @Override
    public URI getResourceID() {
        return resourceID;
    }

    /**
     * Get the container node that represents the root of all other nodes. 
     * This container node is used to navigate a path (from the root) using
     * <code>get(ContainerNode parent, String name)</code>.
     * 
     * @return the root container node
     */
    @Override
    public ContainerNode getRootNode() {
        initRootNode();
        return root;
    }

    @Override
    public boolean isAllocation(ContainerNode cn) {
        if (cn.parent == null) {
            return false; // root is never an allocation
        }
        ContainerNode p = cn.parent;
        for (ContainerNode ap : allocationParents) {
            if (p.getID().equals(ap.getID())) {
                return true;
            }
        }
        return false;
    }
    
    private boolean absoluteEquals(ContainerNode c1, ContainerNode c2) {
        // note: cavern does not use/preserve Node.id except for root
        if (!c1.getName().equals(c2.getName())) {
            return false;
        }
        // same name, check parents
        if (c1.parent == null && c2.parent == null) {
            // both root
            return true;
        }
        if (c1.parent == null || c2.parent == null) {
            // one is root
            return false;
        }
        return absoluteEquals(c1.parent, c2.parent);
    }

    @Override
    public Set<URI> getAdminProps() {
        return Collections.unmodifiableSet(ADMIN_PROPS);
    }
    
    @Override
    public Set<URI> getImmutableProps() {
        return Collections.unmodifiableSet(IMMUTABLE_PROPS);
    }

    /**
     * Get a node by name. Concept: The caller uses this to navigate the path 
     * from the root node to the target, checking permissions and deciding what 
     * to do about LinkNode(s) along the way.
     * 
     * @param parent parent node, may be special root node but not null
     * @param name relative name of the child node
     * @return the child node or null if it does not exist
     * @throws TransientException 
     */
    @Override
    public Node get(ContainerNode parent, String name) throws TransientException {
        if (parent == null || name == null) {
            throw new IllegalArgumentException("args cannot be null: parent, name");
        }
        NodeDAO dao = getDAO();
        Node ret = dao.get(parent, name);
        if (ret == null) {
            return null;
        }
        
        // in principle we could have queried vospace.Node join inventory.Artifact above
        // and avoid this query.... simplicity for now
        if (ret instanceof DataNode) {
            DataNode dn = (DataNode) ret;
            ArtifactDAO artifactDAO = getArtifactDAO();
            Artifact a = artifactDAO.get(dn.storageID);
            DateFormat df = NodeWriter.getDateFormat();
            if (a != null) {
                // DataNode.bytesUsed is an optimization (cache): 
                // if DataNode.bytesUsed != Artifact.contentLength we update the cache
                // this retains put+get consistency in a single-site deployed (with minoc)
                // and may help hide some inconsistencies in child listing sizes
                if (!a.getContentLength().equals(dn.bytesUsed)) {
                    TransactionManager txn = dao.getTransactionManager();
                    try {
                        log.debug("starting node transaction");
                        txn.startTransaction();
                        log.debug("start txn: OK");
            
                        DataNode locked = (DataNode) dao.lock(dn);
                        if (locked != null) {
                            dn = locked; // safer than accidentally using the wrong variable
                            dn.bytesUsed = a.getContentLength();
                            dao.put(dn);
                            ret = dn;
                        }

                        log.debug("commit txn...");
                        txn.commitTransaction();
                        log.debug("commit txn: OK");
                        if (locked == null) {
                            return null; // gone
                        }
                    } catch (Exception ex) {
                        if (txn.isOpen()) {
                            log.error("failed to update bytesUsed on " + dn.getID() + " aka " + dn.getName(), ex);
                            txn.rollbackTransaction();
                            log.debug("rollback txn: OK");
                        }
                    } finally {
                        if (txn.isOpen()) {
                            log.error("BUG - open transaction in finally");
                            txn.rollbackTransaction();
                            log.error("rollback txn: OK");
                        }
                    }
                }
                
                Date d = ret.getLastModified();
                Date cd = null;
                if (ret.getLastModified().before(a.getLastModified())) {
                    d = a.getLastModified();
                }
                if (d.before(a.getContentLastModified())) {
                    // probably not possible
                    d = a.getContentLastModified();
                } else {
                    cd = a.getContentLastModified();
                }
                ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_DATE, df.format(d)));
                if (cd != null) {
                    ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CONTENTDATE, df.format(cd)));
                }
                
                // assume MD5
                ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CONTENTMD5, a.getContentChecksum().getSchemeSpecificPart()));
                
                if (a.contentEncoding != null) {
                    ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CONTENTENCODING, a.contentEncoding));
                }
                if (a.contentType != null) {
                    ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_TYPE, a.contentType));
                }
            }
            if (dn.bytesUsed == null) {
                dn.bytesUsed = 0L; // no data stored
            }
        }
        
        ret.parent = parent;
        IdentityManager identityManager = AuthenticationUtil.getIdentityManager();
        ret.owner = identityManager.toSubject(ret.ownerID);
        ret.ownerDisplay = identityManager.toDisplayString(ret.owner);
        
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
    @Override
    public ResourceIterator<Node> iterator(ContainerNode parent, Integer limit, String start) {
        if (parent == null) {
            throw new IllegalArgumentException("arg cannot be null: parent");
        }
        NodeDAO dao = getDAO();
        ResourceIterator<Node> ret = dao.iterator(parent, limit, start);
        return new ChildNodeWrapper(parent, ret);
    }
    
    // wrapper to add parent, owner, and props to child nodes
    private class ChildNodeWrapper implements ResourceIterator<Node> {

        private final ContainerNode parent;
        private ResourceIterator<Node> childIter;
        
        private final IdentityManager identityManager = AuthenticationUtil.getIdentityManager();
        private final Map<Object, Subject> identCache = new TreeMap<>();
        
        ChildNodeWrapper(ContainerNode parent, ResourceIterator<Node> childIter) {
            this.parent = parent;
            this.childIter = childIter;
            // prime cache with caller
            Subject caller = AuthenticationUtil.getCurrentSubject();
            if (caller != null) {
                Object ownerID = identityManager.toOwner(caller);
                if (ownerID != null) {
                    // HACK: NodeDAO returns ownerID as String and relies on the IM
                    // to convert to a number (eg)
                    identCache.put(ownerID.toString(), caller);
                }
            }
        }
        
        @Override
        public boolean hasNext() {
            if (childIter != null) {
                return childIter.hasNext();
            }
            return false;
        }

        @Override
        public Node next() {
            if (childIter == null) {
                throw new NoSuchElementException("iterator closed");
            }
            Node ret = childIter.next();
            ret.parent = parent;

            // owner
            Subject s = identCache.get(ret.ownerID);
            if (s == null) {
                s = identityManager.toSubject(ret.ownerID);
                identCache.put(ret.ownerID, s);
            }
            ret.owner = s;
            ret.ownerDisplay = identityManager.toDisplayString(ret.owner);

            if (ret instanceof DataNode) {
                DataNode dn = (DataNode) ret;
                if (dn.bytesUsed == null) {
                    dn.bytesUsed = 0L;
                }
            }
            return ret;
        }

        @Override
        public void close() throws IOException {
            if (childIter != null) {
                // silently OK to close more than once
                childIter.close();
                childIter = null;
            }
            identCache.clear();
        }
        
    }

    /**
     * Load additional node properties for the specified node. Note: this may not be 
     * necessary and may be removed. TBD.
     * 
     * @param node
     * @throws TransientException 
     */
    @Override
    public void getProperties(Node node) throws TransientException {
        // no-op
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
    @Override
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
        if (node.ownerID == null) {
            if (node.owner == null) {
                throw new RuntimeException("BUG: cannot persist node without owner: " + node);
            }
            IdentityManager identityManager = AuthenticationUtil.getIdentityManager();
            node.ownerID = identityManager.toOwner(node.owner);
        }

        if (localGroupsOnly) {
            if (!node.getReadOnlyGroup().isEmpty() || !node.getReadWriteGroup().isEmpty()) {
                LocalAuthority loc = new LocalAuthority();
                try {
                    URI localGMS = loc.getServiceURI(Standards.GMS_SEARCH_10.toASCIIString());
                    StringBuilder serr = new StringBuilder("non-local groups:");
                    int len = serr.length();
                    for (GroupURI g : node.getReadOnlyGroup()) {
                        if (!localGMS.equals(g.getServiceID())) {
                            serr.append(" ").append(g.getURI().toASCIIString());
                        }
                    }
                    for (GroupURI g : node.getReadWriteGroup()) {
                        if (!localGMS.equals(g.getServiceID())) {
                            serr.append(" ").append(g.getURI().toASCIIString());
                        }
                    }
                    String err = serr.toString();
                    if (err.length() > len) {
                        throw new IllegalArgumentException(err);
                    }
                } catch (NoSuchElementException ex) {
                    throw new RuntimeException("CONFIG: localGroupOnly policy && local GMS service not configured");
                }
            }
        }
        
        NodeProperty contentType = null;
        NodeProperty contentEncoding = null;
        // need to remove all artifact props from the node.getProperties()
        // and use artifactDAO to set the mutable ones
        Iterator<NodeProperty> i = node.getProperties().iterator();
        while (i.hasNext()) {
            NodeProperty np = i.next();
            if (VOS.PROPERTY_URI_TYPE.equals(np.getKey())) {
                contentType = np;
            } else if (VOS.PROPERTY_URI_CONTENTENCODING.equals(np.getKey())) {
                contentEncoding = np;
            }

            if (ARTIFACT_PROPS.contains(np.getKey())) {
                i.remove();
            }
        }
            
        ArtifactDAO artifactDAO = null;
        Artifact a = null;
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
            } else {
                if (contentType != null || contentEncoding != null) {
                    // update possibly required
                    artifactDAO = getArtifactDAO();
                    a = artifactDAO.get(dn.storageID);
                } else {
                    log.debug("no artifact props to update - skipping ArtifactDAO.get");
                }
            }
        }
            
        boolean useTxn = singlePool && a != null; // TODO
        
        // update node
        NodeDAO dao = getDAO();
        dao.put(node);
        
        // update artifact after node
        if (a != null) {
            if (contentType == null || contentType.isMarkedForDeletion()) {
                a.contentType = null;
            } else {
                a.contentType = contentType.getValue();
            }
            if (contentEncoding == null || contentEncoding.isMarkedForDeletion()) {
                a.contentEncoding = null;
            } else {
                a.contentEncoding = contentEncoding.getValue();
            }
            artifactDAO.put(a);
        
            // re-add node props
            if (contentType != null && !contentType.isMarkedForDeletion()) {
                node.getProperties().add(contentType);
            }
            if (contentEncoding != null && !contentEncoding.isMarkedForDeletion()) {
                node.getProperties().add(contentEncoding);
            }
        }
        return node;
    }

    @Override
    public void move(Node node, ContainerNode dest, String newName) {
        if (node == null || dest == null) {
            throw new IllegalArgumentException("args cannot be null");
        }
        if (node.parent == null || dest.parent == null) {
            throw new IllegalArgumentException("args must both be peristent nodes before move");
        }
        // try to detect attempt to disconnect from path to root: node is a parent of dest
        ContainerNode cur = dest;
        while (!cur.getID().equals(root.getID())) {
            cur = cur.parent;
            if (cur.getID().equals(node.getID())) {
                throw new IllegalArgumentException("invalid destination for move: " + node.getID() + " -> " + dest.getID());
            }
            
        }
        
        NodeDAO dao = getDAO();
        TransactionManager txn = dao.getTransactionManager();
        try {
            log.debug("starting node transaction");
            txn.startTransaction();
            log.debug("start txn: OK");
            
            // lock the source node
            Node locked = dao.lock(node);
            if (locked != null) {      
                node = locked; // safer than having two vars and accidentally using the wrong one
                Subject caller = AuthenticationUtil.getCurrentSubject();
                node.owner = caller;
                node.ownerID = null;
                node.ownerDisplay = null;
                node.parent = dest;
                node.parentID = null;
                if (newName != null) {
                    node.setName(newName);
                }
                Node result = put(node);
                log.debug("moved: " + result);
            }
            log.debug("commit txn...");
            txn.commitTransaction();
            log.debug("commit txn: OK");
        } catch (Exception ex) {
            if (txn.isOpen()) {
                log.error("failed to move " + node.getID() + " aka " + node.getName(), ex);
                txn.rollbackTransaction();
                log.debug("rollback txn: OK");
            }
        } finally {
            if (txn.isOpen()) {
                log.error("BUG - open transaction in finally");
                txn.rollbackTransaction();
                log.error("rollback txn: OK");
            }
        }
    }

    /**
     * Delete the specified node.
     * 
     * @param node the node to delete
     * @throws TransientException 
     */
    @Override
    public void delete(Node node) throws TransientException {
        if (node == null) {
            throw new IllegalArgumentException("arg cannot be null: node");
        }
        
        Artifact a = null;
        final NodeDAO dao = getDAO();
        final ArtifactDAO artifactDAO = getArtifactDAO();
        TransactionManager txn = dao.getTransactionManager();
        TransactionManager atxn = null;
        try {
            if (node instanceof DataNode) {
                DataNode dn = (DataNode) node;
                a = artifactDAO.get(dn.storageID);
            }
            if (a != null && !singlePool) {
                atxn = artifactDAO.getTransactionManager();
            }
            
            log.debug("starting node transaction");
            txn.startTransaction();
            log.debug("start txn: OK");
            
            Node locked = dao.lock(node);
            if (locked != null) {      
                node = locked; // safer than having two vars and accidentally using the wrong one
                URI storageID = null;
                if (node instanceof ContainerNode) {
                    ContainerNode cn = (ContainerNode) node;
                    boolean empty = dao.isEmpty(cn);
                    if (!empty) {
                        log.debug("commit txn...");
                        txn.commitTransaction();
                        log.debug("commit txn: OK");
                        throw new IllegalArgumentException("container node '" + node.getName() + "' is not empty");
                    }
                } else if (node instanceof DataNode) {
                    DataNode dn = (DataNode) node;
                    if (dn.bytesUsed != null) {
                        // artifact exists
                        storageID = dn.storageID;
                    }
                } // else: LinkNode can always be deleted

                if (singlePool && a != null) {
                    // inventory ops inside main txn
                    DeletedArtifactEventDAO daeDAO = new DeletedArtifactEventDAO(artifactDAO);
                    DeletedArtifactEvent dae = new DeletedArtifactEvent(a.getID());
                    daeDAO.put(dae);
                    artifactDAO.delete(a.getID());
                }
                
                // TODO: need DeletedNodeDAO to create DeletedNodeEvent
                dao.delete(node.getID());
            } else {
                log.debug("failed to lock node " + node.getID() + " - assume deleted by another process");
            }

            log.debug("commit txn...");
            txn.commitTransaction();
            log.debug("commit txn: OK");

            if (!singlePool && a != null) {
                log.debug("starting artifact transaction");
                atxn.startTransaction();
                log.debug("start txn: OK");
            
                Artifact alock = artifactDAO.lock(a);
                if (alock != null) {
                    DeletedArtifactEventDAO daeDAO = new DeletedArtifactEventDAO(artifactDAO);
                    DeletedArtifactEvent dae = new DeletedArtifactEvent(alock.getID());
                    daeDAO.put(dae);
                    artifactDAO.delete(alock.getID());
                }
                log.debug("commit artifact txn...");
                atxn.commitTransaction();
                atxn = null;
                log.debug("commit artifact txn: OK");
            }
        } catch (Exception ex) {
            if (txn.isOpen()) {
                log.error("failed to delete " + node.getID() + " aka " + node.getName(), ex);
                txn.rollbackTransaction();
                log.debug("rollback txn: OK");
            }
            if (atxn != null && atxn.isOpen()) {
                log.error("failed to delete " + a.getID() + " aka " + a.getURI(), ex);
                atxn.rollbackTransaction();
                log.debug("rollback artifact txn: OK");
            }
            throw ex;
        } finally {
            if (txn.isOpen()) {
                log.error("BUG - open transaction in finally");
                txn.rollbackTransaction();
                log.error("rollback txn: OK");
            }
            if (atxn != null && atxn.isOpen()) {
                log.error("BUG - open artifact transaction in finally");
                atxn.rollbackTransaction();
                log.error("rollback artifact txn: OK");
            }
        }
    }
    
    // needed by vault-migrate to configure a HarvestStateDAO for delete processing
    public Map<String, Object> getNodeDaoConfig() {
        return nodeDaoConfig;
    }
}

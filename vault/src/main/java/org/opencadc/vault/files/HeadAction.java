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

package org.opencadc.vault.files;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.rest.Version;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.vault.NodePersistenceImpl;
import org.opencadc.vault.VaultTransferGenerator;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeWriter;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.Utils;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;

/**
 * Class to handle a HEAD request to a DataNode
 * @author adriand
 */
public class HeadAction extends RestAction {
    protected static Logger log = Logger.getLogger(HeadAction.class);

    protected VOSpaceAuthorizer voSpaceAuthorizer;
    protected NodePersistenceImpl nodePersistence; // need impl in GetAction
    protected LocalServiceURI localServiceURI;

    public HeadAction() {
        super();
    }

    protected class ResolvedNode {
        DataNode node;
        Artifact artifact;
        List<Protocol> protos;
    }

    @Override
    protected final InlineContentHandler getInlineContentHandler() {
        return null;
    }

    @Override
    protected String getServerImpl() {
        // no null version checking because fail to build correctly can't get past basic testing
        Version v = getVersionFromResource();
        return "storage-inventory/vault-" + v.getMajorMinor();
    }
    
    @Override
    public void initAction() throws Exception {
        String jndiNodePersistence = super.appName + "-" + NodePersistence.class.getName();
        try {
            Context ctx = new InitialContext();
            this.nodePersistence = ((NodePersistenceImpl) ctx.lookup(jndiNodePersistence));
            this.voSpaceAuthorizer = new VOSpaceAuthorizer(nodePersistence);
            localServiceURI = new LocalServiceURI(nodePersistence.getResourceID());
        } catch (Exception oops) {
            throw new RuntimeException("BUG: NodePersistence implementation not found with JNDI key " + jndiNodePersistence, oops);
        }

        checkReadable();
    }

    @Override
    public void doAction() throws Exception {
        resolveAndSetMetadata(true);
    }

    ResolvedNode resolveAndSetMetadata(boolean includeContentLength) throws Exception {
        PathResolver pathResolver = new PathResolver(nodePersistence, voSpaceAuthorizer);
        String filePath = syncInput.getPath();
        Node node = pathResolver.getNode(filePath, true);

        if (node == null) {
            throw new ResourceNotFoundException("Target not found: " + filePath);
        }

        if (!(node instanceof DataNode)) {
            throw new IllegalArgumentException("Resolved target is not a data node: " + Utils.getPath(node));
        }
        
        log.debug("node path resolved: " + node.getName() + " type: " + node.getClass().getName());

        syncOutput.setHeader("Content-Disposition", "inline; filename=\"" + node.getName() + "\"");
        
        ResolvedNode ret = new ResolvedNode();
        ret.node = (DataNode) node;
        DataNode dn = ret.node;
        
        // determine if data node is up to date or we need to find artifact
        if (nodePersistence.preventNotFound && (dn.bytesUsed == null || dn.bytesUsed == 0)) {
            VOSURI targetURI = localServiceURI.getURI(node);
            Transfer pullTransfer = new Transfer(targetURI.getURI(), Direction.pullFromVoSpace);
            pullTransfer.version = VOS.VOSPACE_21;
            pullTransfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTPS_GET)); // anon, preauth

            VaultTransferGenerator tg = nodePersistence.getTransferGenerator();
            ret.protos = tg.getEndpoints(targetURI, pullTransfer, null);
            ret.artifact = tg.resolvedArtifact;
        }
        
        URI contentChecksum = null;
        String contentMD5 = node.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5);
        if (contentMD5 != null) {
            contentChecksum = URI.create("md5:" + contentMD5);
        }
        String contentLength = null;
        if (dn.bytesUsed != null) {
            contentLength = dn.bytesUsed.toString();
        }
        String contentType = node.getPropertyValue(VOS.PROPERTY_URI_TYPE);
        String contentEncoding = node.getPropertyValue(VOS.PROPERTY_URI_CONTENTENCODING);
        String clm = node.getPropertyValue(VOS.PROPERTY_URI_CONTENTDATE); // use if present
        if (clm == null) {
            clm = node.getPropertyValue(VOS.PROPERTY_URI_DATE);
        }
        Date contentLastModified = null;
        if (clm != null) {
            contentLastModified = NodeWriter.getDateFormat().parse(clm);
        }
        if (ret.artifact != null) {
            // preventNotFound
            contentChecksum = ret.artifact.getContentChecksum();
            contentLength = ret.artifact.getContentLength().toString();
            contentLastModified = ret.artifact.getContentLastModified();
            contentType = ret.artifact.contentType;
            contentEncoding = ret.artifact.contentEncoding;
        }
        
        if (includeContentLength && contentLength != null) {
            syncOutput.setHeader("Content-Length", contentLength);
        }
        if (contentType != null) {
            syncOutput.setHeader("Content-Type", contentType);
        }
        if (contentEncoding != null) {
            syncOutput.setHeader("Content-Encoding", contentEncoding);
        }
        if (contentLastModified != null) {
            syncOutput.setLastModified(contentLastModified);
        }
        if (contentChecksum != null) {
            syncOutput.setDigest(contentChecksum);
        }
        syncOutput.setHeader("Accept-Ranges", "bytes");

        syncOutput.setCode(200);
        return ret;
    }

}

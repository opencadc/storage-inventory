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

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.TransientException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Iterator;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vault.VaultTransferGenerator;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.NodeFault;
import org.opencadc.vospace.server.Utils;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;

/**
 * Class to redirect to a storage URL from which the content of a DataNode can be downloaded
 * @author adriand
 */
public class GetAction extends HeadAction {
    protected static Logger log = Logger.getLogger(GetAction.class);

    public GetAction() {
        super();
    }

    @Override
    public void doAction() throws Exception {
        // don't set content-length header since we plan to redirect
        ResolvedNode rn = resolveAndSetMetadata(false);
        DataNode node = rn.node;

        Subject caller = AuthenticationUtil.getCurrentSubject();
        if (!voSpaceAuthorizer.hasSingleNodeReadPermission(node, caller)) {
            // TODO: should output requested vos URI here
            throw NodeFault.PermissionDenied.getStatus(syncInput.getPath());
        }
            
        boolean noArtifact = node.bytesUsed == null || node.bytesUsed == 0L;
        noArtifact = noArtifact && nodePersistence.preventNotFound && rn.artifact == null;
        if (noArtifact) {
            // no artifact found by preventNotFound
            syncOutput.setCode(HttpURLConnection.HTTP_NO_CONTENT);
            return;
        }

        if (rn.protos == null) {
            VOSURI targetURI = localServiceURI.getURI(node);
            Transfer pullTransfer = new Transfer(targetURI.getURI(), Direction.pullFromVoSpace);
            pullTransfer.version = VOS.VOSPACE_21;
            pullTransfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTPS_GET)); // anon, preauth

            VaultTransferGenerator tg = nodePersistence.getTransferGenerator();
            rn.protos = tg.getEndpoints(targetURI, pullTransfer, null);
            rn.artifact = tg.resolvedArtifact; // currently unused at this point
        }
        // check artifact again (!preventNotFound)
        noArtifact = node.bytesUsed == null || node.bytesUsed == 0L;
        noArtifact = noArtifact && rn.artifact == null;
        if (noArtifact) {
            // no file
            syncOutput.setCode(HttpURLConnection.HTTP_NO_CONTENT);
            return;
        }
        
        if (rn.protos.isEmpty()) {
            throw new TransientException("No location found for file " + Utils.getPath(node));
        }
        Protocol proto = rn.protos.get(0);
        URI redirect = URI.create(proto.getEndpoint());
        String loc = appendParams(redirect);
        log.debug("Location: " + loc);
        syncOutput.setHeader("Location", loc);
        syncOutput.setCode(HttpURLConnection.HTTP_SEE_OTHER);
    }
    
    // pass through request params to minoc
    // TODO: this makes slightly better use of the StringBuilder than the code in raven
    //       should put this method into cadc-inventory-server, static in ProtocolsGenerator?
    String appendParams(URI redirect) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (String param : syncInput.getParameterNames()) {
            Iterator<String> values = syncInput.getParameters(param).iterator();
            while (values.hasNext()) {
                if (sb.length() > 0) {
                    sb.append("&");
                }
                sb.append(param);
                sb.append("=");
                sb.append(URLEncoder.encode(values.next(), "UTF-8"));
            }
        }
        if (sb.length() > 0) {
            if (redirect.getQuery() != null) {
                sb.insert(0, "&");
            } else {
                sb.insert(0, "?");
            }
        }
        sb.insert(0, redirect.toASCIIString());
        return sb.toString();
    }
}

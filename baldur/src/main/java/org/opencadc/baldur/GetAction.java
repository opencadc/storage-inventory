/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2019.                            (c) 2019.
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
 *  $Revision: 5 $
 *
 ************************************************************************
 */

package org.opencadc.baldur;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.permissions.Grant;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.WriteGrant;
import org.opencadc.permissions.xml.GrantWriter;

/**
 * Class to handle the retrieving of read-only or read-write permissions
 * for a given artifact.
 * 
 * @author jburke, majorb
 */
public class GetAction extends RestAction {
    
    private static final Logger log = Logger.getLogger(GetAction.class);

    private static final String PARAM_OP = "OP";
    private static final String PARAM_ID = "ID";
    
    public enum Operation {
        read, 
        write;
    }

    public GetAction() {
        super();
    }
    
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

    @Override
    public void doAction() throws Exception {

        PermissionsConfig permissionsConfig = new PermissionsConfig();
        authorizeRequest(permissionsConfig);
        
        String op = syncInput.getParameter(PARAM_OP);
        String sid = syncInput.getParameter(PARAM_ID);
        log.debug(PARAM_OP + ": " + op);
        log.debug(PARAM_ID + ": " + sid);
        
        if (op == null) {
            throw new IllegalArgumentException("missing required parameter, " + PARAM_OP);
        }
        Operation operation = null;
        try {
            operation = Operation.valueOf(op);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid " + PARAM_OP + " parameter, must be "
                + Operation.read  + " or " + Operation.write);
        }
        if (sid == null) {
            throw new IllegalArgumentException("missing required parameter, " + PARAM_ID);
        }

        URI assetID;
        try {
            assetID = new URI(sid);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("invalid " + PARAM_ID + " parameter, not a valid URI: " + sid);
        }

        Grant grant;
        switch (operation) {
            case read:
                grant = getReadGrant(permissionsConfig, assetID);
                break;
            case write:
                grant = getWriteGrant(permissionsConfig, assetID);
                break;
            default:
                throw new IllegalStateException("unknown operation: " + operation);
        }

        syncOutput.setHeader("Content-Type", "text/xml");
        syncOutput.setCode(200);

        OutputStream out = syncOutput.getOutputStream();
        GrantWriter writer = new GrantWriter();
        writer.write(grant, out);
        out.flush();

        logInfo.setMessage(String.format("%s grants: %s", op, grant.getGroups().size()));
        logInfo.setResource(assetID);
    }

    /**
     * Check that the caller (current subject) is authorized to get grants.
     * 
     * @param permissionsConfig 
     */
    protected void authorizeRequest(PermissionsConfig permissionsConfig) {
        Subject subject = AuthenticationUtil.getCurrentSubject();
        if (subject != null) {
            Set<X500Principal> principals = subject.getPrincipals(X500Principal.class);
            for (Principal p : principals) {
                for (Principal authorizedUser : permissionsConfig.getAuthorizedPrincipals()) {
                    log.debug("authorize: " + p + " vs " + authorizedUser);
                    if (AuthenticationUtil.equals(authorizedUser, p)) {
                        return;
                    }
                }
            }
        }
        throw new AccessControlException("permission denied");
    }
    
    /**
     * Get the read grant for the given Artifact URI. 
     *
     * @param permissionsConfig The grant permissions.
     * @param assetID The Artifact URI.
     * @return The read grant information object for the artifact URI.
     * @throws ca.nrc.cadc.net.ResourceNotFoundException if the assetID is not recognised
     */
    protected ReadGrant getReadGrant(PermissionsConfig permissionsConfig, URI assetID)
        throws ResourceNotFoundException {
        assertNotNull(GetAction.class, "assetID", assetID);

        Iterator<PermissionEntry> matchingEntries = permissionsConfig.getMatchingEntries(assetID);
        if (!matchingEntries.hasNext()) {
            throw new ResourceNotFoundException("not found: read grant for " + assetID.toASCIIString());
        }

        boolean anonymousRead = false;
        List<GroupURI> groups = new ArrayList<GroupURI>();
        PermissionEntry next = null;
        log.debug("compiling read grant from matching entries");
        while (matchingEntries.hasNext()) {
            next = matchingEntries.next();
            log.debug("matching entry: " + next);
            if (!anonymousRead) {
                anonymousRead = next.anonRead;
            }
            for (GroupURI groupURI : next.readOnlyGroups) {
                if (!groups.contains(groupURI)) {
                    groups.add(groupURI);
                }
            }
            for (GroupURI groupURI : next.readWriteGroups) {
                if (!groups.contains(groupURI)) {
                    groups.add(groupURI);
                }
            }
        }
        
        ReadGrant readGrant = new ReadGrant(assetID, permissionsConfig.getExpiryDate(), anonymousRead);
        readGrant.getGroups().addAll(groups);
        return readGrant;
    }
    
    /**
     * Get the write grant for the given Artifact URI. 
     *
     * @param permissionsConfig The grant permissions.
     * @param assetID The Artifact URI.
     * @return The write grant information object for the artifact URI.
     * @throws ca.nrc.cadc.net.ResourceNotFoundException if the assetID is not recognised
     */
    protected WriteGrant getWriteGrant(PermissionsConfig permissionsConfig, URI assetID)
        throws ResourceNotFoundException {
        assertNotNull(GetAction.class, "assetID", assetID);

        Iterator<PermissionEntry> matchingEntries = permissionsConfig.getMatchingEntries(assetID);
        if (!matchingEntries.hasNext()) {
            throw new ResourceNotFoundException("not found: write grant for " + assetID.toASCIIString());
        }

        List<GroupURI> groups = new ArrayList<GroupURI>();
        PermissionEntry next = null;
        log.debug("compiling write grant from matching entries");
        while (matchingEntries.hasNext()) {
            next = matchingEntries.next();
            log.debug("matching entry: " + next);
            for (GroupURI groupURI : next.readWriteGroups) {
                if (!groups.contains(groupURI)) {
                    groups.add(groupURI);
                }
            }
        }
        WriteGrant writeGrant = new WriteGrant(assetID, permissionsConfig.getExpiryDate());
        writeGrant.getGroups().addAll(groups);
        return writeGrant;
    }

    private static void assertNotNull(Class caller, String name, Object test) {
        if (test == null) {
            throw new IllegalArgumentException("invalid " + caller.getSimpleName() + "." + name + ": null");
        }
    }
}

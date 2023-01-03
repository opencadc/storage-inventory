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
 ************************************************************************
 */

package org.opencadc.inventory.server;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import java.net.URI;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupClient;
import org.opencadc.gms.GroupURI;
import org.opencadc.gms.GroupUtil;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.permissions.Grant;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.WriteGrant;
import org.opencadc.permissions.client.PermissionsClient;

/**
 * Queries permission services for read or write permissions for an Artifact.
 */
public class PermissionsCheck {

    private final URI artifactURI;
    private final boolean authenticateOnly;
    private final WebServiceLogInfo logInfo;

    public PermissionsCheck(URI artifactURI, boolean authenticateOnly, WebServiceLogInfo logInfo) {
        InventoryUtil.assertNotNull(PermissionsCheck.class, "artifactURI", artifactURI);
        InventoryUtil.assertNotNull(PermissionsCheck.class, "logInfo", logInfo);
        this.artifactURI = artifactURI;
        this.authenticateOnly = authenticateOnly;
        this.logInfo = logInfo;
    }

    private static final Logger log = Logger.getLogger(PermissionsCheck.class);

    /**
     * Check the given read granting services for read permission to the artifact.
     *
     * @param readGrantServices list of read granting services.
     * @throws AccessControlException if read permission is denied.
     * @throws TransientException if call to permission service fails with transient status code
     */
    public void checkReadPermission(List<URI> readGrantServices)
        throws AccessControlException, TransientException {
        InventoryUtil.assertNotNull(PermissionsCheck.class, "readGrantServices", readGrantServices);

        if (this.authenticateOnly) {
            log.warn("authenticateOnly=true: allowing unrestricted access");
            return;
        }

        Set<GroupURI> granted = new TreeSet<>();
        if (!readGrantServices.isEmpty()) {
            Subject ops = CredUtil.createOpsSubject();
            try {
                List<ReadGrant> grants = Subject.doAs(ops, new GetReadGrantsAction(this.artifactURI, readGrantServices));
                for (ReadGrant g : grants) {
                    if (g.isAnonymousAccess()) {
                        logInfo.setGrant("read: anonymous");
                        return;
                    }
                    granted.addAll(g.getGroups());
                }
            } catch (TransientException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new RuntimeException("unexpected exception calling permissions service(s)", ex);
            }
        }
        

        if (granted.isEmpty()) {
            throw new AccessControlException("permission denied: no read grants for " + this.artifactURI);
        }

        // TODO: add profiling
        // if the granted group list is small, it would be better to use GroupClient.isMember()
        // rather than getting all groups... experiment to determine threshold?
        // unfortunately, the speed of GroupClient.getGroups() will also depend on how many groups the
        // caller belongs to...
        try {
            if (CredUtil.checkCredentials()) {
                LocalAuthority loc = new LocalAuthority();
                URI resourceID = loc.getServiceURI(Standards.GMS_SEARCH_01.toString());
                GroupClient client = GroupUtil.getGroupClient(resourceID);
                List<GroupURI> userGroups = client.getMemberships();
                for (GroupURI gg : granted) {
                    for (GroupURI userGroup : userGroups) {
                        if (gg.equals(userGroup)) {
                            this.logInfo.setGrant("read  " + gg);
                            return;
                        }
                    }
                }
            }
        } catch (CertificateException ex) {
            throw new AccessControlException("permission denied (invalid delegated client certificate)");
        }
        
        throw new AccessControlException("permission denied");
    }

    /**
     * Check the given write granting services for write permission to the artifact.
     *
     * @param writeGrantServices list of write granting services.
     * @throws AccessControlException if write permission is denied.
     * @throws TransientException if call to permission service fails with transient status code
     */
    public void checkWritePermission(List<URI> writeGrantServices)
        throws AccessControlException, TransientException {
        InventoryUtil.assertNotNull(PermissionsCheck.class, "writeGrantServices", writeGrantServices);

        AuthMethod am = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (am != null && am.equals(AuthMethod.ANON)) {
            // never support anon write
            throw new AccessControlException("permission denied");
        }

        if (this.authenticateOnly) {
            log.warn("authenticateOnly=true: allowing unrestricted access");
            return;
        }

        Set<GroupURI> granted = new TreeSet<>();
        if (!writeGrantServices.isEmpty()) {
            Subject ops = CredUtil.createOpsSubject();
            try {
                List<WriteGrant> grants = Subject.doAs(ops, new GetWriteGrantsAction(this.artifactURI, writeGrantServices));
                for (WriteGrant g : grants) {
                    granted.addAll(g.getGroups());
                }
            } catch (Exception ex) {
                throw new RuntimeException("unexpected exception calling permissions service(s)", ex);
            }
        }

        if (granted.isEmpty()) {
            throw new AccessControlException("permission denied: no write grants for " + this.artifactURI);
        }

        // TODO: add profiling
        // if the granted group list is small, it would be better to use GroupClient.isMember()
        // rather than getting all groups... experiment to determine threshold?
        // unfortunately, the speed of GroupClient.getGroups() will also depend on how many groups the
        // caller belongs to...
        try {
            if (CredUtil.checkCredentials()) {
                LocalAuthority loc = new LocalAuthority();
                URI resourceID = loc.getServiceURI(Standards.GMS_SEARCH_01.toString());
                GroupClient client = GroupUtil.getGroupClient(resourceID);
                List<GroupURI> userGroups = client.getMemberships();
                for (GroupURI gg : granted) {
                    for (GroupURI userGroup : userGroups) {
                        if (gg.equals(userGroup)) {
                            this.logInfo.setGrant("write: " + gg);
                            return;
                        }
                    }
                }
            }
        } catch (CertificateException ex) {
            throw new AccessControlException("permission denied (invalid delegated client certificate)");
        }

        throw new AccessControlException("permission denied");
    }

    private class GetReadGrantsAction implements PrivilegedExceptionAction<List<ReadGrant>> {

        URI artifactURI;
        private List<URI> readGrantServices;

        GetReadGrantsAction(URI artifactURI, List<URI> readGrantServices) {
            this.artifactURI = artifactURI;
            this.readGrantServices = readGrantServices;
        }

        @Override
        public List<ReadGrant> run() throws Exception {
            // TODO: could call multiple services in parallel
            List<ReadGrant> ret = new ArrayList<>();
            boolean partialSuccess = false;
            URI lastServiceFail = null;
            Exception lastFail = null;
            for (URI ps : this.readGrantServices) {
                try {
                    PermissionsClient pc = new PermissionsClient(ps);
                    ReadGrant grant = pc.getReadGrant(this.artifactURI);
                    if (grant != null) {
                        ret.add(grant);
                    }
                    partialSuccess = true;
                } catch (ResourceNotFoundException ex) {
                    log.warn("failed to find granting service: " + ps + " -- cause: " + ex);
                } catch (Exception ex) {
                    log.warn("failed to call granting service: " + ps + " -- casuse: " + ex);
                    lastServiceFail = ps;
                    lastFail = ex;
                }
            }
            if (!partialSuccess) {
                throw new RuntimeException("failed to call granting service: " + lastServiceFail, lastFail);
            }
            return ret;
        }
    }

    private class GetWriteGrantsAction implements PrivilegedExceptionAction<List<WriteGrant>> {

        URI artifactURI;
        List<URI> writeGrantServices;
        
        GetWriteGrantsAction(URI artifactURI, List<URI> writeGrantServices) {
            this.artifactURI = artifactURI;
            this.writeGrantServices = writeGrantServices;
        }

        @Override
        public List<WriteGrant> run() throws Exception {
            // TODO: could call multiple services in parallel
            List<WriteGrant> ret = new ArrayList<>();
            for (URI ps : this.writeGrantServices) {
                try {
                    PermissionsClient pc = new PermissionsClient(ps);
                    WriteGrant grant = pc.getWriteGrant(this.artifactURI);
                    if (grant != null) {
                        ret.add(grant);
                    }
                } catch (ResourceNotFoundException ex) {
                    log.warn("failed to find granting service: " + ps + " -- cause: " + ex);
                }
            }
            return ret;
        }
    }

}

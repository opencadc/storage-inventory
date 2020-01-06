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

package org.opencadc.inventory.permissions;

import ca.nrc.cadc.auth.AuthenticationUtil;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.util.Calendar;
import java.util.Date;
import javax.security.auth.Subject;
import javax.servlet.ServletException;
import org.apache.log4j.Logger;
import org.opencadc.inventory.permissions.xml.GrantWriter;

public class GetAction extends PermissionsAction {
    private static final Logger log = Logger.getLogger(GetAction.class);

    private static final String OP = "OP";
    private static final String URI = "URI";
    private static final String READ = "read";
    private static final String WRITE = "write";

    public GetAction() {
        super();
    }

    @Override
    public void doAction() throws Exception {

        String op = syncInput.getParameter(OP);
        if (op == null) {
            throw new ServletException("missing required parameter: " + OP);
        }
        if (!op.equals(READ) && !op.equals(WRITE)) {
            throw new ServletException("invalid " + OP + " parameter, must be " + READ  + " or " + WRITE);
        }

        String uri = syncInput.getParameter(URI);
        if (uri == null) {
            throw new ServletException("missing required parameter: " + URI);
        }
        java.net.URI artifactURI;
        try {
            artifactURI = new URI(uri);
        } catch (URISyntaxException e) {
            throw new ServletException(URI + " parameter is not a valid URI");
        }
        log.debug(OP + " = " + op);
        log.debug(URI + " = " + artifactURI.toASCIIString());

        // Check if the calling user is authorized to make the request.
        Subject subject = AuthenticationUtil.getCurrentSubject();
        log.debug(subject.toString());
        if (!isAuthorized(subject)) {
            throw new AccessControlException("User authorization failed.");
        }

        Permissions permissions = getPermissions(artifactURI);
        Date expiryDate = getExpiryDate(artifactURI);

        Grant grant;
        if (op.equals(READ)) {
            grant = new ReadGrant(artifactURI, expiryDate, permissions.getIsAnonymous());
            grant.getGroups().addAll(permissions.getReadOnlyGroups());
        } else {
            grant = new WriteGrant(artifactURI, expiryDate);
            grant.getGroups().addAll(permissions.getReadWriteGroups());
        }

        OutputStream out = null;
        try {
            syncOutput.setHeader("Content-Type", PERMISSIONS_CONTENT_TYPE);
            syncOutput.setCode(200);

            out = syncOutput.getOutputStream();
            GrantWriter writer = new GrantWriter();
            writer.write(grant, out);
            out.flush();
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception ignore) {
                    // ignore
                }
            }
        }
    }

    /**
     * Calculate the expiry date of the grant for the given artifact URI.
     * Arbitrarily set to a week from now.
     *
     * @param artifactURI The Artifact URI.
     * @return A Date.
     */
    private Date getExpiryDate(URI artifactURI) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, 7);
        return cal.getTime();
    }

}

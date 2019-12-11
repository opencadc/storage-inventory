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

package org.opencadc.inventory.permissions.xml;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.xml.XmlUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.opencadc.gms.GroupURI;
import org.opencadc.inventory.permissions.Grant;
import org.opencadc.inventory.permissions.ReadGrant;
import org.opencadc.inventory.permissions.WriteGrant;

public class GrantReader {

    static enum ENAMES {
        grant(), artifactURI(), expiryDate(), isAnonymousAccess(), groups(), groupURI(),
        type(), Read(), Write();
    }

    public GrantReader() {
    }

    public Grant read(String xml)  throws IOException {
        return read(new StringReader(xml));
    }

    public Grant read(InputStream istream)  throws IOException {
        return read(new InputStreamReader(istream));
    }

    public Grant read(Reader reader) throws IOException {
        if (reader == null) {
            throw new IllegalArgumentException("reader must not be null");
        }

        Document document;
        try {
            document = XmlUtil.buildDocument(reader);
        } catch (JDOMException ex) {
            throw new IllegalArgumentException("invalid input document", ex);
        }

        // Root element and namespace of the Document
        Element root = document.getRootElement();
        if (!ENAMES.grant.name().equals(root.getName())) {
            throw new IllegalArgumentException("invalid root element: " + root.getName() + " expected: " + ENAMES.grant);
        }
        boolean isReadGrant = isReadGrant(root);

        URI artifactURI = getURI(ENAMES.artifactURI.name(), root.getChildTextTrim(ENAMES.artifactURI.name()));
        Date expiryDate = getDate(ENAMES.expiryDate.name(), root.getChildTextTrim(ENAMES.expiryDate.name()));

        Grant grant;
        if (isReadGrant) {
            boolean isAnonymousAccess = getBoolean(ENAMES.isAnonymousAccess.name(), root.getChildTextTrim(ENAMES.isAnonymousAccess.name()));
            grant = new ReadGrant(artifactURI, expiryDate, isAnonymousAccess);
        } else {
            grant = new WriteGrant(artifactURI, expiryDate);
        }

        getGroupList(grant.getGroups(), ENAMES.groups.name(), root.getChildren(ENAMES.groups.name()));

        return grant;
    }

    private boolean isReadGrant(Element root) {
        String type = root.getAttributeValue(ENAMES.type.name());
        if (type == null) {
            throw new IllegalArgumentException("invalid grant element, missing expected 'type' attribute");
        }
        if (type.equals(ENAMES.Read.name())) {
            return true;
        }
        return false;
    }

    private URI getURI(String name, String value) {
        if (value == null) {
            throw new IllegalArgumentException("missing " + name + " element: " + value);
        }
        try {
            return new URI(value);
        } catch (Exception ex) {
            throw new IllegalArgumentException("invalid " + name + " element: " + value + ", expected: valid URI");
        }
    }

    private Date getDate(String name, String value) {
        if (value == null) {
            throw new IllegalArgumentException("missing " + name + " element: " + value);
        }
        try {
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            return df.parse(value);
        } catch (ParseException ex) {
            throw new IllegalArgumentException("invalid timestamp " + name + ": " + value);
        }
    }

    private boolean getBoolean(String name, String value) {
        if (value == null) {
            throw new IllegalArgumentException("missing " + name + " element: " + value);
        }
        if (Boolean.TRUE.toString().equals(value)) {
            return true;
        }
        return false;
    }

    private GroupURI getGroupURI(String value) {
        try {
            return new GroupURI(value);
        } catch (Exception ex) {
            throw new IllegalArgumentException("invalid groupURI element: " + value + " expected: valid GroupURI");
        }
    }

    private void getGroupList(List<GroupURI> groups, String name, List<Element> elements) {
        if (elements == null || elements.isEmpty()) {
            return;
        }

        if (elements.size() > 1) {
            throw new IllegalArgumentException("invalid input document: found multiple " + name + " expected: 0 or 1");
        }

        Element groupElements = elements.get(0);
        for (Element groupElement : groupElements.getChildren()) {
            if (!ENAMES.groupURI.name().equals(groupElement.getName())) {
                throw new IllegalArgumentException("invalid child element in " + name + ": " + groupElement.getName() + " expected: groupURI");
            }
            GroupURI groupURI = getGroupURI(groupElement.getTextTrim());
            groups.add(groupURI);
        }
    }

}

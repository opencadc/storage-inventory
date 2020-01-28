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
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.util.List;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.opencadc.gms.GroupURI;
import org.opencadc.inventory.permissions.Grant;
import org.opencadc.inventory.permissions.ReadGrant;

public class GrantWriter {

    private DateFormat dateFormat;
    private final boolean writeEmptyCollections;

    public GrantWriter() {
        this(false);
    }

    public GrantWriter(boolean writeEmptyCollections) {
        this.writeEmptyCollections = writeEmptyCollections;
        this.dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    }

    public void write(Grant grant, OutputStream ostream) throws IOException {
        write(grant, new OutputStreamWriter(ostream));
    }

    public void write(Grant grant, Writer writer) throws IOException {
        Element root = new Element(GrantReader.ENAMES.grant.name());

        addChild(root, GrantReader.ENAMES.artifactURI.name(), grant.getArtifactURI().toASCIIString());
        addChild(root, GrantReader.ENAMES.expiryDate.name(), dateFormat.format(grant.getExpiryDate()));

        if (grant instanceof ReadGrant) {
            // root.addNamespaceDeclaration(XSI_NS);
            root.setAttribute(GrantReader.ENAMES.type.name(), GrantReader.ENAMES.ReadGrant.name());
            Element pub = new Element(GrantReader.ENAMES.anonymousRead.name());
            pub.setText(Boolean.toString(((ReadGrant) grant).isAnonymousAccess()));
            root.addContent(pub);
        } else {
            root.setAttribute(GrantReader.ENAMES.type.name(), GrantReader.ENAMES.WriteGrant.name());
        }

        Element groups = new Element(GrantReader.ENAMES.groups.name());
        if (!grant.getGroups().isEmpty() || writeEmptyCollections) {
            root.addContent(groups);
        }
        addGroups(grant.getGroups(), groups);

        Document doc = new Document(root);
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        outputter.output(doc, writer);
    }

    private void addChild(Element parent, String ename, String eval) {
        Element uri = new Element(ename);
        uri.setText(eval);
        parent.addContent(uri);
    }

    private void addGroups(List<GroupURI> groups, Element parent) {
        for (GroupURI groupURI : groups) {
            Element uri = new Element(GrantReader.ENAMES.groupURI.name());
            uri.setText(groupURI.getURI().toASCIIString());
            parent.addContent(uri);
        }
    }
}

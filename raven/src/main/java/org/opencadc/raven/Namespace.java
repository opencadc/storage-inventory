/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2021.                            (c) 2021.
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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.raven;

import java.net.URI;
import org.opencadc.inventory.InventoryUtil;

/**
 * Class to represent a namespace, which is a prefix for an Artifact URI.
 *
 * <p>A namespace contains a schema with an optional path.</p>
 *
 * {@code <schema>:[<path>/] }
 *
 * <p>A namespace containing a schema only must end with a colon ':', i.e. <code>cadc:</code></p>
 * <p>A namespace with a schema and a path must end with a forward slash '/' after the path,
 * i.e. <code>cadc:CFHT/123/</code></p>
 */
public class Namespace {

    private final String namespace;

    public Namespace(String value) throws IllegalArgumentException {
        this.namespace = value;
        parse(value);
    }

    /**
     * Parse and validate the namespace.
     *
     * @param value namespaced to validate
     * @throws IllegalArgumentException if the namespace fails validation
     */
    private void parse(String value) throws IllegalArgumentException {

        if (value == null) {
            throw new IllegalArgumentException("null namespace");
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException("zero length namespace");
        }

        int colon = value.indexOf(':');
        int slash = value.lastIndexOf('/');
        String message = "invalid namespace %s : %s, expected <schema>:[<path>/]";

        // if namespace doesn't contain a '/', it should end with a ':'
        if (slash == -1 && value.length() > colon + 1) {
            throw new IllegalArgumentException(String.format(message, value,
                                                             "schema only namespace should end with :"));
        }

        // namespace with a path should end with a '/'
        if (slash != -1 && value.length() > slash + 1) {
            throw new IllegalArgumentException(String.format(message, value,
                                                             "namespace with path should end with /"));
        }

        try {
            InventoryUtil.validateArtifactURI(Namespace.class, URI.create(value + "file.ext"));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(message, value, e.getMessage()));
        }

    }

    public String getNamespace() {
        return this.namespace;
    }

    public boolean matches(URI artifactURI) {
        return artifactURI.toASCIIString().startsWith(this.namespace);
    }

}

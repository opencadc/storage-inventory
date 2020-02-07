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
 *  $Revision: 4 $
 *
 ************************************************************************
 */

package org.opencadc.inventory.storage.ad;

//import ca.nrc.cadc.caom2.artifact.resolvers.util.ResolverUtil;
import ca.nrc.cadc.net.StorageResolver;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import org.apache.log4j.Logger;

/**
 * Fork of GeminiArtifactResolver from caom2-artifact-resolver library. Maintained so the original can be modified
 * in future to point to a different storage location, while the AdStorageAdapter can maintain reference
 * to the original storage location.
 *
 * This class can convert a GEMINI URI into a URL.
 *
 * @author jeevesh
 */
public class GeminiArtifactResolver implements StorageResolver {
    public static final String SCHEME = "gemini";
    public static final String ARCHIVE = "GEM";
    public static final String FILE_URI = "file";
    public static final String PREVIEW_URI = "preview";
    private static final Logger log = Logger.getLogger(GeminiArtifactResolver.class);
    private static final String BASE_URL = "https://archive.gemini.edu";
    private static final String JPEG_SUFFIX = ".jpg";

    public GeminiArtifactResolver() {
    }

    @Override
    public URL toURL(URI uri) {
        ResolverUtil.validate(uri, SCHEME);
        String urlStr = "";
        try {
            String path = getPath(uri);
            urlStr = BASE_URL + path;

            URL url = null;
            if (urlStr != null) {
                url = new URL(urlStr);
            }

            log.debug(uri + " --> " + url);
            return url;
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG: could not generate URL from uri " + urlStr, ex);
        }
    }

    private String getPath(URI uri) {
        String[] path = uri.getSchemeSpecificPart().split("/");
        if (path.length != 2) {
            throw new IllegalArgumentException("Malformed URI. Expected 2 path components, found " + path.length);
        }

        String archive = path[0];
        if (!(archive.equals(ARCHIVE))) {
            throw new IllegalArgumentException("Invalid URI. Expected archive: " + ARCHIVE + ", actual archive: " + archive);
        }

        String fileName = path[1];
        String fileType = FILE_URI;
        if (fileName.endsWith(JPEG_SUFFIX)) {
            fileName = fileName.substring(0, fileName.length() - JPEG_SUFFIX.length()) + ".fits"; 
            fileType = PREVIEW_URI;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("/");
        sb.append(fileType);
        sb.append("/");
        sb.append(fileName);

        return sb.toString();
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }



}


/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2026.                            (c) 2026.
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

package org.opencadc.inventory.storage.eos;

import ca.nrc.cadc.io.ResourceIterator;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.log4j.Logger;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 *
 * @author pdowler
 */
public class EosPathIterator implements Iterator<StorageMetadata> {
    private static final Logger log = Logger.getLogger(EosPathIterator.class);

    private final URI mgmServer;
    private final String mgmPath;
    private final String authToken;
    private final String artifactScheme;
    
    // optional storageBucketPrefix
    private final String pathPrefix;
    
    private StorageMetadata cur;
    private Iterator<SubPath> subpathIter;
    private ResourceIterator<StorageMetadata> eosIter;
    
    public EosPathIterator(URI mgmServer, String mgmPath, String authToken, String artifactScheme, String pathPrefix) {
        this.mgmServer = mgmServer;
        this.mgmPath = mgmPath;
        this.authToken = authToken;
        this.artifactScheme = artifactScheme;
        this.pathPrefix = pathPrefix;
        init();
        advance();
    }

    @Override
    public boolean hasNext() {
        return cur != null;
    }

    @Override
    public StorageMetadata next() {
        if (cur == null) {
            throw new NoSuchElementException();
        }
        StorageMetadata ret = cur;
        advance();
        return ret;
    }

    private void init() {
        try {
            EosPathDivider pdiv = new EosPathDivider(mgmServer, mgmPath, authToken, pathPrefix);
            List<SubPath> subpaths = pdiv.subdivide();
            this.subpathIter = subpaths.iterator();
        } catch (IOException ex) {
            throw new StorageEngageException("init: failed to subdivide target (" + pathPrefix + ") into viable subpaths", ex);
        }
    }
    
    private void advance() {
        this.cur = null;
        if (eosIter == null && subpathIter == null) {
            return; // done
        }

        // inner: read output from EosFind
        if (eosIter.hasNext()) {
            this.cur = eosIter.next();
        } else {
            eosIter = null;
        }
        if (eosIter == null && subpathIter != null) {
            while (eosIter == null && subpathIter.hasNext()) {
                // invoke new find
                SubPath sub = subpathIter.next();
                log.warn("find: " + sub.path + " expected file count: " + sub.numFiles);
                this.eosIter = new EosFind(mgmServer, mgmPath, authToken, artifactScheme, sub.path, sub.shallow);
                if (eosIter.hasNext()) {
                    this.cur = eosIter.next();
                } else {
                    eosIter = null; // empty dir
                }
            }
            if (!subpathIter.hasNext()) {
                subpathIter = null;
            }
        }
    }
}

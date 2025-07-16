/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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

package org.opencadc.inventory.storage.fs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * Iterator implementation for the OpaqueFileSystemStorageAdapter.
 * 
 * @author pdowler
 */
class OpaqueIterator implements Iterator<StorageMetadata> {
    private static final Logger log = Logger.getLogger(OpaqueIterator.class);

    private final boolean includeRecoverable;
    private final Path contentPath;
    private Path subPath;
    
    private StorageMetadata nextItem = null;
    private final LinkedList<StackItem> depthFirstPathTraversalStack = new LinkedList<>();
    
    public OpaqueIterator(Path contentPath, String bucketPrefix, boolean includeRecoverable) throws StorageEngageException {
        this.contentPath = contentPath;
        this.includeRecoverable = includeRecoverable;
        StringBuilder sb = new StringBuilder();
        if (bucketPrefix != null) {
            for (char c : bucketPrefix.toCharArray()) {
                sb.append(c).append(File.separator);
            }
        }
        this.subPath = contentPath;
        if (sb.length() > 0) {
            this.subPath = contentPath.resolve(sb.toString());
        }
        if (Files.exists(subPath)) {
            this.depthFirstPathTraversalStack.addFirst(new StackItem(subPath));
            advance();
        } // else: nothing in this bucket
    }
    
    // design:
    // - all directories below contentPath are chars in the storageBucket
    // - all files are leaf nodes with posix attrs
    // - advance() implements look-ahead to find the next item
    
    @Override
    public boolean hasNext() {
        return nextItem != null;
    }

    @Override
    public StorageMetadata next() {
        if (nextItem == null) {
            throw new NoSuchElementException();
        }
        StorageMetadata ret = nextItem;
        log.debug("*** next() returning: " + ret);
        nextItem = null;
        advance();
        log.debug("*** next() look-ahead: " + nextItem);
        return ret;
    }
    
    private void advance() {
        log.debug("advance: stack size = " + depthFirstPathTraversalStack.size() + " START");
        while (!depthFirstPathTraversalStack.isEmpty() && nextItem == null) {
            StackItem cur = depthFirstPathTraversalStack.getFirst();
            log.debug("advance: cur " + cur.dir);
            boolean pushed = false;
            while (cur.children.hasNext() && !pushed && nextItem == null) {
                Path p = cur.children.next();
                if (Files.isRegularFile(p)) {
                    nextItem = createStorageMetadata(p);
                    log.debug("advance: nextItem=" + nextItem);
                } else if (Files.isDirectory(p)) {
                    depthFirstPathTraversalStack.addFirst(new StackItem(p));
                    pushed = true;
                } else {
                    throw new IllegalStateException("INVALID STATE: non-directory and non-regular-file: " + p);
                }
            }
            if (!pushed && nextItem == null) {
                log.debug("advance: return from " + cur.dir + " " + depthFirstPathTraversalStack.size() + " exhausted");
                StackItem rm = depthFirstPathTraversalStack.removeFirst(); // pop exhausted/empty dir
                log.debug("advance: removed " + rm.dir + " stack.isEmpty " + depthFirstPathTraversalStack.isEmpty());
            }
        }
        log.debug("advance: stack size = " + depthFirstPathTraversalStack.size() + " DONE");
    }
    
    private class StackItem {
        Path dir;
        Iterator<Path> children;
        
        StackItem(Path dir) {
            this.dir = dir;
            log.debug("enter dir: " + dir);
            try {
                Stream<Path> str = Files.list(dir);
                Iterator<Path> iter = str.iterator();
                ArrayList<Path> tmp = new ArrayList<>(16); // init size for full directory structure
                while (iter.hasNext()) {
                    Path p = iter.next();
                    tmp.add(p);
                }
                Collections.sort(tmp);
                this.children = tmp.iterator();
            } catch (IOException ex) {
                throw new RuntimeException("failed to list: " + dir, ex);
            }
        }
    }
    
    private StorageMetadata createStorageMetadata(Path p) {
        return OpaqueFileSystemStorageAdapter.createStorageMetadataImpl(contentPath, p, includeRecoverable);
    }
}

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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.Date;
import java.util.EmptyStackException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * An iterator of files within a file system.
 * 
 * @author majorb
 */
class LogicalIterator implements Iterator<StorageMetadata> {
    
    private static final Logger log = Logger.getLogger(LogicalIterator.class);
    
    private final Path base;
    private Path nextFile = null;
    Stack<StackItem> stack;

    /**
     * FileSystemIterator constructor.
     * 
     * @param base The base path to content
     * @throws IOException If there is a problem with file-system interaction.
     */
    public LogicalIterator(Path base, String storageBucketPrefix) {
        InventoryUtil.assertNotNull(LogicalIterator.class, "dir", base);
        if (!Files.isDirectory(base)) {
            throw new IllegalArgumentException("not a directory: " + base);
        }
        this.base = base;
        
        try {
            this.stack = new Stack<>();
            Path p = base;
            if (storageBucketPrefix != null) {
                p = base.resolve(storageBucketPrefix);
            }
            if (p != null && Files.isDirectory(p)) {
                log.debug("entering directory: " + p);
                StackItem item = new StackItem(Files.list(p));
                stack.push(item);
                advance();
            } else {
                this.nextFile = null;
            }
        } catch (IOException ex) {
            throw new StorageEngageException("failed to read filesystem: " + ex.getMessage(), ex);
        }
    }
    
    @Override
    public boolean hasNext() {
        return (nextFile != null);
        
    }

    /**
     * Get the next file element.
     * @return The next file in the iterator, identified by StorageMetadata.
     */
    @Override
    public StorageMetadata next() {
        if (nextFile == null) {
            throw new NoSuchElementException();
        }
        Path rpath = nextFile;
        try {
            advance();
        } catch (IOException ex) {
            throw new StorageEngageException("failed to read filesystem: " + ex.getMessage(), ex);
        }
        return LogicalFileSystemStorageAdapter.createStorageMetadataImpl(base, rpath, false);
    }
    
    private void advance() throws IOException {
        this.nextFile = null;
        
        while (nextFile == null) {
            if (stack.empty()) {
                return;
            }
            StackItem currentStackItem = stack.peek();
            Iterator<Path> currentIterator = currentStackItem.iterator;
            if (currentIterator.hasNext()) {
                Path nextPath = currentIterator.next();
                if (Files.isDirectory(nextPath)) {
                    log.debug("entering directory: " + nextPath);
                    StackItem item = new StackItem(Files.list(nextPath));
                    stack.push(item);
                } else {
                    this.nextFile = nextPath;
                }
            } else {
                log.debug("completed directory listing");
                StackItem item = stack.pop();
                item.stream.close();
            }
        }
    }

    private class StackItem {
        Stream<Path> stream;
        Iterator<Path> iterator;

        StackItem(Stream<Path> stream) {
            this.stream  = stream;
            this.iterator = stream.sorted().iterator();
        }
    }
    
    //private class PathItem {
    //    Path path;
    //    String pathAndFileName;
    //}
}
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
import java.util.Stack;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * An iterator of files within a file system.
 * 
 * @author majorb
 */
public class FileSystemIterator implements Iterator<StorageMetadata> {
    
    private static final Logger log = Logger.getLogger(FileSystemIterator.class);
    
    private PathItem next = null;
    Stack<StackItem> stack;
    private String fixedParentDir;

    /**
     * FileSystemIterator constructor.
     * 
     * @param dir The directory to iterate
     * @param ignoreDepth The depth of directories to navigate until non-bucket
     *     directories are seen.
     * @param fixedParentDir A path to add to the start of all returned files.
     * @throws IOException If there is a problem with file-system interaction.
     */
    public FileSystemIterator(Path dir, int ignoreDepth, String fixedParentDir) throws IOException {
        InventoryUtil.assertNotNull(FileSystemIterator.class, "dir", dir);
        if (!Files.isDirectory(dir)) {
            throw new IllegalArgumentException("not a directory: " + dir);
        }
        stack = new Stack<StackItem>();
        this.fixedParentDir = fixedParentDir;
        
        StackItem item = new StackItem();
        item.stream = Files.list(dir);;
        item.iterator = item.stream.iterator();
        // base parentDir for the Iterator is CONTENT_FOLDER
        item.parentDir = FileSystemStorageAdapter.CONTENT_FOLDER;
        item.ignoreDepth = ignoreDepth;
        
        log.debug("bucket depth: " + item.ignoreDepth);
        log.debug("parentDir: " + item.parentDir);
        log.debug("entering directory [physical][logical]: [" + dir + "][]");
        
        stack.push(item);
    }
    
    /**
     * Used to see if there are more elements in the iterator.
     * @return true if there are more files over which to iterate.
     */
    @Override
    public boolean hasNext() {
        try {
            StackItem currentStackItem = stack.peek();
            Iterator<Path> currentIterator = currentStackItem.iterator;
            if (currentIterator.hasNext()) {
                Path nextPath = currentIterator.next();
                if (Files.isDirectory(nextPath)) {
                    StackItem item = new StackItem();
                    log.debug("bucket depth: " + currentStackItem.ignoreDepth);
                    log.debug("parentDir: " + currentStackItem.parentDir);
                    if (currentStackItem.ignoreDepth > 0) {
                        item.ignoreDepth = currentStackItem.ignoreDepth - 1;
                        item.parentDir = currentStackItem.parentDir;
                    } else {
                        String parentDir = currentStackItem.parentDir + nextPath.getFileName() + "/";
                        item.parentDir = parentDir;
                    }
                    log.debug("entering directory [physical][logical]: [" + nextPath + "][" + item.parentDir + "]");
                    item.stream = Files.list(nextPath);;
                    item.iterator = item.stream.iterator();
                    stack.push(item);
                    return this.hasNext();
                } else {
                    next = new PathItem();
                    next.pathAndFileName = currentStackItem.parentDir + nextPath.getFileName();
                    if (fixedParentDir != null) {
                        next.pathAndFileName = fixedParentDir + File.separator + next.pathAndFileName;
                    }
                    next.path = nextPath;
                    return true;
                }
            } else {
                log.debug("completed directory listing");
                StackItem item = stack.pop();
                item.stream.close();
                return this.hasNext();
            }
        } catch (EmptyStackException e) {
            log.debug("no more iterators, done");
            return false;
        } catch (IOException e) {
            throw new IllegalStateException("io exception: " + e.getMessage(), e);
        }
    }

    /**
     * Get the next file element.
     * @return The next file in the iterator, identified by StorageMetadata.
     */
    @Override
    public StorageMetadata next() {
        if (next == null) {
            throw new IllegalStateException("No more elements");
        }
        URI storageID = URI.create(next.pathAndFileName);
        StorageLocation storageLocation = new StorageLocation(storageID);
        // TODO: restore bucket consistent with put() return value
        //storageLocation.storageBucket = ??; 
        try {
            URI checksum = new URI(getFileAttribute(next.path, FileSystemStorageAdapter.CHECKSUM_ATTRIBUTE_NAME));
            long length = Files.size(next.path);
            StorageMetadata meta = new StorageMetadata(storageLocation, storageID, 
                    checksum, length, new Date(Files.getLastModifiedTime(next.path).toMillis()));
            return meta;
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute file metadata: " + e.getMessage(), e);
        }
    }
    
    private class StackItem {
        Stream<Path> stream;
        Iterator<Path> iterator;
        String parentDir;
        int ignoreDepth;
    }
    
    private class PathItem {
        Path path;
        String pathAndFileName;
    }

    private static String getFileAttribute(Path path, String attributeName) throws IOException {
        UserDefinedFileAttributeView udv = Files.getFileAttributeView(path,
            UserDefinedFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);

        int sz = udv.size(attributeName);
        ByteBuffer buf = ByteBuffer.allocate(2 * sz);
        udv.read(attributeName, buf);
        return new String(buf.array(), Charset.forName("UTF-8")).trim();
    }

}

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
package org.opencadc.inventory.storage;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.HexUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencadc.inventory.StorageLocation;

/**
 * This storage adapter implementation is fixed on a set of existing data
 * and metadata defined in the static attributes of this class.
 * 
 * @author majorb
 */
public class TestStorageAdapter implements StorageAdapter {
    
    private static final Logger log = Logger.getLogger(TestStorageAdapter.class);
    
    static final String dataString = "abcdefghijklmnopqrstuvwxyz";
    static final byte[] data = dataString.getBytes();
    static final URI storageID = URI.create("test:path/file");
    static final Long contentLength = new Long(data.length);
    static URI contentChecksum;
    static {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md5 = md.digest(data);
            String md5hex = HexUtil.toHex(md5);
            contentChecksum = URI.create("md5:" + md5hex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    static int BUF_SIZE = 6;
    
    public TestStorageAdapter() {
    }

    public void get(StorageLocation storageLocation, OutputStream dest)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        InputStream source = new ByteArrayInputStream(data);
        ThreadedIO io = new ThreadedIO(BUF_SIZE, 3);
        io.ioLoop(dest, source);
    }

    public void get(StorageLocation storageLocation, OutputStream dest, Set<String> cutouts)
        throws ResourceNotFoundException, ReadException, WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    public StorageMetadata put(NewArtifact newArtifact, InputStream source)
        throws StreamCorruptedException, ReadException, WriteException, StorageEngageException, TransientException {

        ByteArrayOutputStream dest = new ByteArrayOutputStream();
        ThreadedIO io = new ThreadedIO(BUF_SIZE, 3);
        io.ioLoop(dest, source);

        byte[] newData = dest.toByteArray();
        log.info("received data: " + new String(newData));
        
        URI newContentChecksum = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md5 = md.digest(newData);
            String md5hex = HexUtil.toHex(md5);
            newContentChecksum = URI.create("md5:" + md5hex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long newContentLength = newData.length;
        if (!newContentChecksum.equals(contentChecksum)) {
            throw new StreamCorruptedException("checksum: " +
                newContentChecksum + " does not equal " + contentChecksum);
        }
        if (newContentLength != contentLength) {
            throw new StreamCorruptedException("length: " +
                newContentLength + " does not equal " + contentLength);
        }
        
        StorageLocation storageLocation = new StorageLocation(storageID);
        StorageMetadata storageMetadata = new StorageMetadata(storageLocation, contentChecksum, contentLength);
        storageMetadata.artifactURI = newArtifact.getArtifactURI();
        return storageMetadata;
    }
        
    public void delete(StorageLocation storageLocation)
        throws ResourceNotFoundException, IOException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    public Iterator<StorageMetadata> iterator()
        throws ReadException, WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    public Iterator<StorageMetadata> iterator(String storageBucket)
        throws ReadException, WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

    public Iterator<StorageMetadata> unsortedIterator(String storageBucket)
        throws ReadException, WriteException, StorageEngageException, TransientException {
        throw new UnsupportedOperationException();
    }

}

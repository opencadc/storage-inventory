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
************************************************************************
 */

package org.opencadc.minoc.operations;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.SortedSet;
import java.util.TreeSet;
import nom.tam.util.RandomAccess;
import org.apache.log4j.Logger;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.ByteRange;
import org.opencadc.inventory.storage.StorageAdapter;

/**
 * Proxy stream that reads from a StorageAdapter.
 * 
 * @author pdowler
 */
public class ProxyInputStream extends InputStream {
    private static final Logger log = Logger.getLogger(ProxyInputStream.class);

    private final StorageAdapter sad;
    private final StorageLocation loc;
    private long curpos = 0L;
    private long markpos = -1L;
    private final long contentLength;

    public ProxyInputStream(StorageAdapter sad, StorageLocation loc, long contentLength) {
        super();
        this.sad = sad;
        this.loc = loc;
        this.contentLength = contentLength;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void reset() throws IOException {
        if (markpos == -1L) {
            throw new IOException("no mark position for reset");
        }
        this.curpos = markpos;
    }

    @Override
    public synchronized void mark(int readLimit) {
        // ignore readLimit
        this.markpos = curpos;
    }

    @Override
    public void close() throws IOException {
        this.curpos = -1L;
    }

    @Override
    public int available() throws IOException {
        return 0;
    }

    @Override
    public long skip(long len) throws IOException {
        long ret = len;
        if (curpos + len > contentLength) {
            len = contentLength - curpos;
        }
        curpos += len;
        log.warn("ProxyInputStream.skip(" + len + " (curpos:" + curpos + ")");
        return len;
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        return read(bytes, 0, bytes.length);
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        try {
            long avail = Math.min(len, contentLength - curpos); 
            ByteRange range = new ByteRange(curpos, avail);
            SortedSet<ByteRange> ranges = new TreeSet<>();
            ranges.add(range);
            ByteArrayOutputStream bos = new ByteArrayOutputStream(len);
            //log.warn("calling  StorageAdapter.get: curpos=" + curpos + " len=" + len);
            sad.get(loc, bos, ranges);
            byte[] result = bos.toByteArray();

            int ret = bos.size();
            System.arraycopy(result, 0, bytes, off, ret);
            curpos += ret;
            log.warn("ProxyInputStream.read(" + off + "," + len + " read " + ret + " (curpos:" + curpos + ")");
            return ret;
        } catch (Exception ex) {
            throw new IOException("read failed", ex);
        }
    }
}

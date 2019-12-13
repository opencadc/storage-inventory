
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
 *
 ************************************************************************
 */

package org.opencadc.inventory.storage.ceph;

import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;


/**
 * Creates an InputStream for the underlying IO Context from RADOS.  Since RADOS wants to only write out Byte Arrays,
 * we need to use that as a data source to this Input Stream.
 * Note that the main read() method is not implemented here as it relies on the RADOS API to provide the next buffer of
 * bytes.
 * jenkinsd 2019.12.11
 */
public class RadosInputStream extends InputStream {

    private static final Logger LOGGER = Logger.getLogger(RadosInputStream.class);
    private static final String DATA_POOL_NAME = "default.rgw.buckets.non-ec";


    /**
     * The Object ID to lookup in RADOS.
     */
    final String objectID;

    /**
     * The Context lookup object to communicate with the RADOS cluster with.  Will be closed with this stream.
     */
    final Rados rados;

    /**
     * The entire offset for the RADOS IO Context.  When reading from RADOS, we need to provide the offset (current)
     * position in the stream.
     */
    long position;


    /**
     * Creates a {@code RadosInputStream} so that it uses the {@code ByteArrayInputStream} as its buffer stream.
     *
     * @param rados    The RADOS object.  It should already be connected.
     * @param objectID The Object ID to lookup.
     */
    RadosInputStream(final Rados rados, final String objectID) {
        this.rados = rados;
        this.objectID = objectID;
    }

    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an <code>int</code> in the range <code>0</code> to
     * <code>255</code>. If no byte is available because the end of the stream
     * has been reached, the value <code>-1</code> is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     * <p> A subclass must provide an implementation of this method.
     *
     * @return the next byte of data, or <code>-1</code> if the end of the
     * stream is reached.
     */
    @Override
    public int read() {
        throw new UnsupportedOperationException("Cannot read single byte.  Use read(byte[], int, int).");
    }

    /**
     * Skips over and discards <code>n</code> bytes of data from this input
     * stream. The <code>skip</code> method may, for a variety of reasons, end
     * up skipping over some smaller number of bytes, possibly <code>0</code>.
     * This may result from any of a number of conditions; reaching end of file
     * before <code>n</code> bytes have been skipped is only one possibility.
     * The actual number of bytes skipped is returned. If {@code n} is
     * negative, the {@code skip} method for class {@code InputStream} always
     * returns 0, and no bytes are skipped. Subclasses may handle the negative
     * value differently.
     * <p> The <code>skip</code> method implementation of this class creates a
     * byte array and then repeatedly reads into it until <code>n</code> bytes
     * have been read or the end of the stream has been reached. Subclasses are
     * encouraged to provide a more efficient implementation of this method.
     * For instance, the implementation may depend on the ability to seek.
     *
     * @param n the number of bytes to be skipped.
     * @return the actual number of bytes skipped.
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public long skip(long n) throws IOException {
        position += n;
        return n;
    }

    /**
     * Reads up to <code>len</code> bytes of data from the input stream into
     * an array of bytes.  An attempt is made to read as many as
     * <code>len</code> bytes, but a smaller number may be read.
     * The number of bytes actually read is returned as an integer.
     * <p> This method blocks until input data is available, end of file is
     * detected, or an exception is thrown.
     * <p> If <code>len</code> is zero, then no bytes are read and
     * <code>0</code> is returned; otherwise, there is an attempt to read at
     * least one byte. If no byte is available because the stream is at end of
     * file, the value <code>-1</code> is returned; otherwise, at least one
     * byte is read and stored into <code>b</code>.
     * <p> The first byte read is stored into element <code>b[off]</code>, the
     * next one into <code>b[off+1]</code>, and so on. The number of bytes read
     * is, at most, equal to <code>len</code>. Let <i>k</i> be the number of
     * bytes actually read; these bytes will be stored in elements
     * <code>b[off]</code> through <code>b[off+</code><i>k</i><code>-1]</code>,
     * leaving elements <code>b[off+</code><i>k</i><code>]</code> through
     * <code>b[off+len-1]</code> unaffected.
     * <p> In every case, elements <code>b[0]</code> through
     * <code>b[off]</code> and elements <code>b[off+len]</code> through
     * <code>b[b.length-1]</code> are unaffected.
     * <p> The <code>read(b,</code> <code>off,</code> <code>len)</code> method
     * for class <code>InputStream</code> simply calls the method
     * <code>read()</code> repeatedly. If the first such call results in an
     * <code>IOException</code>, that exception is returned from the call to
     * the <code>read(b,</code> <code>off,</code> <code>len)</code> method.  If
     * any subsequent call to <code>read()</code> results in a
     * <code>IOException</code>, the exception is caught and treated as if it
     * were end of file; the bytes read up to that point are stored into
     * <code>b</code> and the number of bytes read before the exception
     * occurred is returned. The default implementation of this method blocks
     * until the requested amount of input data <code>len</code> has been read,
     * end of file is detected, or an exception is thrown. Subclasses are
     * encouraged to provide a more efficient implementation of this method.
     *
     * @param b   the buffer into which the data is read.
     * @param off the start offset in array <code>b</code>
     *            at which the data is written.
     * @param len the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or
     * <code>-1</code> if there is no more data because the end of
     * the stream has been reached.
     *
     * @throws IOException               If the first byte cannot be read for any reason
     *                                   other than end of file, or if the input stream has been closed, or if
     *                                   some other I/O error occurs.
     * @throws NullPointerException      If <code>b</code> is <code>null</code>.
     * @throws IndexOutOfBoundsException If <code>off</code> is negative,
     *                                   <code>len</code> is negative, or <code>len</code> is greater than
     *                                   <code>b.length - off</code>
     * @see InputStream#read()
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        LOGGER.debug(String.format("Adjusting position by %d bytes.", off));

        // Add the provided offset as well.
        position += off;

        /*
         * This read() method will return <code>0</code> when there are no more bytes to be found.  This needs to be
         * taken into account as the <code>InputStream</code> API requires a <code>-1</code> to be returned when the
         * Stream is exhausted.
         */
        try (final IoCTX ioCTX = rados.ioCtxCreate(DATA_POOL_NAME)) {
            final int bytesRead = readRadosBytes(ioCTX, b, len);

            LOGGER.debug(String.format("Read %d bytes from RADOS.", bytesRead));

            // Update the current position in the stream.
            position += bytesRead;

            LOGGER.debug(String.format("Current position in stream is at byte %d.", position));
            return (bytesRead == 0) ? -1 : bytesRead;
        }
    }

    /**
     * Read bytes from the RADOS back end.
     * @param ioCTX     The IO Context connection.
     * @param b         The byte array to read into.
     * @param len       The amount of bytes to read.
     * @return          Count of bytes read.
     * @throws IOException  For any backend reading.
     */
    int readRadosBytes(final IoCTX ioCTX, byte[] b, int len) throws IOException {
        return ioCTX.read(objectID, len, position, b);
    }
}

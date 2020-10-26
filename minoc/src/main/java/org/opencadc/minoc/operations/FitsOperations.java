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

import ca.nrc.cadc.io.ReadException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import nom.tam.fits.BasicHDU;
import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.Header;
import nom.tam.util.RandomAccess;
import org.apache.log4j.Logger;
import org.opencadc.inventory.storage.StorageAdapter;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * Operation on FITS files.
 * 
 * @author pdowler
 */
public class FitsOperations {
    private static final Logger log = Logger.getLogger(FitsOperations.class);

    private final StorageAdapter storageAdapter;
    
    public FitsOperations(StorageAdapter storageAdapter) {
        this.storageAdapter = storageAdapter;
    }

    // commented out code requires implementation of readHeaderAndSkipHDU() as described in
    // https://github.com/nom-tam-fits/nom-tam-fits/issues/66
    
    public Header getPrimaryHeader(StorageMetadata sm) throws ReadException {
        try {
            ProxyInputStream istream = new ProxyInputStream(storageAdapter, sm.getStorageLocation(), sm.getContentLength());
            Fits fits = new Fits(istream);
            
            Header ret = fits.readHeaderSkipData();
            //BasicHDU hdu = fits.readHDU();
            //Header ret = hdu.getHeader();
            
            return ret;
        } catch (FitsException ex) {
            throw new RuntimeException("invalid fits data: " + sm.getStorageLocation());
        } catch (IOException ex) {
            throw new ReadException("failed to read " + sm.getStorageLocation(), ex);
        }
    }
    
    public List<Header> getHeaders(StorageMetadata sm) throws ReadException {
        try {
            List<Header> ret = new ArrayList<>();
            ProxyInputStream istream = new ProxyInputStream(storageAdapter, sm.getStorageLocation(), sm.getContentLength());
            Fits fits = new Fits(istream);
            
            Header h = fits.readHeaderSkipData();
            while (h != null) {
            //BasicHDU hdu = fits.readHDU();
            //while (hdu != null) {
                //Header h = hdu.getHeader();
                ret.add(h);
                h = fits.readHeaderSkipData();
                //hdu = fits.readHDU();
            }
            return ret;
        } catch (FitsException ex) {
            throw new RuntimeException("invalid fits data: " + sm.getStorageLocation());
        } catch (IOException ex) {
            throw new ReadException("failed to read " + sm.getStorageLocation(), ex);
        }
    }
    
    public BasicHDU getHDU(StorageMetadata sm) throws ReadException {
        try {
            ProxyInputStream istream = new ProxyInputStream(storageAdapter, sm.getStorageLocation(), sm.getContentLength());
            Fits fits = new Fits(istream);
            
            BasicHDU hdu = fits.readHDU();
            
            return hdu;
        } catch (FitsException ex) {
            throw new RuntimeException("invalid fits data: " + sm.getStorageLocation());
        } catch (IOException ex) {
            throw new ReadException("failed to read " + sm.getStorageLocation(), ex);
        }
    }
    
    private void scratch() {
        
        
    }
    
    private class StorageAdapterWrapper implements RandomAccess {
        
        private final StorageAdapter sa;
        
        public StorageAdapterWrapper(StorageAdapter sa) {
            this.sa = sa;
        }

        @Override
        public long getFilePointer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seek(long offsetFromStart) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mark(int readlimit) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(byte[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(byte[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(boolean[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(boolean[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(char[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(char[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(double[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(double[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(float[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(float[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(int[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(int[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(long[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(long[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(short[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(short[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readArray(Object o) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long readLArray(Object o) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long skip(long distance) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void skipAllBytes(long toSkip) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void skipAllBytes(int toSkip) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int skipBytes(int i) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean readBoolean() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte readByte() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readUnsignedByte() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public short readShort() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readUnsignedShort() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public char readChar() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readInt() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long readLong() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public float readFloat() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public double readDouble() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String readLine() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String readUTF() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            throw new UnsupportedOperationException();
        }
        
        
    }
    
    private class FileWrapper extends RandomAccessFile implements RandomAccess {

        public FileWrapper(File file) throws FileNotFoundException {
            super(file, "r");
        }

        @Override
        public long getFilePointer() {
            try {
                return super.getFilePointer();
            } catch (IOException ex) {
                throw new RuntimeException("API mismatch: RandomAccessFile.getFilePointer failed", ex);
            }
        }

        @Override
        public void mark(int readlimit) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(boolean[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(boolean[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(char[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(char[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(double[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(double[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(float[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(float[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(int[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(int[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(long[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(long[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(short[] buf) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(short[] buf, int offset, int size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readArray(Object o) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long readLArray(Object o) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long skip(long distance) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void skipAllBytes(long toSkip) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void skipAllBytes(int toSkip) throws IOException {
            throw new UnsupportedOperationException();
        }
        
        
    }
}

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

package org.opencadc.inventory.storage.eos;

import ca.nrc.cadc.exec.BuilderOutputGrabber;
import ca.nrc.cadc.io.ResourceIterator;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageEngageException;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 * A class to exec <code>eos find</code> command line.
 * 
 * @author pdowler
 */
public class EosFind implements ResourceIterator<StorageMetadata> {
    private static final Logger log = Logger.getLogger(EosFind.class);

    private static final String EOS_PATH = "path";
    private static final String EOS_CONTENT_TIME = "ctime";
    private static final String EOS_CHECKSUM_TYPE = "checksumtype";
    private static final String EOS_CHECKSUM_VALUE = "checksum";
    private static final String EOS_FILE_SIZE = "size";
    
    private final URI mgmServer;
    private final String mgmPath;
    private final String authToken;
    private final String artifactScheme;
    
    // optional storageBucketPrefix
    public String pathPrefix;
    
    private final StringBuilder stderr = new StringBuilder();
    private ReaderThread errThread;
    private Process proc;
    private InputStream istream;
    private OutputStream ostream;
    private InputStream estream;
    private int exitValue;
    
    private LineNumberReader inputReader;
    private StorageMetadata cur = null;

    public EosFind(URI mgmServer, String mgmPath, String authToken, String artifactScheme) {
        this.mgmServer = mgmServer;
        this.mgmPath = mgmPath;
        this.authToken = authToken;
        this.artifactScheme = artifactScheme;
    }

    // explicit start
    public void start() throws StorageEngageException {
        // two implementations:
        // readFully uses the well tested BuilderOutptuGrabber from cadc-util
        // openStream uses internal code based on above, reads from the stream while iterating,
        //            but less well tested for error conditions
        // currently used: openStream()
        try {
            String path = mgmPath;
            if (pathPrefix != null) {
                path += "/" + pathPrefix;
            }
            openStream(mgmServer, path, authToken);
            this.inputReader = new LineNumberReader(new InputStreamReader(istream));
            //this.inputReader = readFully(mgmServer, mgmPath, authToken);
            advance();
        } catch (IOException ex) {
            throw new StorageEngageException("failed to connect to EOS: " + ex.getMessage(), ex);
        }
    }

    @Override
    public boolean hasNext() {
        return cur != null;
    }

    @Override
    public StorageMetadata next() {
        StorageMetadata ret = cur;
        advance();
        if (cur != null) {
            // check order in the stream is as expected
            int cmp = ret.compareTo(cur);
            if (cmp >= 0) {
                throw new RuntimeException("FATAL: found out of order entries in EOS find stream:"
                    + ret.getStorageLocation() + " vs " + cur.getStorageLocation());
            }
        }
        return ret;
    }

    @Override
    public void close() throws IOException {
        closeStream();
    }

    private void advance() {
        this.cur = null;
        try {
            while (cur == null) {
                String line = inputReader.readLine();
                if (line == null) {
                    return; // end
                }
                this.cur = parseFileInfo(line);
            }
        } catch (IOException ex) {
            throw new RuntimeException("failed to read input", ex);
        }
    }

    // implementation
    StorageMetadata parseFileInfo(String line) {
        if (line == null || line.isEmpty() || line.isBlank()) {
            log.debug("skip blank line");
            return null;
        }
        
        if (!line.startsWith("path=")) {
            log.debug("skip unexpected output: " + line);
            return null;
        }

        URI artifactURI = null;
        URI contentChecksum = null;
        Date contentLastModified = null;
        Long contentLength = null;
        StorageLocation sloc = null;
        
        String csAlg = null;
        String csHex = null;
        String[] parts = line.split(" ");
        for (String s : parts) {
            String[] kv = s.split("=");
            if (kv.length == 2) {
                switch (kv[0]) {
                    case EOS_PATH:
                        log.debug("path: " + kv[1]);
                        // also remove quotes
                        String rel = kv[1].substring(mgmPath.length() + 2, kv[1].length() - 1);
                        artifactURI = URI.create(artifactScheme + ":" + rel);
                        sloc = pathToStorageLocation(rel);
                        break;
                    case EOS_FILE_SIZE:
                        log.debug("size: " + kv[1]);
                        try {
                            contentLength = Long.parseLong(kv[1]);
                        } catch (NumberFormatException ex) {
                            throw new RuntimeException("failed to parse " + s + ": " + kv[1] + " is not a valid long", ex);
                        }
                        break;
                    case EOS_CHECKSUM_TYPE:
                        log.debug("checksum scheme: " + kv[1]);
                        csAlg = kv[1];
                        break;
                    case EOS_CHECKSUM_VALUE:
                        log.debug("checksum hex: " + kv[1]);
                        csHex = kv[1];
                        break;
                    case EOS_CONTENT_TIME:
                        log.debug("time: " + kv[1]);
                        try {
                            double ctime = Double.parseDouble(kv[1]);
                            ctime *= 1000; // seconds to milliseconds
                            contentLastModified = new Date((long) Math.floor(ctime));
                        } catch (NumberFormatException ex) {
                            throw new RuntimeException("failed to parse " + s + ": " + kv[1] + " is not a valid double", ex);
                        }
                        break;
                    default:
                        log.debug("skip: " + kv[0]);
                }
            } else {
                log.debug("skip token: " + s);
            }
        }

        if (csAlg != null && csHex != null) {
            contentChecksum = URI.create(csAlg + ":" + csHex);
        }
        StorageMetadata ret = new StorageMetadata(sloc, artifactURI, contentChecksum, contentLength, contentLastModified);
        return ret;
    }

    StorageLocation pathToStorageLocation(String rel) {
        int i = rel.lastIndexOf('/');
        String filename = rel.substring(i + 1);
        StorageLocation ret = new StorageLocation(URI.create(filename));
        ret.storageBucket = rel.substring(0, i);
        return ret;
    }

    // read-into-memory implementation
    private LineNumberReader readFully(URI mgmServer, String remotePath, String authToken) throws IOException {
        
        String str = "eos find -f --format path,size,checksumtype,checksum,ctime " + remotePath;
        final String[] cmd = str.split(" ");
        final Map<String, String> environment = new HashMap<>();
        environment.put("EOS_MGM_URL", mgmServer.toASCIIString());
        environment.put("EOSAUTHZ", authToken);
        
        log.warn("readFully: " + str);
        BuilderOutputGrabber exec = new BuilderOutputGrabber();
        exec.captureOutput(cmd, environment);
        log.warn("exit status: " + exec.getExitValue());
        
        if (exec.getExitValue() != 0) {
            throw new StorageEngageException("eos find failed (" + exec.getExitValue() + "): " + exec.getOutput());
        }

        return new LineNumberReader(new StringReader(exec.getOutput()));
    }

    // streaming implementation
    private void openStream(URI mgmServer, String remotePath, String authToken) throws IOException {
        final String str = "eos find -f --format path,size,checksumtype,checksum,ctime " + remotePath;
        List<String> parameters = Arrays.asList(str.split(" "));
        
        ProcessBuilder processBuilder = new ProcessBuilder(parameters);
        Map<String, String> environment = processBuilder.environment();
        environment.clear();
        environment.put("EOS_MGM_URL", mgmServer.toASCIIString());
        environment.put("EOSAUTHZ", authToken);

        log.warn("openStream: " + str);
        this.proc = processBuilder.start();
        this.istream = proc.getInputStream();
        this.estream = proc.getErrorStream();
        this.ostream = proc.getOutputStream();

        this.errThread = new ReaderThread(estream, stderr);
        errThread.start();
    }

    private void closeStream() throws IOException {
        if (errThread != null) {
            try {
                errThread.join();
                exitValue = proc.waitFor(); // block
            } catch (InterruptedException ignore) {
                log.debug("ignore: " + ignore);
            }
            if (errThread.ex != null) {
                stderr.append("exception while reading command error output:\n").append(errThread.ex.toString());
            }
        }

        if (istream != null) {
            try {
                istream.close();
            } catch (IOException ignore) {
                // do nothing
            }
        }

        if (estream != null) {
            try {
                estream.close();
            } catch (IOException ignore) {
                // do nothing
            }
        }

        if (ostream != null) {
            try {
                ostream.close();
            } catch (IOException ignore) {
                // do nothing
            }
        }
    }

    private class ReaderThread extends Thread {
        public Exception ex;
        private StringBuilder sb;
        private LineNumberReader reader;

        public ReaderThread(InputStream istream, StringBuilder sb) {
            this.reader = new LineNumberReader(new InputStreamReader(istream));
            this.sb = sb;
        }

        public void run() {
            try {
                String s;
                while ((s = reader.readLine()) != null) {
                    sb.append(s).append("\n");
                }
            } catch (Exception iex) {
                this.ex = iex;
            }
        }
    }
}

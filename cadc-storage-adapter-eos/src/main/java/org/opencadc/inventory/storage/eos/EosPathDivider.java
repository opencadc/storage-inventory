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

import ca.nrc.cadc.exec.BuilderOutputGrabber;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class EosPathDivider {
    private static final Logger log = Logger.getLogger(EosPathDivider.class);

    private final URI mgmServer;
    private final String mgmPath;
    private final String authToken;
    
    // optional storageBucketPrefix
    private final String pathPrefix;
    
    private final Path basePath;
    
    public static class SubpathInfo {
        String path;
        Integer numFiles;

        public SubpathInfo(String path, Integer numFiles) {
            this.path = path;
            this.numFiles = numFiles;
        }
    }
    
    public EosPathDivider(URI mgmServer, String mgmPath, String authToken, String pathPrefix) {
        this.mgmServer = mgmServer;
        this.mgmPath = mgmPath;
        this.authToken = authToken;
        this.pathPrefix = pathPrefix;
        this.basePath = Path.of(mgmPath);
    }
    
    public List<SubpathInfo> subdivide() throws IOException {
        // dir scan
        String path = mgmPath + "/" + pathPrefix;
        // TODO: useful to be able to start with a depth > 1 on the first call
        return subdivide(path); 
    }

    List<SubpathInfo> subdivide(String path) throws IOException {
        final Map<String, String> environment = new HashMap<>();
        environment.put("EOS_MGM_URL", mgmServer.toASCIIString());
        environment.put("EOSAUTHZ", authToken);
        
        // TODO: might be faster to try maxdepth > 1 here and adjust 
        // if too many subpaths or truncated
        int depth = 3; // try 3, 2, 1
        List<String> dirs = dirScan(environment, path, depth);
        while (dirs.isEmpty() && depth > 1) {
            // truncated
            depth--;
            dirs = dirScan(environment, path, depth);
        }
        // too many subpaths -> too many calls to eos newfind
        // if someone thought about scalability and used hex chars for dir names,
        // then we'd have 16 or 16*16 = 256 or 16*16*16 = 4096 subdirs
        // TODO: make limit configurable?
        while (dirs.size() > 4096 && depth > 1) {
            // too many
            depth--;
            dirs = dirScan(environment, path, depth);
        }
        log.debug("dir scan produced " + dirs.size() + " subpaths to count...");
        // the above scanning will fail if it encounters a flat dir a/b with more than 50k children
        // because both dir scan and file count will be truncated
        if (dirs.isEmpty()) {
            throw new RuntimeException("FAIL: dir scan resulted on no countable subpaths in " + path);
        }
        
        
        List<SubpathInfo> ret = new ArrayList<>();
        for (String subpath : dirs) {
            String count = "eos newfind -f --count " + subpath;
            log.info("file count: " + count);
            long t1 = System.currentTimeMillis();
            BuilderOutputGrabber proc = new BuilderOutputGrabber();
            proc.captureOutput(count.split(" "), environment);
            long dt = System.currentTimeMillis() - t1;
            switch (proc.getExitValue()) {
                case 7:
                    log.warn("count: " + count + " TRUNCATED " + dt + "ms");
                    // recursion
                    List<SubpathInfo> recurse = subdivide(subpath);
                    for (SubpathInfo spi : recurse) {
                        ret.add(spi);
                    }   
                    break;
                case 0:
                    log.info("file count: " + count + " OK " + dt + "ms");
                    // output: nfiles=X ndirectories=Y
                    String cr = proc.getOutput();
                    log.debug("file count raw: " + cr);
                    String[] split = cr.split("[= ]");
                    Integer nf = Integer.parseInt(split[1]);
                    Path full = Path.of(subpath);
                    Path rel = basePath.relativize(full);
                    SubpathInfo spi = new SubpathInfo(rel.toString(), nf);
                    ret.add(spi);
                    break;
                default:
                    throw new RuntimeException("unexpected exit code: " + proc.getExitValue() + " exec: " + count
                        + " cause:\n" + proc.getErrorOutput());
            }
        }
        
        return ret;
        
    }
    
    private List<String> dirScan(Map<String, String> environment, String path, int depth) throws IOException {
        String scan = "eos newfind -d --maxdepth " + depth + " " + path;
        log.info("dir scan: " + scan);
        
        List<String> ret = new ArrayList<>();
        
        long t1 = System.currentTimeMillis();
        BuilderOutputGrabber proc = new BuilderOutputGrabber();
        proc.captureOutput(scan.split(" "), environment);
        long dt = System.currentTimeMillis() - t1;
        switch (proc.getExitValue()) {
            case 7:
                log.warn("dir scan: " + scan + " TRUNCATED " + dt + "ms");
                return ret; // empty
            case 0:
                log.info("dir scan: " + scan + " OK " + dt + "ms");
                LineNumberReader reader = new LineNumberReader(new StringReader(proc.getOutput()));
                String line = reader.readLine();
                String next = reader.readLine();
                while (line != null) {
                    log.debug("scan: " + line + " START");
                    if (next != null && next.startsWith(line)) {
                        log.debug("skip: " + line);
                    } else {
                        log.debug("keep: " + line);
                        ret.add(line);
                    }
                    log.debug("scan: " + line + " DONE");
                    line = next;
                    next = reader.readLine();
                }   
                break;
            default:
                throw new RuntimeException("unexpected exit code: " + proc.getExitValue() 
                    + " exec: " + scan
                    + " cause:\n" + proc.getErrorOutput());
        }
        
        return ret;
    }
}

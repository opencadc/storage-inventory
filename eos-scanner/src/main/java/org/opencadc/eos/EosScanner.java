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

package org.opencadc.eos;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.InvalidConfigException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.storage.DuplicateStorageLocationException;
import org.opencadc.inventory.storage.InitDatabaseSM;
import org.opencadc.inventory.storage.StorageMetadata;
import org.opencadc.inventory.storage.StorageMetadataDAO;
import org.opencadc.inventory.storage.eos.EosPathDivider;
import org.opencadc.inventory.storage.eos.EosPathIterator;
import org.opencadc.inventory.storage.eos.EosStorageAdapter;
import org.opencadc.inventory.storage.eos.SubPath;

/**
 *
 * @author pdowler
 */
public class EosScanner {
    private static final Logger log = Logger.getLogger(EosScanner.class);

    private final String bucket;
    private final StorageMetadataDAO dao;
    private final EosStorageAdapter eos;
    
    public final boolean dirScanOnly;
    
    public EosScanner(String pathPrefix, boolean dirScanOnly) {
        this.bucket = pathPrefix;
        this.eos = new EosStorageAdapter(); // reads config
        
        // db setup
        this.dao = new StorageMetadataDAO();
        dao.setConfig(eos.initDataSource());
        InitDatabaseSM init = new InitDatabaseSM(dao.getDataSource(), dao.getSchema());
        init.doInit();
        
        this.dirScanOnly = dirScanOnly;
    }
    
    public void doit() throws IOException {
        // subdivide EOS into scannable chunks below find truncation limit
        List<SubPath> dirs = doDirScan();
        if (dirScanOnly) {
            log.info("dir scan found: " + dirs.size() + " subpaths");
            return;
        }

        // scan eos for file metadata and store in db
        EosPathIterator iter = eos.getPathIterator(dirs);
        while (iter.hasNext()) {
            StorageMetadata sm = iter.next();
            log.info("found: " + sm);
            try {
                dao.put(sm);
            } catch (DuplicateStorageLocationException ex) {
                log.warn("skip: " + ex.getMessage());
            }
        }
    }
    
    private List<SubPath> doDirScan() throws IOException {
        // read list from file instead of scan
        File wd = new File("/work");
        File cf = new File(wd, "dir-scan.txt");
        if (bucket != null) {
            cf = new File(wd, bucket + "-dir-scan.txt");
        }
        
        if (cf.exists()) {
            List<SubPath> ret = new ArrayList<>();
            LineNumberReader r = new LineNumberReader(new FileReader(cf));
            String line = r.readLine();
            while (line != null) {
                SubPath sp = SubPath.fromString(line);
                ret.add(sp);
                line = r.readLine();
            }
            r.close();
            log.info("dir scan: using cache from previous run: " + cf.getAbsolutePath());
            return ret;
        }
        
        EosPathDivider eos = this.eos.getPathDivider(bucket);
        List<SubPath> ret = eos.subdivide();
        
        // write list to file for next time
        PrintWriter w = new PrintWriter(new FileWriter(cf));
        for (SubPath sp : ret) {
            w.println(SubPath.toString(sp));
        }
        w.flush();
        w.close();
        log.info("dir scan: wrote scan result: " + cf.getAbsolutePath());
        return ret;
    }
}

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

    private final String pathPrefix;
    private final StorageMetadataDAO dao;
    private final EosStorageAdapter eos;
    
    public final boolean dirScanOnly;
    
    public EosScanner(String pathPrefix, Map<String,Object> daoConfig, ConnectionConfig cc,
            boolean dirScanOnly) {
        this.pathPrefix = pathPrefix;
        this.eos = new EosStorageAdapter(); // reads config
        
        // db setup
        try {
            String dsName = "jdbc/storage-metadata";
            daoConfig.put("jndiDataSourceName", dsName);
            DBUtil.createJNDIDataSource(dsName, cc);
            this.dao = new StorageMetadataDAO();
            dao.setConfig(daoConfig);
            
            DataSource ds = DBUtil.findJNDIDataSource(dsName);
            InitDatabaseSM init = new InitDatabaseSM(dao.getDataSource(), dao.getSchema());
            init.doInit();
        } catch (NamingException ex) {
            throw new InvalidConfigException("failed to setup database connectivity", ex);
        }
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
            dao.put(sm);
        }
    }
    
    private List<SubPath> doDirScan() throws IOException {
        // read list from file instead of scan
        File f = new File("/work/dir-scan.txt");
        if (f.exists()) {
            List<SubPath> ret = new ArrayList<>();
            LineNumberReader r = new LineNumberReader(new FileReader(f));
            String line = r.readLine();
            while (line != null) {
                SubPath sp = SubPath.fromString(line);
                ret.add(sp);
                line = r.readLine();
            }
            r.close();
            return ret;
        }
        
        EosPathDivider eos = this.eos.getPathDivider(pathPrefix);
        List<SubPath> ret = eos.subdivide();
        
        // write list to file for next time
        PrintWriter w = new PrintWriter(new FileWriter(f));
        for (SubPath sp : ret) {
            w.println(SubPath.toString(sp));
        }
        w.flush();
        w.close();
        
        return ret;
    }
}

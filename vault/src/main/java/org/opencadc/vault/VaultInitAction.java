/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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

package org.opencadc.vault;

import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.rest.InitAction;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.db.SQLGenerator;

/**
 *
 * @author pdowler
 */
public class VaultInitAction extends InitAction {
    private static final Logger log = Logger.getLogger(VaultInitAction.class);
    
    static final String JNDI_DATASOURCE = "jdbc/nodes"; // context.xml
    
    // config keys
    private static final String VAULT_KEY = "org.opencadc.vault";
    static final String RESOURCE_ID_KEY = VAULT_KEY + ".resourceID";
    static final String SQLGEN_KEY = SQLGenerator.class.getName();
    static final String SCHEMA_KEY = VAULT_KEY + ".nodes.schema";
    
    MultiValuedProperties props;
    private URI resourceID;
    private Map<String,Object> daoConfig;

    public VaultInitAction() {
        super();
    }

    @Override
    public void doInit() {
        initConfig();
        initDatabase();
    }
    
    /**
     * Read config file and verify that all required entries are present.
     * 
     * @return MultiValuedProperties containing the application config
     * @throws IllegalStateException if required config items are missing
     */
    static MultiValuedProperties getConfig() {
        PropertiesReader r = new PropertiesReader("vault.properties");
        MultiValuedProperties mvp = r.getAllProperties();

        StringBuilder sb = new StringBuilder();
        sb.append("incomplete config: ");
        boolean ok = true;

        String rid = mvp.getFirstPropertyValue(RESOURCE_ID_KEY);
        sb.append("\n\t" + RESOURCE_ID_KEY + ": ");
        if (rid == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }

        String sqlgen = mvp.getFirstPropertyValue(SQLGEN_KEY);
        sb.append("\n\t").append(SQLGEN_KEY).append(": ");
        if (sqlgen == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            try {
                Class c = Class.forName(sqlgen);
                sb.append("OK");
            } catch (ClassNotFoundException ex) {
                sb.append("class not found: " + sqlgen);
                ok = false;
            }
        }

        String schema = mvp.getFirstPropertyValue(SCHEMA_KEY);
        sb.append("\n\t").append(SCHEMA_KEY).append(": ");
        if (schema == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }

        if (!ok) {
            throw new IllegalStateException(sb.toString());
        }

        return mvp;
    }
    
    static Map<String,Object> getDaoConfig(MultiValuedProperties props) {
        String cname = props.getFirstPropertyValue(SQLGenerator.class.getName());
        try {
            Map<String,Object> ret = new TreeMap<>();
            Class clz = Class.forName(cname);
            ret.put(SQLGenerator.class.getName(), clz);
            ret.put("jndiDataSourceName", org.opencadc.vault.VaultInitAction.JNDI_DATASOURCE);
            ret.put("schema", props.getFirstPropertyValue(org.opencadc.vault.VaultInitAction.SCHEMA_KEY));
            //config.put("database", null);
            return ret;
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("invalid config: failed to load SQLGenerator: " + cname);
        }
    }
    
    private void initConfig() {
        log.info("initConfig: START");
        this.props = getConfig();
        String rid = props.getFirstPropertyValue(RESOURCE_ID_KEY);
        
        try {
            this.resourceID = new URI(rid);
            this.daoConfig = getDaoConfig(props);
            log.info("initConfig: OK");
        } catch (URISyntaxException ex) {
            throw new IllegalStateException("invalid config: " + RESOURCE_ID_KEY + " must be a valid URI");
        }
    }
    
    private void initDatabase() {
        log.info("initDatabase: START");
        try {
            DataSource ds = DBUtil.findJNDIDataSource(JNDI_DATASOURCE);
            String database = (String) daoConfig.get("database");
            String schema = (String) daoConfig.get("schema");
            VaultInitDatabase init = new VaultInitDatabase(ds, database, schema);
            init.doInit();
            log.info("initDatabase: " + JNDI_DATASOURCE + " " + schema + " OK");
        } catch (Exception ex) {
            throw new IllegalStateException("check/init database failed", ex);
        }
    }

}
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

package org.opencadc.minoc;

import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.db.SQLGenerator;
import org.opencadc.inventory.storage.StorageAdapter;

/**
 * Configuration object that can be stored in JNDI and used by actions.
 * 
 * @author pdowler
 */
public class MinocConfig {
    private static final Logger log = Logger.getLogger(MinocConfig.class);

    static final String JNDI_DATASOURCE = "jdbc/inventory"; // context.xml
    
    // config keys
    private static final String MINOC_KEY = "org.opencadc.minoc";
    static final String RESOURCE_ID_KEY = MINOC_KEY + ".resourceID";
    static final String SQLGEN_KEY = SQLGenerator.class.getName();
    static final String SCHEMA_KEY = MINOC_KEY + ".inventory.schema";
    static final String SA_KEY = StorageAdapter.class.getName();
    static final String TRUST_KEY = MINOC_KEY + ".trust.preauth";
    static final String READ_GRANTS_KEY = MINOC_KEY + ".readGrantProvider";
    static final String WRITE_GRANTS_KEY = MINOC_KEY + ".writeGrantProvider";
    static final String READABLE_KEY = MINOC_KEY + ".readable";
    static final String WRITABLE_KEY = MINOC_KEY + ".writable";
    static final String RECOVERABLE_NS_KEY = MINOC_KEY + ".recoverableNamespace";
    static final String ARCHIVE_OPS_KEY = MINOC_KEY + ".archiveOperatorX509";
    static final String FILE_SYNC_OPS_KEY = MINOC_KEY + ".fileSyncOperatorX509";
    
    static final String DEV_AUTH_ONLY_KEY = MINOC_KEY + ".authenticateOnly";
    
    private final MultiValuedProperties configProperties;
    
    private final Map<URI,byte[]> trustedServices = new TreeMap<>();
    private final List<URI> readGrantServices = new ArrayList<>();
    private final List<URI> writeGrantServices = new ArrayList<>();
    private final boolean readable;
    private final boolean writable;
    private final List<Namespace> recoverableNamespaces = new ArrayList<>();
    
    final Set<X500Principal> archiveOperators = new TreeSet<>(new X500PrincipalComparator());
    final Set<X500Principal> fileSyncOperators = new TreeSet<>(new X500PrincipalComparator());
    
    final boolean authenticateOnly;
    
    public MinocConfig() {
        PropertiesReader r = new PropertiesReader("minoc.properties");
        this.configProperties = r.getAllProperties();
        
        validateConfigProps(configProperties);
        
        // from here on, fail on invalid config
        List<String> readGrants = configProperties.getProperty(READ_GRANTS_KEY);
        if (readGrants != null) {
            for (String s : readGrants) {
                try {
                    URI u = new URI(s);
                    readGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + READ_GRANTS_KEY + "=" + s + " INVALID", ex);
                }
            }
        }

        List<String> writeGrants = configProperties.getProperty(WRITE_GRANTS_KEY);
        if (writeGrants != null) {
            for (String s : writeGrants) {
                try {
                    URI u = new URI(s);
                    writeGrantServices.add(u);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + WRITE_GRANTS_KEY + "=" + s + " INVALID", ex);
                }
            }
        }
        
        List<String> aops = configProperties.getProperty(ARCHIVE_OPS_KEY);
        if (aops != null) {
            for (String s : aops) {
                try {
                    X500Principal p = new X500Principal(s);
                    archiveOperators.add(p);
                } catch (Exception ex) {
                    throw new IllegalStateException("invalid config: " + ARCHIVE_OPS_KEY + "=" + s + " INVALID", ex);
                }
            }
        }
        
        List<String> fsus = configProperties.getProperty(FILE_SYNC_OPS_KEY);
        if (fsus != null) {
            for (String s : fsus) {
                try {
                    X500Principal p = new X500Principal(s);
                    fileSyncOperators.add(p);
                } catch (Exception ex) {
                    throw new IllegalStateException("invalid config: " + FILE_SYNC_OPS_KEY + "=" + s + " INVALID", ex);
                }
            }
        }
        
        String ao = configProperties.getFirstPropertyValue(DEV_AUTH_ONLY_KEY);
        if (ao != null) {
            try {
                this.authenticateOnly = Boolean.valueOf(ao);
                if (authenticateOnly) {
                    log.warn("(configuration) authenticateOnly = " + authenticateOnly);
                }
            } catch (Exception ex) {
                throw new IllegalStateException("invalid config: " + DEV_AUTH_ONLY_KEY + "=" + ao + " must be true|false or not set");
            }
        } else {
            authenticateOnly = false;
        }

        List<String> trusted = configProperties.getProperty(TRUST_KEY);
        if (trusted != null) {
            for (String s : trusted) {
                try {
                    URI u = new URI(s);
                    trustedServices.put(u, null);
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("invalid config: " + TRUST_KEY + "=" + s + " INVALID", ex);
                }
            }
            // try to sync keys on startup
            syncKeys();
        }
        
        List<String> recov = configProperties.getProperty(RECOVERABLE_NS_KEY);
        if (recov != null) {
            for (String s : recov) {
                try {
                    Namespace ns = new Namespace(s);
                    recoverableNamespaces.add(ns);
                } catch (Exception ex) {
                    throw new IllegalStateException("invalid config: " + RECOVERABLE_NS_KEY + "=" + s + " INVALID", ex);
                }
            }
        }
        
        // optional
        String sread = configProperties.getFirstPropertyValue(MinocConfig.READABLE_KEY);
        if (sread != null) {
            this.readable = Boolean.parseBoolean(sread);
        } else {
            this.readable = !readGrantServices.isEmpty() || !trustedServices.isEmpty();
        }
        String swrite = configProperties.getFirstPropertyValue(MinocConfig.WRITABLE_KEY);
        if (swrite != null) {
            this.writable = Boolean.parseBoolean(swrite);
        } else {
            this.writable = !writeGrantServices.isEmpty() || !trustedServices.isEmpty();
        }
    }
 
    private void validateConfigProps(MultiValuedProperties mvp) {
        // plain validate once at startup
        StringBuilder sb = new StringBuilder();
        sb.append("incomplete config: ");
        boolean ok = true;

        String rid = mvp.getFirstPropertyValue(MinocConfig.RESOURCE_ID_KEY);
        sb.append("\n\t" + RESOURCE_ID_KEY + ": ");
        if (rid == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }
        
        String sac = mvp.getFirstPropertyValue(SA_KEY);
        sb.append("\n\t").append(SA_KEY).append(": ");
        if (sac == null) {
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
        
        // optional
        List<String> trusted = mvp.getProperty(TRUST_KEY);
        if (trusted != null) {
            for (String s : trusted) {
                sb.append("\n\t").append(TRUST_KEY + "=").append(s);
                try {
                    URI uri = new URI(s);
                    sb.append(" OK");
                } catch (URISyntaxException ex) {
                    sb.append(" INVALID");
                    ok = false;
                }
            }
        }
        
        // optional
        List<String> readGrants = mvp.getProperty(READ_GRANTS_KEY);
        if (readGrants != null) {
            for (String s : readGrants) {
                sb.append("\n\t").append(READ_GRANTS_KEY + "=").append(s);
                try {
                    URI u = new URI(s);
                    sb.append(" OK");
                } catch (URISyntaxException ex) {
                    sb.append(" INVALID");
                    ok = false;
                }
            }
        }

        // optional
        List<String> writeGrants = mvp.getProperty(WRITE_GRANTS_KEY);
        if (writeGrants != null) {
            for (String s : writeGrants) {
                sb.append("\n\t").append(WRITE_GRANTS_KEY + "=").append(s);
                try {
                    URI u = new URI(s);
                    sb.append(" OK");
                } catch (URISyntaxException ex) {
                    sb.append(" INVALID");
                    ok = false;
                }
            }
        }
        
        // optional
        String sread = mvp.getFirstPropertyValue(MinocConfig.READABLE_KEY);
        sb.append("\n\t" + READABLE_KEY + ": ");
        if (sread != null) {
            if ("true".equals(sread) || "false".equals(sread)) {
                sb.append(" OK");
            } else {
                sb.append(" INVALID");
                ok = false;
            }
        }
        
        // optional
        String swrite = mvp.getFirstPropertyValue(MinocConfig.WRITABLE_KEY);
        sb.append("\n\t" + WRITABLE_KEY + ": ");
        if (sread != null) {
            if ("true".equals(swrite) || "false".equals(swrite)) {
                sb.append(" OK");
            } else {
                sb.append(" INVALID");
                ok = false;
            }
        }
        
        // optional
        List<String> rawRecNS = mvp.getProperty(RECOVERABLE_NS_KEY);
        if (rawRecNS != null) {
            for (String s : rawRecNS) {
                sb.append("\n\t").append(RECOVERABLE_NS_KEY + "=").append(s);
                try {
                    Namespace ns = new Namespace(s);
                } catch (Exception ex) {
                    sb.append(" INVALID");
                }
            }
        }

        if (!ok) {
            throw new InvalidConfigException(sb.toString());
        }
    }
    
    public MultiValuedProperties getProperties() {
        return configProperties;
    }

    public Map<URI, byte[]> getTrustedServices() {
        // check and try to sync missing keys before request
        syncKeys();
        return trustedServices;
    }
    
    private void syncKeys() {
        RegistryClient reg = new RegistryClient();
        // check map for null keys and try to retrieve them
        // ASSUMPTION: keys never change once generated so if they do then minoc
        //             needs to be restarted
        for (Map.Entry<URI,byte[]> me : trustedServices.entrySet()) {
            if (me.getValue() == null) {
                try {
                    log.info("get trusted pubkey: " + me.getKey());
                    URL capURL = reg.getAccessURL(RegistryClient.Query.CAPABILITIES, me.getKey());
                    String s = capURL.toExternalForm().replace("/capabilities", "/pubkey");
                    URL keyURL = new URL(s);
                    log.info("get trusted pubkey: " + me.getKey() + " -> " + keyURL);
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    HttpGet get = new HttpGet(keyURL, bos);
                    get.setConnectionTimeout(6000);
                    get.setReadTimeout(6000);
                    get.setRetry(0, 0, HttpTransfer.RetryReason.NONE);
                    get.run();
                    if (get.getThrowable() != null) {
                        throw (Exception) get.getThrowable();
                    }
                    byte[] key = bos.toByteArray();
                    trustedServices.put(me.getKey(), key);
                    log.info("get trusted pubkey: " + me.getKey() + " OK");
                } catch (Exception ex) {
                    log.warn("failed to get public key from " + me.getKey() + ": " + ex);
                }
            }
        }
    }

    public boolean isReadable() {
        return readable;
    }

    public boolean isWritable() {
        return writable;
    }
    
    public List<URI> getReadGrantServices() {
        return readGrantServices;
    }

    public List<URI> getWriteGrantServices() {
        return writeGrantServices;
    }

    public boolean isAuthenticateOnly() {
        return authenticateOnly;
    }
    
    public List<Namespace> getRecoverableNamespaces() {
        return recoverableNamespaces;
    }
    
    public StorageAdapter getStorageAdapter() {
        String cname = configProperties.getFirstPropertyValue(MinocConfig.SA_KEY);
        StorageAdapter storageAdapter = InventoryUtil.loadPlugin(cname);
        if (!recoverableNamespaces.isEmpty()) {
            storageAdapter.setRecoverableNamespaces(recoverableNamespaces);
        }
        return storageAdapter;
    }
    
    public Map<String,Object> getDaoConfig() {
        String cname = configProperties.getFirstPropertyValue(SQLGenerator.class.getName());
        try {
            Map<String,Object> ret = new TreeMap<>();
            Class clz = Class.forName(cname);
            ret.put(SQLGenerator.class.getName(), clz);
            ret.put("jndiDataSourceName", JNDI_DATASOURCE);
            ret.put("invSchema", configProperties.getFirstPropertyValue(SCHEMA_KEY));
            ret.put("genSchema", configProperties.getFirstPropertyValue(SCHEMA_KEY));
            return ret;
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("invalid config: failed to load SQLGenerator: " + cname);
        }
    }
}

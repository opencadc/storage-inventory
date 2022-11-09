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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.luskan;

import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import java.net.URI;
import java.util.List;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;

public class LuskanConfig {
    private static final Logger log = Logger.getLogger(LuskanConfig.class);

    // config keys
    private static final String LUSKAN_KEY = LuskanConfig.class.getPackage().getName();
    public static final String STORAGE_SITE_KEY = LUSKAN_KEY + ".isStorageSite";
    public static final String ALLOW_ANON = LUSKAN_KEY + ".allowAnon";
    public static final String ALLOWED_GROUP = LUSKAN_KEY + ".allowedGroup";
    
    // dev use only
    public static final String DISABLE_FILTERS = LUSKAN_KEY + ".disableQueryFilters";

    public LuskanConfig() {

    }

    /**
     * Verify the config file is valid and the resourceID value is a valid URI.
     */
    public static void initConfig() {
        log.debug("initConfig: START");
        MultiValuedProperties props = getConfig();
        
    }

    /**
     * Read config file and verify that all required entries are present.
     *
     * @return MultiValuedProperties containing the application config
     * @throws IllegalStateException if required config items are missing
     */
    public static MultiValuedProperties getConfig() {
        PropertiesReader r = new PropertiesReader("luskan.properties");
        MultiValuedProperties props = r.getAllProperties();

        StringBuilder sb = new StringBuilder();
        sb.append("incomplete config: ");
        boolean ok = true;

        String ssk = props.getFirstPropertyValue(STORAGE_SITE_KEY);
        sb.append("\n\t").append(STORAGE_SITE_KEY).append(" - ");
        if (ssk == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }
        sb.append("\n");

        // optional
        sb.append("\n\t").append(ALLOW_ANON).append(" - ");
        String anonStr = props.getFirstPropertyValue(ALLOW_ANON);
        if (anonStr == null) {
            sb.append("DEFAULT");
        } else if ("false".equals(anonStr) || "true".equals(anonStr)) {
            sb.append("OK");
        } else {
            sb.append("INVALID: " + anonStr);
        }
        
        List<String> allowedGroups = props.getProperty(ALLOWED_GROUP);
        for (String allowedGroup : allowedGroups) {
            sb.append("\n\t").append(ALLOWED_GROUP).append(" - ").append(allowedGroup);
            try {
                new GroupURI(URI.create(allowedGroup));
                sb.append(" OK");
            } catch (IllegalArgumentException e) {
                sb.append(" INVALID");
                ok = false;
            }
        }
        
        if (!ok) {
            throw new IllegalStateException(sb.toString());
        }

        return props;
    }
}

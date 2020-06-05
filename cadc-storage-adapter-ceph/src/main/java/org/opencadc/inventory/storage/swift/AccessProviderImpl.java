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

package org.opencadc.inventory.storage.swift;

import ca.nrc.cadc.net.HttpGet;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import org.apache.log4j.Logger;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.client.factory.TempUrlHashPrefixSource;
import org.javaswift.joss.model.Access;

/**
 * Implementation of a javaswift AccessProvider for AuthenticationMethod.EXTERNAL mode.
 * This is not used but retained in case someone wants to know how Swift Auth v1 works,
 * as a starting point to authenticate vs some API not supported by javaswift,
 * and/or needs to work around some sort of auth issue as it currently one does.
 * 
 * @author pdowler
 */
public class AccessProviderImpl implements AuthenticationMethod.AccessProvider {
    private static final Logger log = Logger.getLogger(AccessProviderImpl.class);

    private final URL authURL;
    private final String user;
    private final String  key;
    
    public AccessProviderImpl(URL authURL, String user, String key) { 
        this.authURL = authURL;
        this.user = user;
        this.key = key;
    }

    @Override
    public Access authenticate() {
        log.debug("AccessProvider.authenticate - START");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HttpGet login = new HttpGet(authURL, bos);
        login.setFollowRedirects(true);
        login.setRequestProperty("X-Auth-User", user);
        login.setRequestProperty("X-Auth-Key", key);
        login.run();
        log.debug("auth: " + login.getResponseCode() + " " + login.getContentType());
        if (login.getThrowable() != null) {
            throw new RuntimeException("auth failure", login.getThrowable());
        }
        String storageURL = login.getResponseHeader("X-Storage-Url");
        //String storageToken = login.getResponseHeader("X-Storage-Token");
        String authToken = login.getResponseHeader("X-Auth-Token");
        log.debug("storageURL: " + storageURL);
        //log.debug("storageToken: " + storageToken);
        log.debug("authToken: " + authToken);
        log.debug("body: " + bos.toString());

        AccessWorkaroundHack ret = new AccessWorkaroundHack();
        ret.storageURL = storageURL.replace("http://", "https://").replace(":8080", "");
        ret.token = authToken;

        log.debug("AccessProvider.authenticate - DONE");
        return ret;
    }
    
    private static class AccessWorkaroundHack implements Access {

        private String token;
        private String storageURL;
        
        @Override
        public void setPreferredRegion(String string) {
            //ignore
        }

        @Override
        public String getToken() {
            return token;
        }

        @Override
        public String getInternalURL() {
            return null;
        }

        @Override
        public String getPublicURL() {
            return storageURL;
        }

        @Override
        public boolean isTenantSupplied() {
            return true; // via {username}:{tenant}
        }

        @Override
        public String getTempUrlPrefix(TempUrlHashPrefixSource tuhps) {
            return null;
        }
        
    }
}

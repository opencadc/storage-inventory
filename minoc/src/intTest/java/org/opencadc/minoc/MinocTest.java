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
 ************************************************************************
 */

package org.opencadc.minoc;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Abstract integration test class with general setup and test support.
 * 
 * @author majorb
 */
public abstract class MinocTest {
    
    private static final Logger log = Logger.getLogger(BasicOpsTest.class);
    public static final URI MINOC_SERVICE_ID = URI.create("ivo://cadc.nrc.ca/minoc");
    
    protected URL anonURL;
    protected URL certURL;
    protected Subject anonSubject;
    protected Subject userSubject;
    
    static {
        Log4jInit.setLevel("org.opencadc.minoc", Level.INFO);
    }
    
    public MinocTest() {
        RegistryClient regClient = new RegistryClient();
        anonURL = regClient.getServiceURL(MINOC_SERVICE_ID, Standards.SI_FILES, AuthMethod.ANON);
        log.info("anonURL: " + anonURL);
        certURL = regClient.getServiceURL(MINOC_SERVICE_ID, Standards.SI_FILES, AuthMethod.CERT);
        log.info("certURL: " + certURL);
        anonSubject = AuthenticationUtil.getAnonSubject();
        File cert = FileUtil.getFileFromResource("minoc-test.pem", MinocTest.class);
        log.debug("anonSubject: " + anonSubject);
        userSubject = SSLUtil.createSubject(cert);
        log.debug("userSubject: " + userSubject);
    }
    
    protected static String computeMD5(byte[] input) throws NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        InputStream in = new ByteArrayInputStream(input);
        DigestInputStream dis = new DigestInputStream(in, md);
        int bytesRead = dis.read();
        byte[] buf = new byte[512];
        while (bytesRead > 0) {
            bytesRead = dis.read(buf);
        }
        byte[] digest = md.digest();
        return HexUtil.toHex(digest);
    }

}

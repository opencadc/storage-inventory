/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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

package ca.nrc.cadc.data;

import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.util.CaseInsensitiveStringComparator;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferReader;
import ca.nrc.cadc.vos.TransferWriter;
import ca.nrc.cadc.vos.VOS;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

/**
 * Redirect caller to a download URL.
 * 
 * @author pdowler
 *
 */
public class GetAction extends BaseAction {
    
    protected static Logger log = Logger.getLogger(GetAction.class);
    
    private static final Map<String,String> PARAM_TRANSFORMS = new TreeMap<>(new CaseInsensitiveStringComparator());
    
    static {
        // SI files API pass through
        PARAM_TRANSFORMS.put("meta", "META");
        PARAM_TRANSFORMS.put("sub", "SUB");
        // data pub API transform
        PARAM_TRANSFORMS.put("fhead", "META");
        PARAM_TRANSFORMS.put("cutout", "SUB");
    }
    
    public GetAction() {
        super();
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

    @Override
    public void doAction() throws Exception {
        List<URI> uris = getURIs();
        boolean doAuthRedirect = false;
        
        CredUtil.checkCredentials();
        
        for (URI uri : uris) {
            log.warn("try: " + uri);
            // request all protocols that can be used
            List<Protocol> protocolList = new ArrayList<>();
            protocolList.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));
            protocolList.add(new Protocol(VOS.PROTOCOL_HTTP_GET));
            // TODO: add explicit request for pre-auth URL here

            Transfer transfer = new Transfer(uri, Direction.pullFromVoSpace);
            transfer.version = VOS.VOSPACE_21;
            transfer.getProtocols().addAll(protocolList);

            TransferWriter writer = new TransferWriter();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writer.write(transfer, out);
            FileContent content = new FileContent(out.toByteArray(), "text/xml");

            // NOTE: this relies on the fact that POST /raven/locate does not
            // redirect and returns contetn in the response... we need follow==false
            // because HttpPost refuses when subject contains cookie credentials
            // see: HttpTransfer for details
            HttpPost post = new HttpPost(ravenLocateURL, content, false);
            post.setConnectionTimeout(6000); // ms
            post.setReadTimeout(60000);      // ms
            
            try {
                post.prepare();
                log.debug("post prepare done");

                TransferReader reader = new TransferReader();
                Transfer t = reader.read(post.getInputStream(), null);
                List<String> endpoints = t.getAllEndpoints();
                if (!endpoints.isEmpty()) {
                    String surl = endpoints.get(0);
                    
                    StringBuilder sb = new StringBuilder();
                    Map<String,List<String>> params = getTransformParams();
                    if (!params.isEmpty()) {
                        String separator = "?";
                        for (Map.Entry<String,List<String>> me : params.entrySet()) {
                            if (!me.getValue().isEmpty()) {
                                for (String v : me.getValue()) {
                                    if (v.length() > 0) {
                                        sb.append(separator).append(me.getKey()).append("=").append(NetUtil.encode(v));
                                        separator = "&";
                                    }
                                }
                            }
                        }
                        if (sb.length() > 0) {
                            surl = surl + sb.toString();
                        }
                    }
                    
                    syncOutput.setCode(303);
                    syncOutput.setHeader("location", surl);
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                log.warn("not found: " + uri);
            } catch (AccessControlException | NotAuthenticatedException ex) {
                doAuthRedirect = true;
                break; // out of for loop
            }
        }
        
        if (doAuthRedirect) {
            fireAuthRedirect();
            return;
        }
        
        fireNotFound();
    }

    Map<String,List<String>> getTransformParams() {
        Map<String,List<String>> ret = new TreeMap<>();
        
        for (Map.Entry<String,String> trans : PARAM_TRANSFORMS.entrySet()) {
            List<String> vals = syncInput.getParameters(trans.getKey());
            if (vals == null) {
                log.debug("param: " + trans.getKey() + " values: null");
            } else {
                log.debug("param: " + trans.getKey() + " values: " + vals.size());
                if (!vals.isEmpty()) {
                    ret.put(trans.getValue(), vals);
                }
            }
        }
        
        return ret;
    }
}

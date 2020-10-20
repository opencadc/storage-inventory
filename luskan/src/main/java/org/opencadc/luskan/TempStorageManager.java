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
 *  $Revision: 6 $
 *
 ************************************************************************
 */

package org.opencadc.luskan;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.dali.tables.TableWriter;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.tap.ResultStore;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.web.InlineContentException;
import ca.nrc.cadc.uws.web.UWSInlineContentHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.sql.ResultSet;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.opencadc.luskan.ws.QueryJobManager;

/**
 * Basic temporary storage implementation for a tap service.
 *
 * @author pdowler, adapted by adriand
 */
public class TempStorageManager implements ResultStore, UWSInlineContentHandler {

    private static final Logger log = Logger.getLogger(TempStorageManager.class);
    
    private final File resultsDir;
    private String baseResultsURL;

    private Job job;
    private String filename;

    // singleton executor service
    private static ScheduledExecutorService SCHEDULER = null;

    public TempStorageManager() {
        try {
            MultiValuedProperties props = LuskanConfig.getConfig();

            String suri = props.getFirstPropertyValue(LuskanConfig.URI_KEY);
            log.info("system property: " + LuskanConfig.URI_KEY + " = " + suri);
            URI luskan = new URI(suri);
            
            RegistryClient regClient = new RegistryClient();
            URL serviceURL = regClient.getServiceURL(luskan, Standards.TAP_10, AuthMethod.CERT);
            
            // NOTE: "results" is used in the servlet mapping in web.xml
            this.baseResultsURL = serviceURL.toExternalForm() + "/results";
            log.info("resultsURL: " + baseResultsURL);

            String srd = props.getFirstPropertyValue(LuskanConfig.TMPDIR_KEY);
            this.resultsDir = new File(srd);
            resultsDir.mkdirs();
            log.info("resultsDir: " + resultsDir.getCanonicalPath());
            
        } catch (Exception ex) {
            log.error("CONFIG: failed to load/read config from system properties", ex);
            throw new RuntimeException("CONFIG: failed to load/read config from system properties", ex);
        }
    }

    // used by TempStorageGetAction
    File getStoredFile(String filename) {
        return new File(resultsDir, filename);
    }

    // cadc-tap-server ResultStore implementation
    public URL put(ResultSet rs, TableWriter<ResultSet> writer)
            throws IOException {
        return put(rs, writer, null);
    }

    public URL put(ResultSet rs, TableWriter<ResultSet> writer, Integer maxRows)
            throws IOException {
        Long num = null;
        if (maxRows != null) {
            num = new Long(maxRows);
        }

        File dest = getDestFile();
        URL ret = getURL();
        try (FileOutputStream ostream = new FileOutputStream(dest)) {
            writer.write(rs, ostream, num);
        }
        // TODO: store requested content-type with file so that 
        // TempStorageGetAction can set content-type header correctly

        scheduleDeletion(dest);
        return ret;
    }

    public URL put(Throwable t, TableWriter writer) throws IOException {
        File dest = getDestFile();
        URL ret = getURL();
        try (FileOutputStream ostream = new FileOutputStream(dest)) {
            writer.write(t, ostream);
        }
        // TODO: store requested content-type with file so that 
        // TempStorageGetAction can set content-type header correctly

        scheduleDeletion(dest);
        return ret;
    }

    @Override
    public void setJob(Job job) {
        this.job = job;
    }

    @Override
    public void setContentType(String contentType) {
        // TODO: store and set xattr on the result file once written?
    }

    @Override
    public void setFilename(String filename) {
        this.filename = filename;
    }

    private File getDestFile() {
        if (!resultsDir.exists()) {
            resultsDir.mkdirs();
        }
        return new File(resultsDir, filename);
    }

    private URL getURL() {
        StringBuilder sb = new StringBuilder();
        sb.append(baseResultsURL);

        if (!baseResultsURL.endsWith("/")) {
            sb.append("/");
        }

        sb.append(filename);
        String s = sb.toString();
        try {
            return new URL(s);
        } catch (MalformedURLException ex) {
            throw new RuntimeException("failed to create URL from " + s, ex);
        }
    }

    // cadc-uws-server UWSInlineContentHandler implementation

    @Override
    public Content accept(String name, String contentType, InputStream inputStream)
            throws InlineContentException, IOException {
        // store the file in tmp storage
        log.debug("name: " + name);
        log.debug("Content-Type: " + contentType);
        if (inputStream == null) {
            throw new IOException("InputStream cannot be null");
        }

        String filename = name + "-" + getRandomString();

        File put = new File(resultsDir, filename);

        log.debug("put: " + put);
        log.debug("contentType: " + contentType);

        FileOutputStream fos = new FileOutputStream(put);
        byte[] buf = new byte[16384];
        int num = inputStream.read(buf);
        while (num > 0) {
            fos.write(buf, 0, num);
            num = inputStream.read(buf);
        }
        fos.flush();
        fos.close();

        scheduleDeletion(put);

        URL retURL = new URL(baseResultsURL + "/" + filename);
        Content ret = new Content();
        ret.name = UWSInlineContentHandler.CONTENT_PARAM_REPLACE;
        ret.value = new UWSInlineContentHandler.ParameterReplacement("param:" + name, retURL.toExternalForm());
        return ret;
    }

    private static String getRandomString() {
        return new RandomStringGenerator(16).getID();
    }

    private void scheduleDeletion(final File file) {
        getScheduler().schedule(new Runnable() {
            @Override public void run() {
                try {
                    if (!file.exists()) {
                        log.info(String.format("%s not found for deletion", file.getAbsolutePath()));
                    } else {
                        boolean deleted = file.delete();
                        log.debug(String.format("deleted %s: %s", file.getAbsolutePath(), deleted));
                    }
                } catch (SecurityException e) {
                    log.error(String.format("Unable to delete %s because %s", file.getAbsolutePath(),
                                            e.getMessage()));
                }
            }
        }, QueryJobManager.MAX_DESTRUCTION, TimeUnit.MILLISECONDS);
    }

    // lazy init of the scheduler.
    private static ScheduledExecutorService getScheduler() {
        if (SCHEDULER == null) {
            synchronized (ScheduledExecutorService.class) {
                if (SCHEDULER == null) {
                    SCHEDULER = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());
                }
            }
        }
        return SCHEDULER;
    }

    // Returns daemon threads for jvm termination.
    static class DaemonThreadFactory implements ThreadFactory {

        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            return thread;
        }
    }

}
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

package org.opencadc.fenwick;

import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.ExpectationFailedException;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.RemoteServiceException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.AccessControlException;
import java.security.PrivilegedAction;
import java.util.Date;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.HarvestStateDAO;
import org.opencadc.tap.RowMapException;

/**
 * Internal base class with common retry and summary logging support.
 * 
 * @author pdowler
 */
abstract class AbstractSync implements Runnable {
    private static final Logger log = Logger.getLogger(AbstractSync.class);

    public static final String CERTIFICATE_FILE_LOCATION = System.getProperty("user.home") + "/.ssl/cadcproxy.pem";
    
    protected static final long LOOKBACK_TIME_MS = 60 * 1000L;
    
    protected final URI resourceID;
    protected final int querySleepInterval;
    protected final int maxRetryInterval;
    protected final ArtifactDAO artifactDAO;
    protected final HarvestStateDAO harvestStateDAO;
    
    private final int summaryInterval = 5 * 60; // 5 min
    private long lastSummaryTime = 0L;
    private long numEvents = 0L;
    private long numEventsTotal = 0L;
    
    private Exception fail;
    
    protected AbstractSync(ArtifactDAO artifactDAO, URI resourceID, int querySleepInterval, int maxRetryInterval) {
        this.artifactDAO = artifactDAO;
        this.resourceID = resourceID;
        this.querySleepInterval = querySleepInterval;
        this.maxRetryInterval = maxRetryInterval;
        this.harvestStateDAO = new HarvestStateDAO(artifactDAO);
    }
    
    // for StorageSiteSync only
    protected AbstractSync(URI resourceID, int querySleepInterval, int maxRetryInterval) {
        this.artifactDAO = null;
        this.resourceID = resourceID;
        this.querySleepInterval = querySleepInterval;
        this.maxRetryInterval = maxRetryInterval;
        this.harvestStateDAO = null;
    }
    
    // unit test ctor
    protected AbstractSync(boolean unitTestOnly) {
        this.resourceID = null;
        this.querySleepInterval = 10;
        this.maxRetryInterval = 10;
        this.artifactDAO = null;
        this.harvestStateDAO = null;
    }
    
    public Exception getFail() {
        return fail;
    }
    
    public void run() {
        boolean retry;
        int retryCount = 1;
        int sleepSeconds = querySleepInterval;

        while (true) {
            try {
                final Subject subject = SSLUtil.createSubject(new File(CERTIFICATE_FILE_LOCATION));
                int retries = retryCount;
                int timeout = sleepSeconds;

                retry = Subject.doAs(subject, (PrivilegedAction<Boolean>) () -> {
                    boolean isRetry = true;
                    try {
                        doit();
                        isRetry = false;
                        // catch exceptions resulting in a retry
                    } catch (RowMapException | ResourceNotFoundException | PreconditionFailedException 
                            | ExpectationFailedException | RemoteServiceException 
                            | TransientException | IOException | NotAuthenticatedException
                            | AccessControlException | IllegalArgumentException
                            | IndexOutOfBoundsException ex) {
                        logRetry(retries, timeout, ex.getMessage());
                    } catch (InterruptedException ex) {
                        // Thread interrupted, fail.
                        throw new RuntimeException(ex.getMessage(), ex);
                    }
                    return isRetry;
                });
            } catch (RuntimeException ex) {
                // also IllegalStateException
                logExit(ex.getMessage());
                this.fail = ex;
                return;
            }

            // TODO: dynamic depending on how rapidly the remote content is changing
            // ... this value and the reprocess-last-N-seconds should be related
            if (retry && numEvents == 0) {
                // action failed before processing any events: delay
                retryCount++;
                sleepSeconds *= 2;
                if (sleepSeconds > this.maxRetryInterval) {
                    sleepSeconds = this.maxRetryInterval;
                }
            } else {
                // successful run or failure after processing some events, reset retry values
                retryCount = 1;
                sleepSeconds = querySleepInterval;
            }

            try {
                
                log.info(this.getClass().getSimpleName() + ".sleep duration=" + sleepSeconds);
                Thread.sleep(sleepSeconds * 1000L);
            } catch (InterruptedException ex) {
                logExit(ex.getMessage());
                this.fail = ex;
                return;
            }
        }
    }
    
    /**
     * Perform a single query and sync of entities.
     * 
     * @throws ResourceNotFoundException if failing to lookup target luskan service
     * @throws IOException               failure to read or write resources
     * @throws IllegalStateException     fatal inconsistent scenario encountered
     * @throws TransientException        temporary failure of a remote call
     * @throws InterruptedException      thread interrupted
     */
    abstract void doit() throws ResourceNotFoundException, IOException, 
            IllegalStateException, TransientException, InterruptedException;
    
    // incremental mode: look back in time a little because head of sequence is not stable
    protected final Date getQueryLowerBound(Date lookBack, Date lastModified) {
        if (lookBack == null) {
            // feature not enabled
            return lastModified;
        }
        if (lastModified == null) {
            // first harvest
            return null;
        }
        if (lookBack.before(lastModified)) {
            return lookBack;
        }
        return lastModified;
        
    }
    
    protected final void logSummary(Class c) {
        logSummary(c, false);
    }
    
    protected final void logSummary(Class c, boolean doFinal) {
        if (!doFinal) {
            numEvents++;
            numEventsTotal++;
        }
        if (lastSummaryTime == 0L) {
            // first event in query result
            lastSummaryTime = System.currentTimeMillis();
            return;
        }
        if (numEvents > 0L) {
            long t2 = System.currentTimeMillis();
            long dt = t2 - lastSummaryTime;
            if (dt >= (summaryInterval * 1000L) || doFinal) {
                double minutes = ((double) dt) / (60L * 1000L);
                long epm = Math.round(numEvents / minutes); 
                String msg = "%s.summary%s numTotal=%d num=%d events-per-minute=%d eob=%b";
                log.info(String.format(msg, this.getClass().getSimpleName(), c.getSimpleName(), 
                        numEventsTotal, numEvents, epm, doFinal));
                this.lastSummaryTime = t2;
                this.numEvents = 0L;
            }
        }
        if (doFinal) {
            this.lastSummaryTime = 0L;
            this.numEvents = 0L;
            this.numEventsTotal = 0L;
        }
    }
    
    protected final void logRetry(int retries, int timeout, String message) {
        log.error(String.format("retry[%s] timeout %ss - reason: %s", retries, timeout, message));
    }

    protected final void logExit(String message) {
        log.error(String.format("Exiting, reason - %s", message));
    }
}

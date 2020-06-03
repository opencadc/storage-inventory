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

package org.opencadc.critwall;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import org.apache.log4j.Logger;

/**
 * Implementation of a thread pool executor.
 * Thread pool is fixed size. At startup, threads are set to consume the contents of the BlockingQueue.
 * Threads in the pool are blocked when trying to pull tasks from an empty queue. (see API for BlockingQueue.take().)
 */
public class ThreadPool {
    private static Logger log = Logger.getLogger(ThreadPool.class);

    private final String poolBasename = ThreadPool.class.getName();
    private final BlockingQueue<Runnable> taskQueue;
    private final int nthreads;
    private final ArrayList threads;


    public ThreadPool(BlockingQueue<Runnable> blockingQueue, int nthreads) {

        if (nthreads <= 0) {
            String errMsg = "nthreads should > 1 (" + nthreads + ")";
            log.error(errMsg);
            throw new InvalidParameterException(errMsg);
        }

        this.taskQueue = blockingQueue;
        this.nthreads = nthreads;
        this.threads = new ArrayList(nthreads);

        log.info(poolBasename + " - starting up");
        log.debug("initial thread count: " + threads.size() + " requested size: " + nthreads);

        synchronized (threads) {
            while (threads.size() < nthreads) {
                int threadNum = threads.size() + 1;
                log.debug("adding worker thread " + threadNum);
                WorkerThread t = new WorkerThread(threadNum);
                t.setPriority(Thread.MIN_PRIORITY);
                threads.add(t);
                t.start();
            }
        }
        log.debug("after pool startup - thread count: " + threads.size() + " requested size: " + nthreads);
        log.debug(poolBasename + " - ctor done");
    }

    public void terminate() {
        log.debug(poolBasename + ".terminate() starting");

        // terminate thread pool members
        synchronized (threads) {
            Iterator<WorkerThread> threadIter = threads.iterator();
            while (threadIter.hasNext()) {
                WorkerThread t = threadIter.next();
                log.debug(poolBasename + ".terminate() interrupting WorkerThread " + t.getName());
                synchronized (t) {
                    log.debug(poolBasename + ".terminate(): interrupting " + t.getName());
                    t.interrupt();
                }
            }
        }

        log.debug(poolBasename + ".terminate() DONE");
    }

    private class WorkerThread extends Thread {
        Runnable currentTask;

        WorkerThread(int threadNum) {
            super();
            setDaemon(true);
            setName(poolBasename + "-" + threadNum);
        }

        // threads keep running as long as they are in the threads list
        public void run() {
            log.debug(poolBasename + " - START");
            boolean cont = true;
            while (cont) {
                try {
                    log.debug("taking from taskQueue");
                    Runnable tmp = taskQueue.take(); // should block on take from queue if queue empty
                    synchronized (this) {
                        currentTask = tmp;
                    }
                    synchronized (threads) {
                        cont = threads.contains(this);
                    }
                    if (cont) {
                        log.debug("running current task");
                        currentTask.run();
                        log.debug("finished running task");
                    }
                } catch (InterruptedException ignore) {
                    // take() was interrupted, let finally and while condition decide if we
                    // should loop or return
                    log.error("thread interrupted: " + ignore);
                } finally {
                    synchronized (this) {
                        currentTask = null;
                    }
                }
            }
            log.debug(poolBasename + " - END");
        }
    }
}

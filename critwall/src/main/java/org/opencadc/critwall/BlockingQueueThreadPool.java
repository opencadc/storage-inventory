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

import java.util.ArrayList;

import java.util.concurrent.BlockingQueue;
import org.apache.log4j.Logger;

/**
 * Implementation of a thread pool executor. Provides both producer and consumer task blocking
 * if queue is full.
 */
public class BlockingQueueThreadPool {
    private static Logger log = Logger.getLogger(BlockingQueueThreadPool.class);
    private String workerBasename = "BlockingQueueThreadPool.WorkerThread: ";

    private BlockingQueue<Runnable> taskQueue;

    private ArrayList threads;
    // Requested number of threads
    private int nthreads;
    // Current number of workers
    private int numWorkers;


    // TODO: currently queue size is outside this ctor, possibly
    // should be inside so relative size of queue and thread pool can be managed in one place?
    public BlockingQueueThreadPool(BlockingQueue<Runnable> blockingQueue, int nthreads) {
        this.taskQueue = blockingQueue;
        this.threads = new ArrayList(nthreads);
        this.nthreads = nthreads;
        start();
    }

    public void addTask(Runnable task) {
        try {
            log.debug("queueing: " + task);
            // put)() should block until there is room on the queue
            taskQueue.put(task);
        } catch (InterruptedException ie) {
            // this is required because LinkedBlockingQueue allows
            // for tasks (threads) waiting to be executed to be interrupted.
            log.info("task interrupted");
        }
    }

    public void terminate() {
        log.debug("ThreadPool.terminate()");

        // terminate thread pool members
        numWorkers = 0;
        synchronized (threads) { // vs sync block in WorkerThread.run() that calls threads.remove(this) after an interrupt
            for (int i = 0; i < threads.size(); i++) {
                log.debug("ThreadPool.terminate() interrupting WorkerThread " + i);
                WorkerThread wt = (WorkerThread) threads.get(i);
                synchronized (wt) {
                    log.debug("ThreadPool.terminate(): interrupting " + wt.getName());
                    wt.interrupt();
                }
            }
        }

        // pop remaining tasks from queue and cancel them
        log.debug("ThreadPool.terminate() flushing queue");
        // Q: how to flush BlockingQueue?
        //            tasks.update(new QueueFlush());
        log.debug("ThreadPool.terminate() DONE");
    }

    public void start() {
        log.debug("BlockingQueueThreadPool: starting up");

        log.debug("initial thread count: " + threads.size() + " requested size: " + nthreads + " num workers: " + numWorkers);

        synchronized (threads) {
            while (threads.size() < nthreads) {
                log.debug("adding worker thread");
                WorkerThread t = new WorkerThread();
                t.setPriority(Thread.MIN_PRIORITY); // TODO: what priority? (this is from DM:) mainly IO blocked anyway, so keep the UI thread happy
                threads.add(t);
                t.start();
            }
        }

        numWorkers = threads.size();
        log.debug("after pool startup - thread count: " + threads.size() + " requested size: " + nthreads + " num workers: " + numWorkers);

    }

    private class WorkerThread extends Thread {
        Runnable currentTask;

        WorkerThread() {
            super();
            setDaemon(true);
            setName(workerBasename);
        }

        // threads keep running as long as they are in the threads list
        public void run() {
            log.debug(workerBasename + "START");
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
                        log.debug(workerBasename + "still part of pool");
                        // set thread name so thread dumps are intelligible
                        setName(workerBasename + currentTask);
                        log.debug("running current task");
                        currentTask.run();
                        log.debug("finished running task");
                    } else {
                        log.debug(workerBasename + "no longer part of pool");
                        synchronized (this) { // vs sync block in terminate()
                            // make sure to clear interrupt flag from an interrupt() in stateChanged()
                            // in case it comes after pop() and before threads.contains()
                            interrupted();
                            // we should quit, so put task back
                            log.debug(workerBasename + "OOPS (put it back): " + tmp);
                            taskQueue.put(tmp);  // will block if queue is full
                            currentTask = null;
                        }
                    }
                } catch (InterruptedException ignore) {
                    // put() was interrupted, let finally and while condition decide if we
                    // should loop or return
                    log.error("thread interrupted: " + ignore);
                } finally {
                    setName(workerBasename);
                    synchronized (this) {
                        currentTask = null;
                    }
                    synchronized (threads) {
                        if (threads.size() > numWorkers) {
                            log.debug(workerBasename + "numWorkers=" + numWorkers + " threads.size() = " + threads.size());
                            threads.remove(this);
                        }
                        cont = threads.contains(this);
                    }
                }
            }
            log.debug(workerBasename + "DONE");
        }
    }

}

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

package org.opencadc.ringhold;

import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.BucketSelector;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedStorageLocationEventDAO;

/**
 * Remove artifacts from local inventory and generate DeletedStorageLocationEvent(s). 
 * This currently supports cleanup after a change in the fenwick filter policy.
 * 
 * @author pdowler
 */
public class InventoryValidator implements Runnable {
    private static final Logger log = Logger.getLogger(InventoryValidator.class);

    private final ArtifactDAO artifactIteratorDAO;
    private final ArtifactDAO artifactDAO;
    private final List<Namespace> namespaces;
    private final BucketSelector bucketSelector;
    
    public InventoryValidator(Map<String, Object> txnConfig, Map<String, Object> iterConfig,
                              List<Namespace> namespaces, BucketSelector bucketSelector) {
        InventoryUtil.assertNotNull(InventoryValidator.class, "txnConfig", txnConfig);
        InventoryUtil.assertNotNull(InventoryValidator.class, "iterConfig", iterConfig);
        InventoryUtil.assertNotNull(InventoryValidator.class, "namespaces", namespaces);

        this.artifactDAO = new ArtifactDAO();
        artifactDAO.setConfig(txnConfig);
        
        this.artifactIteratorDAO = new ArtifactDAO();
        artifactIteratorDAO.setConfig(iterConfig);

        this.namespaces = namespaces;
        this.bucketSelector = bucketSelector;
    }

    /**
     * Find an artifact for the given namespace(s) and optional bucketUri,
     * delete the artifact and generate a deleted storage location event.
     */
    @Override
    public void run() {
        final TransactionManager transactionManager = this.artifactDAO.getTransactionManager();
        final DeletedStorageLocationEventDAO deletedStorageLocationEventDAO =
            new DeletedStorageLocationEventDAO(this.artifactDAO);

        for (Namespace namespace : namespaces) {
            if (bucketSelector == null) {
                iterateBucket(transactionManager, deletedStorageLocationEventDAO, namespace,null);
            } else {
                Iterator<String> bucketIter = bucketSelector.getBucketIterator();
                while (bucketIter.hasNext()) {
                    String bucket = bucketIter.next();
                    log.info(InventoryValidator.class.getSimpleName() + ".START bucket=" + bucket);
                    int retries = 0;
                    boolean done = false;
                    while (!done && retries < 3) {
                        try {
                            iterateBucket(transactionManager, deletedStorageLocationEventDAO, namespace, bucket);
                            log.info(InventoryValidator.class.getSimpleName() + ".END bucket=" + bucket);
                            done = true;
                        } catch (TransientException ex) {
                            log.error(InventoryValidator.class.getSimpleName() + ".FAIL bucket=" + bucket, ex);
                            retries++;
                        } catch (IllegalArgumentException ex) {
                            log.error(InventoryValidator.class.getSimpleName() + ".FAIL bucket=" + bucket, ex);
                            throw ex;
                        } catch (RuntimeException ex) {
                            // TODO: probably not a great idea to retry on these...
                            log.error(InventoryValidator.class.getSimpleName() + ".FAIL bucket=" + bucket, ex);
                            retries++;
                        } catch (Exception ex) {
                            log.error(InventoryValidator.class.getSimpleName() + ".FAIL bucket=" + bucket, ex);
                            throw ex;
                        }
                    }
                }
            }
        }
    }

    private void iterateBucket(TransactionManager transactionManager,
                               DeletedStorageLocationEventDAO deletedStorageLocationEventDAO,
                               Namespace namespace, String bucket) {
        try (final ResourceIterator<Artifact> artifactIterator = this.artifactIteratorDAO.iterator(namespace, bucket, false)) {
            while (artifactIterator.hasNext()) {
                Artifact artifact = artifactIterator.next();
                log.debug("START: Process Artifact " + artifact.getID() + " " + artifact.getURI());

                try {
                    transactionManager.startTransaction();

                    Artifact cur = this.artifactDAO.lock(artifact);
                    if (cur != null) {
                        DeletedStorageLocationEvent deletedStorageLocationEvent = new DeletedStorageLocationEvent(cur.getID());
                        deletedStorageLocationEventDAO.put(deletedStorageLocationEvent);

                        this.artifactDAO.delete(cur.getID());

                        transactionManager.commitTransaction();
                        log.info("DELETE: Artifact " + cur.getID() + " " + cur.getURI());
                    } else {
                        transactionManager.rollbackTransaction();
                        log.debug("Artifact not found");
                    }

                    log.debug("END: Process Artifact " + artifact.getID() + " "
                            + artifact.getURI());
                } catch (Exception exception) {
                    if (transactionManager.isOpen()) {
                        log.error("Exception in transaction.  Rolling back...");
                        transactionManager.rollbackTransaction();
                        log.error("Rollback: OK");
                    }
                    throw exception;
                } finally {
                    if (transactionManager.isOpen()) {
                        log.error("BUG: transaction open in finally. Rolling back...");
                        transactionManager.rollbackTransaction();
                        log.error("Rollback: OK");
                        throw new RuntimeException("BUG: transaction open in finally");
                    }
                }
            }
        } catch (IOException e) {
            log.error("Error closing iterator: " + e.getMessage());
        }
    }

}

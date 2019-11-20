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
 *  $Revision: 4 $
 *
 ************************************************************************
 */

package org.opencadc.inventory.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.StorageLocation;

import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;

/**
 * Provides access to storage.  
 *
 * @author majorb
 *
 */
public class StorageClient {
    
    private static Logger log = Logger.getLogger(StorageClient.class);
    private static String STORAGE_ADPATER_CLASS_PROPERTY = StorageAdapter.class.getName();
    
    private StorageAdapter adapter;

    public StorageClient() {
        adapter = getStorageAdapter();
    }
    
    public void get(URI storageID, OutputStream out) throws ResourceNotFoundException, TransientException {
        InputStreamWrapper handler = new InputStreamWrapper() {
            public void read(InputStream in) throws IOException {
                ioLoop(out, in);
            }
        };
        adapter.get(storageID, handler);
    }

    public StorageLocation put(Artifact artifact, InputStream in, String bucket) throws StreamCorruptedException, TransientException {
        OutputStreamWrapper wrapper = new OutputStreamWrapper() {
            public void write(OutputStream out) throws IOException {
                ioLoop(out, in);
            }
        };
        return adapter.put(artifact, wrapper, bucket);
    }

    public void delete(URI storageID) throws ResourceNotFoundException, TransientException {
        adapter.delete(storageID);
    }

    public Iterator<StorageMetadata> iterator() throws TransientException {
        return adapter.iterator();
    }
    
    public Iterator<StorageMetadata> iterator(String bucket) throws TransientException {
        return adapter.iterator(bucket);
    }
    
    private void ioLoop(OutputStream out, InputStream in) {
        // TODO: Write 2 threaded io loop--one thread reading
        // and one thread writing.
    }
    
    /**
     * Load the configured storage adapter or the default one if none
     * are found.
     * 
     * @return The storage adapter to be used by this client.
     */
    private StorageAdapter getStorageAdapter() {
        // Load the adapter based on a classname in a system property
        String cname = System.getProperty(STORAGE_ADPATER_CLASS_PROPERTY);
        if (cname == null) {
            throw new IllegalStateException(
                "No storage adapter defined by system property: " + STORAGE_ADPATER_CLASS_PROPERTY);
        }
      
        try {
            Class c = Class.forName(cname);
            Constructor con = c.getConstructor();
            Object o = con.newInstance();
            StorageAdapter ret = (StorageAdapter) o;
            log.debug("Loaded Storage adapter: " + cname);
            return ret;
        } catch (Throwable t) {
            throw new IllegalStateException("Failed to load storage adapter " + cname, t);
        }
        
    }

}

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
 *
 ************************************************************************
 */

package org.opencadc.inventory.storage.ad;

import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.StorageMetadata;

public class AdStorageIteratorTest {

    private static final Logger log = Logger.getLogger(AdStorageIteratorTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage", Level.DEBUG);
    }

    @Test
    public void testGetIterator() throws Exception {
        ArrayList<StorageMetadata> duplicates = new ArrayList<StorageMetadata>();

        for (int i = 0; i < 7; i++) {
            duplicates.add(getStorageMetadata(i));
        }

        duplicates.add(0, duplicates.get(0));
        duplicates.add(5, duplicates.get(5));
        duplicates.add(8, duplicates.get(8));

        int count = 0;
        Iterator<StorageMetadata> dupIter = duplicates.iterator();
        while (dupIter.hasNext()) {
            StorageMetadata curMeta = dupIter.next();
            count++;
            log.debug("position: " + count + ": " + curMeta);
        }

        Assert.assertEquals("expected array count is 10 but got " + count, 10, count);

        log.debug("array with duplicates size:  " + count + "\n");

        AdStorageIterator asi = new AdStorageIterator(duplicates.iterator());

        count = 0;
        while (asi.hasNext()) {
            StorageMetadata curMeta = asi.next();
            count++;
            log.debug("position: " + count + ": " + curMeta);
        }

        Assert.assertEquals("expected filtered array count is 7 but got " + count, 7, count);
        log.debug("total items from AdStorageIterator: " + count);
    }
    
    @Test
    public void testGetIteratorFirstRowNull() throws Exception {
        ArrayList<StorageMetadata> rows = new ArrayList<StorageMetadata>();

        rows.add(null);
        int numRows = 3;
        for (int i = 0; i < numRows; i++) {
            rows.add(getStorageMetadata(i));
        }

        int count = 0;
        AdStorageIterator iterator = new AdStorageIterator(rows.iterator());
        while (iterator.hasNext()) {
            StorageMetadata storageMetadata = iterator.next();
            count++;
            log.debug("position: " + count + ": " + storageMetadata);
        }
        Assert.assertEquals("expected rows: " + numRows + ", actual rows: " + count, numRows, count);
    }

    @Test
    public void testGetIteratorWithNull() throws Exception {
        ArrayList<StorageMetadata> rows = new ArrayList<StorageMetadata>();

        int numRows = 6;
        for (int i = 0; i < numRows; i++) {
            if (i == 2) {
                rows.add(null);
            }
            if (i == 4) {
                rows.add(null);
                rows.add(null);
            }
            rows.add(getStorageMetadata(i));
        }

        int count = 0;
        AdStorageIterator iterator = new AdStorageIterator(rows.iterator());
        while (iterator.hasNext()) {
            StorageMetadata storageMetadata = iterator.next();
            count++;
            log.debug("position: " + count + ": " + storageMetadata);
        }
        Assert.assertEquals("expected rows: " + numRows + ", actual rows: " + count, numRows, count);
    }

    @Test
    public void testGetIteratorLastRowNull() throws Exception {
        ArrayList<StorageMetadata> rows = new ArrayList<StorageMetadata>();

        int numRows = 3;
        for (int i = 0; i < numRows; i++) {
            rows.add(getStorageMetadata(i));
        }
        rows.add(null);

        int count = 0;
        AdStorageIterator iterator = new AdStorageIterator(rows.iterator());
        while (iterator.hasNext()) {
            StorageMetadata storageMetadata = iterator.next();
            count++;
            log.debug("position: " + count + ": " + storageMetadata);
        }
        Assert.assertEquals("expected rows: " + numRows + ", actual rows: " + count, numRows, count);
    }

    private StorageMetadata getStorageMetadata(int i) throws Exception {
        String uriStr = "ad:TEST/fileuri_" + i + ".txt";
        URI uri = new URI(uriStr);
        StorageLocation storageLocation = new StorageLocation(uri);
        storageLocation.storageBucket = "testBucket";
        StorageMetadata storageMetadata = new StorageMetadata(storageLocation, new URI("md5:12345"), 12345L);
        storageMetadata.artifactURI = uri;
        return storageMetadata;
    }

}

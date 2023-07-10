/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2023.                            (c) 2023.
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


package org.opencadc.raven;

import ca.nrc.cadc.util.Log4jInit;

import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.inventory.Namespace;
import org.opencadc.inventory.StorageSite;

public class ProtocolsGeneratorTest {
    private static final Logger log = Logger.getLogger(ProtocolsGeneratorTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.raven", Level.INFO);
    }

    @Test
    public void testPrioritizePullFromSites() throws Exception {
        List<StorageSite> sites = new ArrayList<>();
        Random rd = new Random();
        for (int i = 0; i < 10; i++) {
            sites.add(new StorageSite(URI.create("ivo://site" + i), "site1" + i, true, rd.nextBoolean()));
        }
        ProtocolsGenerator.prioritizePullFromSites(sites);
        for (StorageSite s : sites) {
            log.info("found: " + s.getID() + " aka " +  s.getResourceID());
        }
    }

    @Test
    public void testPrioritizingStorageSiteComparator() throws Exception {

        Map<URI, StorageSiteRule> siteRules = new HashMap<>();
        InetAddress clientIP = null;

        ProtocolsGenerator.PrioritizingStorageSiteComparator comparator =
            new ProtocolsGenerator.PrioritizingStorageSiteComparator(siteRules, URI.create("ivo:aaa/123"), clientIP);

        // both StorageSite's null
        int actual = comparator.compare(null, null);
        Assert.assertEquals(0, actual);

        //2nd StorageSite null
        actual = comparator.compare(new StorageSite(URI.create("ivo:site1"), "site1", true, true), null);
        Assert.assertTrue(actual != 0);

        // 1st StorageSite null
        actual = comparator.compare(null, new StorageSite(URI.create("ivo:site1"), "site1", true, true));
        Assert.assertTrue(actual != 0);


        // site2 orders less than site5 using StorageSite default ordering
        actual = comparator.compare(new StorageSite(URI.create("ivo:site2"), "site2", true, true),
                                    new StorageSite(URI.create("ivo:site5"), "site5", true, true));
        Assert.assertTrue(actual < 0);


        // site2 orders greater than site5 using the site rules
        List<Namespace> namespaces = new ArrayList<>();
        namespaces.add(new Namespace("ivo:aaa/"));
        siteRules.put(URI.create("ivo:site5"), new StorageSiteRule(namespaces));

        actual = comparator.compare(new StorageSite(URI.create("ivo:site2"), "site2", true, true),
                                    new StorageSite(URI.create("ivo:site5"), "site5", true, true));
        Assert.assertTrue(actual > 0);


        // 1st StorageSite has a rule with a namespace matching the ArtifactURI
        comparator = new ProtocolsGenerator.PrioritizingStorageSiteComparator(siteRules, URI.create("ivo:aaa/123"), clientIP);

        actual = comparator.compare(new StorageSite(URI.create("ivo:site5"), "site5", true, true),
                                    new StorageSite(URI.create("ivo:site2"), "site2", true, true));
        Assert.assertTrue(actual != 0);

        // 2nd StorageSite has a rule with a namespace matching the ArtifactURI
        actual = comparator.compare(new StorageSite(URI.create("ivo:site2"), "site2", true, true),
                                    new StorageSite(URI.create("ivo:site5"), "site5", true, true));
        Assert.assertTrue(actual != 0);

        comparator = new ProtocolsGenerator.PrioritizingStorageSiteComparator(siteRules, URI.create("ivo:bbb/123"), clientIP);

        // no StorageSite's with a rule with a namespace that matches ArtifactURI, StorageSite ordering used (resourceID)
        actual = comparator.compare(new StorageSite(URI.create("ivo:site2"), "site2", true, true),
                                    new StorageSite(URI.create("ivo:site3"), "site3", true, true));
        Assert.assertTrue(actual != 0);

        actual = comparator.compare(new StorageSite(URI.create("ivo:site3"), "site3", true, true),
                                    new StorageSite(URI.create("ivo:site2"), "site2", true, true));
        Assert.assertTrue(actual != 0);
    }

    @Test
    public void testPrioritizePushToSites() throws Exception {
        List<Namespace> readWriteNamespaces = new ArrayList<>();
        readWriteNamespaces.add(new Namespace("readwrite:RW1/"));
        readWriteNamespaces.add(new Namespace("readwrite:RW2/"));
        readWriteNamespaces.add(new Namespace("readwrite:RW3/"));

        List<Namespace> readOnlyNamespaces = new ArrayList<>();
        readOnlyNamespaces.add(new Namespace("readonly:RO1/"));
        readOnlyNamespaces.add(new Namespace("readonly:RO2/"));

        List<Namespace> writeOnlyNamespaces = new ArrayList<>();
        writeOnlyNamespaces.add(new Namespace("writeonly:WO1/"));
        writeOnlyNamespaces.add(new Namespace("writeonly:WO2/"));

        SortedSet<StorageSite> sites = new TreeSet<>();
        Map<URI, StorageSiteRule> siteRules = new HashMap<>();

        // empty set of StorageSite's
        SortedSet<StorageSite> actual = ProtocolsGenerator.prioritizePushToSites(sites, URI.create("other:SITE/file.ext"), siteRules);
        Assert.assertTrue(actual.isEmpty());
        URI readWriteResourceID = URI.create("ivo://read-write-site");
        StorageSite readWriteSite = new StorageSite(readWriteResourceID, "read-write-site", true, true);
        // single StorageSite
        sites.add(readWriteSite);
        actual = ProtocolsGenerator.prioritizePushToSites(sites, URI.create("other:SITE/file.ext"), siteRules);
        Assert.assertEquals(1, actual.size());
        actual.clear();

        URI readOnlyResourceID = URI.create("ivo://read-only-site");
        URI writeOnlyResourceID = URI.create("ivo://write-only-site");
        StorageSite readOnlySite = new StorageSite(readOnlyResourceID, "read-only-site", true, false);
        StorageSite writeOnlySite = new StorageSite(writeOnlyResourceID, "write-only-site", false, true);
        // artifact with no preferences in config, returns two read-write sites
        sites.add(readOnlySite);
        sites.add(writeOnlySite);

        siteRules.put(readWriteResourceID, new StorageSiteRule(readWriteNamespaces));
        siteRules.put(readOnlyResourceID, new StorageSiteRule(readOnlyNamespaces));
        siteRules.put(writeOnlyResourceID, new StorageSiteRule(writeOnlyNamespaces));

        actual = ProtocolsGenerator.prioritizePushToSites(sites, URI.create("other:SITE/file.ext"), siteRules);
        Assert.assertEquals(2, actual.size());
        Assert.assertFalse(actual.contains(readOnlySite));
        actual.clear();

        // artifact with namespace in read-only site, returns two read-write sites
        actual = ProtocolsGenerator.prioritizePushToSites(sites, URI.create("readonly:RO2/file.ext"), siteRules);
        Assert.assertEquals(2, actual.size());
        Assert.assertFalse(actual.contains(readOnlySite));
        actual.clear();

        // artifact with read-write namespace, returns two read-write sites, cadc-site first
        actual = ProtocolsGenerator.prioritizePushToSites(sites, URI.create("readwrite:RW3/file.ext"), siteRules);
        Assert.assertEquals(2, actual.size());
        Assert.assertEquals(readWriteSite, actual.first());
        Assert.assertEquals(writeOnlySite, actual.last());
        actual.clear();

        // artifact with write-only namespace, return two read-write sites, write-only site first
        actual = ProtocolsGenerator.prioritizePushToSites(sites, URI.create("writeonly:WO1/file.ext"), siteRules);
        Assert.assertEquals(2, actual.size());
        Assert.assertEquals(writeOnlySite, actual.first());
        Assert.assertEquals(readWriteSite, actual.last());

        // two sites that both have a namespace matching the artifact URI
        //sites.clear();
        siteRules.clear();
        siteRules.put(readWriteResourceID, new StorageSiteRule(readWriteNamespaces));
        siteRules.put(writeOnlyResourceID, new StorageSiteRule(readWriteNamespaces));
        siteRules.put(readOnlyResourceID, new StorageSiteRule(readOnlyNamespaces));

        actual = ProtocolsGenerator.prioritizePushToSites(sites, URI.create("readwrite:RW2/file.ext"), siteRules);
        Assert.assertEquals(2, actual.size());
        // ivo://read-write-site orders higher than ivo://write-only-site
        Assert.assertEquals(readWriteSite, actual.first());
        Assert.assertEquals(writeOnlySite, actual.last());
    }

}


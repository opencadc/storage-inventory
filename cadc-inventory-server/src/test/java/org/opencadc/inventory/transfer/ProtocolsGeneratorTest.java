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


package org.opencadc.inventory.transfer;

import ca.nrc.cadc.util.Log4jInit;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
    }

    @Test
    public void testPrioritizePullFromSites() throws Exception {
        ProtocolsGenerator gen = new ProtocolsGenerator(null);
        Set<StorageSite> sites = new TreeSet<>();
        for (int i = 0; i < 3; i++) {
            sites.add(new StorageSite(URI.create("ivo://site" + i), "site" + i, true, true));
        }
        List<StorageSite> result1 = gen.prioritizePullFromSites(sites, URI.create("foo:bar1/file.txt"));
        Assert.assertEquals(sites.size(), result1.size());
        Assert.assertTrue(result1.containsAll(sites));
        
        List<StorageSite> result2 = gen.prioritizePullFromSites(sites, URI.create("foo:bar1/file.txt"));
        Assert.assertEquals(sites.size(), result2.size());
        Assert.assertTrue(result2.containsAll(sites));
        
        int num = 1;
        while (result1.get(0).equals(result2.get(0)) && num < 10) {
            num++;
            result2 = gen.prioritizePullFromSites(sites, URI.create("foo:bar1/file.txt"));
        }
        log.info("3 sites, no rules: random after " + num);
        Assert.assertTrue("looks random", num < 5);
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
        final Namespace ns1 = new Namespace("foo:bar1/");
        final Namespace ns2 = new Namespace("foo:bar2/");
        final Namespace ns3 = new Namespace("foo:bar3/");
        
        Set<StorageSite> sites = new TreeSet<>();
        Map<URI, StorageSiteRule> siteRules = new HashMap<>();
        ProtocolsGenerator gen = new ProtocolsGenerator(siteRules);
        
        // no sites, no rules
        List<StorageSite> actual = gen.prioritizePushToSites(sites, URI.create("foo:bar2/file.ext"));
        Assert.assertTrue(actual.isEmpty());
        URI rid1 = URI.create("ivo://site1");
        StorageSite site1 = new StorageSite(rid1, "site1", true, true);
        sites.add(site1);
        
        // 1 site, no rules
        gen = new ProtocolsGenerator(siteRules);
        actual = gen.prioritizePushToSites(sites, URI.create("foo:bar2/file.ext"));
        Assert.assertEquals(1, actual.size());

        URI rid2 = URI.create("ivo://site2");
        URI rid3 = URI.create("ivo://site3");
        StorageSite site2 = new StorageSite(rid2, "site2", true, true);
        StorageSite site3 = new StorageSite(rid3, "site3", true, true);
        sites.add(site2);
        sites.add(site3);
        
        // 3 sites, no rules
        actual = gen.prioritizePushToSites(sites, URI.create("foo:bar2/file.ext"));
        Assert.assertEquals(3, actual.size());
        List<StorageSite> a2 = gen.prioritizePushToSites(sites, URI.create("foo:bar2/file.ext"));
        int num = 1;
        while (actual.get(0).equals(a2.get(0)) && num < 10) {
            num++;
            a2 = gen.prioritizePushToSites(sites, URI.create("foo:bar2/file.ext"));
        }
        log.info("3 sites, no rules: random after " + num);
        Assert.assertTrue("looks random", num < 5);
        
        List<Namespace> nss;
        nss = new ArrayList<>();
        nss.add(ns1);
        siteRules.put(rid1, new StorageSiteRule(nss));
        nss = new ArrayList<>();
        nss.add(ns2);
        siteRules.put(rid2, new StorageSiteRule(nss));
        
        gen = new ProtocolsGenerator(siteRules);

        // priority == non-random
        for (int i = 0; i < 10; i++) {
            actual = gen.prioritizePushToSites(sites, URI.create("foo:bar1/file.ext"));
            Assert.assertEquals(3, actual.size());
            Assert.assertEquals(site1, actual.get(0));
            actual.clear();
        }
        
        // priority == non-random
        for (int i = 0; i < 10; i++) {
            actual = gen.prioritizePushToSites(sites, URI.create("foo:bar2/file.ext"));
            Assert.assertEquals(3, actual.size());
            Assert.assertEquals(site2, actual.get(0));
            actual.clear();
        }
        
        // no rule match == random
        actual = gen.prioritizePushToSites(sites, URI.create("foo:bar3/file.ext"));
        Assert.assertEquals(3, actual.size());
        a2 = gen.prioritizePushToSites(sites, URI.create("foo:bar3/file.ext"));
        num = 0;
        while (actual.get(0).equals(a2.get(0)) && num < 10) {
            num++;
            a2 = gen.prioritizePushToSites(sites, URI.create("foo:bar3/file.ext"));
        }
        log.info("3 sites, 2 rules, no match: random after " + num);
        Assert.assertTrue("looks random", num < 5);
    }

}


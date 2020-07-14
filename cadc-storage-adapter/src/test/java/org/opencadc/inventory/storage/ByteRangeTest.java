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

package org.opencadc.inventory.storage;

import ca.nrc.cadc.util.Log4jInit;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class ByteRangeTest {
    private static final Logger log = Logger.getLogger(ByteRangeTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory.storage", Level.INFO);
    }
    
    public ByteRangeTest() { 
    }
    
    @Test
    public void testCtor() {
        ByteRange ok = new ByteRange(0, 1);
        log.info("created: " + ok);
        
        try {
            ByteRange fail = new ByteRange(-1, 1);
            Assert.fail("expected IllegalArgumentException, got: " + fail);
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        }
        
        try {
            ByteRange fail = new ByteRange(0, 0);
            Assert.fail("expected IllegalArgumentException, got: " + fail);
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        }
        
        try {
            ByteRange fail = new ByteRange(0, -1);
            Assert.fail("expected IllegalArgumentException, got: " + fail);
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        }
    }
    
    @Test
    public void testComparableEqualsHashcode() {
        ByteRange a = new ByteRange(0, 10);
        ByteRange b = new ByteRange(20, 10);
        ByteRange c = new ByteRange(40, 10);
        
        SortedSet<ByteRange> rs = new TreeSet<>();
        rs.add(b);
        rs.add(c);
        rs.add(a);
        Assert.assertEquals(3, rs.size());
        
        Iterator<ByteRange> iter = rs.iterator();
        ByteRange aa = iter.next();
        Assert.assertEquals(a.getOffset(), aa.getOffset());
        Assert.assertEquals(a.getLength(), aa.getLength());
        Assert.assertEquals(a, aa); 
        Assert.assertEquals(a.hashCode(), aa.hashCode());
        
        ByteRange bb = iter.next();
        Assert.assertEquals(b.getOffset(), bb.getOffset());
        Assert.assertEquals(b.getLength(), bb.getLength());
        Assert.assertEquals(b, bb); 
        Assert.assertEquals(b.hashCode(), bb.hashCode());
        
        ByteRange cc = iter.next();
        Assert.assertEquals(c.getOffset(), cc.getOffset());
        Assert.assertEquals(c.getLength(), cc.getLength());
        Assert.assertEquals(c, cc); 
        Assert.assertEquals(c.hashCode(), cc.hashCode());
        
        rs.clear();
        rs.add(a);
        try {
            rs.add(new ByteRange(5, 10)); // overlap a from above
            Assert.fail("expected IllegalArgumentException, added 5/10");
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        }
        
        rs.clear();
        rs.add(c);
        try {
            rs.add(new ByteRange(35, 10)); // overlap c from below
            Assert.fail("expected IllegalArgumentException, added 35/10");
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        }
        
        rs.clear();
        rs.add(b);
        try {
            rs.add(new ByteRange(24, 2)); // inside b
            Assert.fail("expected IllegalArgumentException, added 24/2");
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        }
        
        rs.clear();
        rs.add(b);
        try {
            rs.add(new ByteRange(10, 30)); // contains b
            Assert.fail("expected IllegalArgumentException, added 10/30");
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        }
    }
}

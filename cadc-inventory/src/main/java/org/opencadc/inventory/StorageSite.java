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
************************************************************************
*/

package org.opencadc.inventory;

import java.net.URI;
import java.util.UUID;
import org.apache.log4j.Logger;

/**
 * Entity describing a storage site. Each storage site will have one instance of this
 * and global inventory will harvest those and thus have a small set of sites it knows
 * about. While global will bootstrap using a registry (search), it will track info 
 * gathered from the sites themselves via this class.
 * 
 * @author pdowler
 */
public class StorageSite extends Entity {
    private static final Logger log = Logger.getLogger(StorageSite.class);

    private URI resourceID;
    private String name;
    
    /**
     * Create a new StorageSite.
     * 
     * @param resourceID IVOA resourceID
     * @param name display name
     */
    public StorageSite(URI resourceID, String name) {
        super();
        init(resourceID, name);
    }
    
    /**
     * Reconstruct a storage site from serialized state.
     * 
     * @param id entity ID
     * @param resourceID IVOA resourceID
     * @param name display name
     */
    public StorageSite(UUID id, URI resourceID, String name) {
        super(id);
        init(resourceID, name);
    }
    
    private void init(URI resourceID, String name) {
        InventoryUtil.assertNotNull(StorageSite.class, "resourceID", resourceID);
        InventoryUtil.assertNotNull(StorageSite.class, "name", name);
        this.resourceID = resourceID;
        this.name = name;
    }

    /**
     * Get the resourceID (service identifier) for this site.
     * @return resourceID
     */
    public URI getResourceID() {
        return resourceID;
    }

    /**
     * Get the display name of this site.
     * 
     * @return display name
     */
    public String getName() {
        return name;
    }

    /**
     * Compares Site.resourceID values.
     * 
     * @param o object to compare to
     * @return true if the object is a StorageSite with the same resourceID, otherwise false
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        StorageSite f = (StorageSite) o;
        return resourceID.equals(f.resourceID);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append("[");
        sb.append(resourceID).append(",");
        sb.append(name);
        sb.append("]");
        return sb.toString();
    }
}

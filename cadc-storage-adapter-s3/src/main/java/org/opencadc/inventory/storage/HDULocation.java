/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2011.                            (c) 2011.
 * National Research Council            Conseil national de recherches
 * Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 * All rights reserved                  Tous droits reserves
 *
 * NRC disclaims any warranties         Le CNRC denie toute garantie
 * expressed, implied, or statu-        enoncee, implicite ou legale,
 * tory, of any kind with respect       de quelque nature que se soit,
 * to the software, including           concernant le logiciel, y com-
 * without limitation any war-          pris sans restriction toute
 * ranty of merchantability or          garantie de valeur marchande
 * fitness for a particular pur-        ou de pertinence pour un usage
 * pose.  NRC shall not be liable       particulier.  Le CNRC ne
 * in any event for any damages,        pourra en aucun cas etre tenu
 * whether direct or indirect,          responsable de tout dommage,
 * special or general, consequen-       direct ou indirect, particul-
 * tial or incidental, arising          ier ou general, accessoire ou
 * from the use of the software.        fortuit, resultant de l'utili-
 *                                      sation du logiciel.
 *
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 ************************************************************************
 */

package org.opencadc.inventory.storage;

import org.apache.log4j.Logger;

import ca.nrc.cadc.util.StringUtil;


/**
 * Base class defining the Header-Data Unit location.
 */
public class HDULocation {

    // Logger
    private static final Logger LOG = Logger.getLogger(HDULocation.class);

    protected static final String FIELD_SEPARATOR = ",";

    /*
     * Extracts the Header-Data Unit location information in specifier.
     * @param specifier May contain Header-Data Unit location information
     * @return null if specified does not contain Header-Data Unit location information
     *         a sub-type of HDULocation instance.
     */
    public static HDULocation extractLocation(String specifier) {
        LOG.debug("extractLocation: " + specifier);
        HDULocation hduLocation = null;

        if (StringUtil.hasLength(specifier)) {
            String[] fields = specifier.split(FIELD_SEPARATOR);
            if (fields.length > 1 && fields.length < 4 && NamedHDULocation.isNamedHDULocation(specifier)) {
				if (fields.length == 2) {
					hduLocation = new NamedHDULocation(fields[0].trim(), fields[1].trim(), null);
				} else {
					hduLocation = new NamedHDULocation(fields[0].trim(), fields[1].trim(), fields[2].trim());
				}
            } else if (fields.length == 1) {
				if (PrimaryHDULocation.isPrimaryHDULocation(specifier)) {
					hduLocation = new PrimaryHDULocation(specifier);
				} else if (NamedHDULocation.isNamedHDULocation(specifier)) {
					hduLocation = new NamedHDULocation(fields[0].trim(), null, null);
				} else if (NumberedHDULocation.isNumberedHDULocation(specifier))
				// only one field that could represent either extNum or cutout
				// if it's an integer, it represents the extension number
				// otherwise it's a cutout
				{
					hduLocation = new NumberedHDULocation(fields[0].trim());
				} else
				// not an HDULocation and not a valid cutout specifier since a valid cutout
				// specifier contains at least two fields
				{
					throw new IllegalArgumentException("Failed to parse: " + specifier
													   +
													   " reason: neither a valid HDU location name nor a valid cutout " +
													   "specifier");
				}
            }
        }

		if (hduLocation == null) {
			LOG.debug("extractLocation: returns null");
		} else {
			LOG.debug("extractLocation: returns " + hduLocation.toString());
		}
        return hduLocation;
    }
}

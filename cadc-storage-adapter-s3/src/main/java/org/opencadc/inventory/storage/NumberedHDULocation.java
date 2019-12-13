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
 * Represents HDU locations specified by an extension number.
 *
 * @author yeunga
 */
public class NumberedHDULocation extends HDULocation {

    // Logger
    private static final Logger LOG = Logger.getLogger(NumberedHDULocation.class);

    private int extNum = 0;

    public static boolean isNumberedHDULocation(String specifier) {
        LOG.debug("isNumberedHDULocation: " + specifier);

        boolean isNumber = false;

        if (StringUtil.hasLength(specifier)) {
            String[] fields = specifier.split(HDULocation.FIELD_SEPARATOR);
            String field0 = fields[0].trim();
            if (fields.length == 1) {
                try {
                    Integer.parseInt(field0);
                    isNumber = true;
                } catch (NumberFormatException e) {
                    // not a number, do nothing
                }
            }
        }

        LOG.debug("isNumberedHDULocation: returns " + isNumber);
        return isNumber;
    }

    /**
     * Constructor with parameters.
     */
    public NumberedHDULocation(String extNum) {
        LOG.debug("NumberedHDULocation: extension number = " + extNum);

        Integer num = null;

        if (StringUtil.hasText(extNum) &&
            (extNum.startsWith("+") || (extNum.startsWith("-")))) {
            throw new NumberFormatException();
        }
        num = Integer.parseInt(extNum);


        if (num < 0) {
            throw new IllegalArgumentException("Extension number cannot be less than zero.");
        }

        this.extNum = num;
    }

    @Override
    public String toString() {
        return Integer.toString(this.extNum);
    }
}

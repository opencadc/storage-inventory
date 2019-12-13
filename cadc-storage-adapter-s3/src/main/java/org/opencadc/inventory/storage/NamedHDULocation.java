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
 * Represents HDU locations specified by an extension name.
 *
 * @author yeunga
 */
public class NamedHDULocation extends HDULocation {

    // Logger
    private static final Logger LOG = Logger.getLogger(NamedHDULocation.class);

    private static final String IMAGE_TYPE = "IMAGE";
    private static final String ASCII_TYPE = "ASCII";
    private static final String TABLE_TYPE = "TABLE";
    private static final String BINTABLE_TYPE = "BINTABLE";
    private String extName = null;
    private Integer extVer = null;
    private String type = null;

    /**
     * Determines if the specifier contains a valid HDU location name.
     * A valid HDU location name consists of three fields: [extName, extVer, extType].
     * extName is an extension name and is mandatory. In this implementation,
     * extName cannot contain either ':' or '*' character.
     * extVer is an extension version and is an integer. extVer is optional.
     * extType is an extension type and is optional. extType can be one of "i", "a", "t" or "b".
     *
     * @param specifier
     * @return true if the specifier is considered a valid name, false otherwise
     *
     * @throws IllegalArgumentException if the specifier neither be an HDU location name nor a cutout specifier.
     */
    public static boolean isNamedHDULocation(String specifier) {
        LOG.debug("isNamedHDULocation: " + specifier);

        boolean isNamed = false;

        // validate name
        if (StringUtil.hasLength(specifier)) {
            String[] fields = specifier.split(HDULocation.FIELD_SEPARATOR);
            String field0 = fields[0].trim();
            if (fields.length == 1) {
                isNamed = isExtName(field0);
            } else if (fields.length == 2) {
                if (isExtVersion(fields[1].trim())) {
                    isNamed = isExtName(field0);
                }
            } else if (fields.length == 3) {
                if (isExtType(fields[2].trim())) {
                    if (isExtVersion(fields[1].trim())) {
                        isNamed = isExtName(field0);
                    }
                }
            }
        }

        LOG.debug("isNamedHDULocation: returns " + isNamed);
        return isNamed;
    }

    /**
     * Constructor with parameters.
     */
    public NamedHDULocation(String name, String version, String type) {
        LOG.debug("NamedHDULocation: name = " + name + ", version = " + version + ", type = " + type);

        // validate name
        if (!isExtName(name)) {
            throw new IllegalArgumentException("Extension name is missing.");
        }

        // validate version
        if (StringUtil.hasLength(version)) {
            try {
                this.extVer = Integer.parseInt(version);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Not a valid extension number.", e);
            }
        } else {
            // empty version is allowed
            this.extVer = null;
        }

        // validate type
        if (StringUtil.hasLength(type)) {
            if (type.length() > 1) {
                validateLong(type);
            } else {
                // type length == 1
                validateShort(type);
            }
        } else {
            // empty string for type is allowed
            this.type = type;
        }

        this.extName = name;
        this.type = type;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.extName);
        if (StringUtil.hasLength(type)) {
            sb.append(',');
            if (this.extVer != null) {
                sb.append(this.extVer);
            }

            sb.append(',');
            sb.append(this.type);
        } else if (this.extVer != null) {
            sb.append(',');
            sb.append(this.extVer);
        }

        LOG.debug("toString: " + sb.toString());
        return sb.toString();
    }

    protected static boolean isValidName(String name) {
        // define a set of characters not allowed in an extension name
        char[] invalidChars = {':', '*'};
        boolean isValid = true;

        for (char ch : invalidChars) {
            if (name.indexOf(ch) > -1) {
                isValid = false;
                break;
            }
        }

        return isValid;
    }

    protected static boolean isExtName(String name) {
        boolean isExtName = false;

        // there is only one field in the HDU location
        if (StringUtil.hasLength(name)) {
            try {
                new NumberedHDULocation(name);
            } catch (NumberFormatException e) {
                // not an extension number, must be a named HDU location if not a PrimaryHDULocation
                isExtName = !PrimaryHDULocation.isPrimaryHDULocation(name) && isValidName(name);
            }
        }

        return isExtName;
    }

    protected static boolean isExtVersion(String version) {
        boolean isExtType = false;

        if (StringUtil.hasLength(version)) {
            try {
                Integer.parseInt(version);
                isExtType = true;
            } catch (NumberFormatException e) {
                // not a valid extension version, do nothing
            }
        } else
        // empty string is allowed
        {
            isExtType = true;
        }

        return isExtType;
    }

    protected static boolean isExtType(String type) {
        boolean isExtType = false;

        if (StringUtil.hasLength(type)) {
            if (type.length() > 1) {
                try {
                    validateLong(type);
                    isExtType = true;
                } catch (IllegalArgumentException e) {
                    // not a named HDU location, do nothing
                }
            } else {
                // type length == 1
                try {
                    validateShort(type);
                    isExtType = true;
                } catch (IllegalArgumentException e) {
                    // not a named HDU location, do nothing
                }
            }
        } else
        // empty type string is allowed
        {
            isExtType = true;
        }

        return isExtType;
    }

    protected static void validateLong(String type) {
        if (!(IMAGE_TYPE.equalsIgnoreCase(type) ||
              ASCII_TYPE.equalsIgnoreCase(type) ||
              TABLE_TYPE.equalsIgnoreCase(type) ||
              BINTABLE_TYPE.equalsIgnoreCase(type))) {
            throw new IllegalArgumentException("Extension type " + type + " is not supported.");
        }
    }

    protected static void validateShort(String type) {
        char ch = type.charAt(0);

        if (!('i' == ch || 'I' == ch ||
              'a' == ch || 'A' == ch ||
              't' == ch || 'T' == ch ||
              'b' == ch || 'B' == ch)) {
            throw new IllegalArgumentException("Extension type " + type + " is not supported.");
        }
    }
}

/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2019.                            (c) 2019.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;


/**
 * Class responsible for parsing and validating pixel cutout syntax.
 * Logic and code taken/refactored from getData pixel cutout validation.
 */
public class CutoutValidator {

    // Logger
    private static final Logger LOG = Logger.getLogger(CutoutValidator.class);

    /**
     * Given an array of pixel cutout strings, validate and return corresponding
     * PixelCutout objects.
     *
     * @param cutoutStrings The string array
     * @return The PixelCutout objects
     *
     * @throws IllegalArgumentException If the strings are not in a correct format.
     */
    public static Collection<Cutout> validateCutouts(Collection<String> cutoutStrings)
            throws IllegalArgumentException {
        if (cutoutStrings == null) {
            throw new IllegalArgumentException("Cutout validation error: missing cutout parameters.");
        }

        final Collection<Cutout> cutouts = new ArrayList<>(cutoutStrings.size());
        for (String cutoutString : cutoutStrings) {
            try {
                cutouts.add(validateCutout(cutoutString));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Cutout validation error: " + e.getMessage());
            }
        }

        return cutouts;
    }

    /**
     * Validate a single cutout string.
     *
     * @param cutoutString      The String to validate and parse.
     * @return  Cutout Cutout instance.
     *
     * @throws IllegalArgumentException     For NULL or bad input.
     */
    private static Cutout validateCutout(String cutoutString) throws IllegalArgumentException {
        if (cutoutString == null) {
            throw new IllegalArgumentException("Failed to parse null cutout string.");
        }

        cutoutString = cutoutString.trim();
        if (cutoutString.length() == 0) {
            throw new IllegalArgumentException("Failed to parse empty cutout string.");
        }
        if (cutoutString.length() < 3) // minimum size
        {
            throw new IllegalArgumentException("Failed to parse cutout string: " + cutoutString);
        }
        LOG.debug("validateCutout: " + cutoutString);

        // index access
        if ((cutoutString.charAt(0) == '[') && (cutoutString.charAt(cutoutString.length() - 1) == ']')) {
            String temp = cutoutString.substring(1, cutoutString.length() - 1);
            String[] fields = temp.split("\\]\\[");
            HDULocation hduLocation = HDULocation.extractLocation(fields[0].trim());

            if (hduLocation == null) {
                LOG.debug("validateCutout: hduLocation = null");
            } else {
                LOG.debug("validateCutout: hduLocation = " + hduLocation.toString());
            }

            if (fields.length == 1) {
                if (hduLocation == null) {
                    return extractCutout(cutoutString, null, fields[0]);
                } else {
                    return new Cutout(hduLocation, null, null);
                }
            } else if (fields.length == 2) {
                if (hduLocation == null) {
                    throw new IllegalArgumentException("Failed to parse: " + cutoutString
                                                       + " reason: no valid Header-Data Unit location.");
                } else {
                    return extractCutout(cutoutString, hduLocation, fields[1]);
                }
            } else {
                throw new IllegalArgumentException("Failed to parse: " + cutoutString
                                                   + " reason: too many [] fields");
            }
        }
        throw new IllegalArgumentException("Failed to parse: " + cutoutString);
    }

    /**
     * Create the PixelCutout objects.
     *
     * @param s
     * @param hduLocation
     * @param sub
     * @return
     */
    private static Cutout extractCutout(String s, HDULocation hduLocation, String sub)
            throws IllegalArgumentException {
        // try the full/generic IRAF/imcopy cutout spec
        Part[] parts = new Part[4];
        int i = 0;
        LOG.debug("parsing: " + sub);
        StringTokenizer st = new StringTokenizer(sub, ",");
        while (st.hasMoreTokens()) {
            String s1 = st.nextToken().trim();
            LOG.debug("\t" + s1);
            Part p = new Part();
            // parse
            StringTokenizer st2 = new StringTokenizer(s1, ":");
            while (st2.hasMoreTokens()) {
                p.a = nextTokenOrNull(st2);
                p.b = nextTokenOrNull(st2);
                p.c = nextTokenOrNull(st2);
                if (st2.hasMoreTokens()) {
                    throw new IllegalArgumentException("Too many parts in '" + s1 + "'");
                }
            }
            validate(p, s, s1);
            parts[i++] = p;
        }
        StringBuffer sb = new StringBuffer();
        int[] offsets = new int[parts.length];
        for (i = 0; i < parts.length; i++) {
            if (parts[i] != null) {
                if (sb.length() > 0) {
                    sb.append(",");
                }

                try {
                    offsets[i] = Integer.parseInt(parts[i].a);
                } catch (NumberFormatException nope) {
                    offsets[i] = 0;
                }

                sb.append(parts[i].a);
                if (parts[i].b != null) {
                    sb.append(":" + parts[i].b);
                }

                if (parts[i].c != null) {
                    sb.append(":" + parts[i].c);
                }
            }
        }
        return new Cutout(hduLocation, sb.toString(), offsets);
    }

    /**
     * Validate additional syntax.
     *
     * @param p
     * @param s
     * @param s1
     * @throws IllegalArgumentException
     */
    private static void validate(Part p, String s, String s1)
            throws IllegalArgumentException {
        if (p.a == null) {
            throw new IllegalArgumentException("Failed to parse: " + s);
        }

        if (p.a.equals("*") || p.a.equals("-*")) {
            if (p.c != null) {
                throw new IllegalArgumentException("Failed to parse: " + s
                                                   + " reason: too many parts in '" + s1 + "'");
            }
            if (p.b != null && !isInt(p.b)) {
                throw new IllegalArgumentException("Failed to parse: " + s
                                                   + " reason: resample must be an integer in '" + s1 + "'");
            }
            return;
        }

        if (!isInt(p.a) || !isInt(p.b)) {
            throw new IllegalArgumentException("Failed to parse: " + s
                                               + " reason: range must be integers or * in '" + s1 + "'");
        }

        if (p.c != null && !isInt(p.c)) {
            throw new IllegalArgumentException("Failed to parse: " + s
                                               + " reason: resample must be an integer in '" + s1 + "'");
        }
    }

    /**
     * Return true if s is an Int.
     *
     * @param s
     * @return
     */
    private static boolean isInt(String s) {
        if (s == null) {
            return false;
        }

        try {
            Integer.parseInt(s);
        } catch (NumberFormatException nex) {
            return false;
        }

        return true;
    }

    /**
     * Get the next token.  If not available, return null.
     *
     * @param st
     * @return
     */
    private static String nextTokenOrNull(StringTokenizer st) {
        try {
            String s = st.nextToken();
            s = s.trim();
            LOG.debug("\t\t" + s);
            return s;
        } catch (NoSuchElementException ignore) {
            return null;
        }
    }

    /**
     * Convenience class to represent a Part.
     */
    private static class Part {

        String a;
        String b;
        String c;
    }

}


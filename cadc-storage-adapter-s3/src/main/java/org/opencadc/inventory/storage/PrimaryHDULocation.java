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
 * Represents HDU locations specified by the PRIMARY specifier.
 * 
 * @author yeunga
 *
 */
public class PrimaryHDULocation extends HDULocation
{
    // Logger
    private static final Logger LOG = Logger.getLogger(PrimaryHDULocation.class);
    
    private static final String PRIMARY_SPECIFIER = "PRIMARY";
    private String primary = null;
    
    public static boolean isPrimaryHDULocation(String specifier)
    {
        LOG.debug("isPrimaryHDULocation: " + specifier);
        
    	boolean isPrimary = false;
    	
        if (StringUtil.hasLength(specifier))
        {
	    	String[] fields = specifier.split(HDULocation.FIELD_SEPARATOR);    	
	    	if (fields.length == 1 && StringUtil.hasLength(fields[0].trim()))
	    	{
	    		if (specifier.length() > 1)
	    			isPrimary = PRIMARY_SPECIFIER.equalsIgnoreCase(specifier);
	    		else
	    		{
	    			char ch = specifier.charAt(0);
	    			isPrimary = 'p' == ch || 'P' == ch;
	    		}    			
	    	}
        }
    	
        LOG.debug("isPrimaryHDULocation: returns " + isPrimary);
    	return isPrimary;
    }
    
    /**
     * Constructor with parameters.
     */
    public PrimaryHDULocation(String specifier)
    {
        LOG.debug("PrimaryHDULocation: " + specifier);
        
    	// validate name
        if (!StringUtil.hasLength(specifier))
            throw new IllegalArgumentException("Primary specifier is missing.");
        else if (specifier.length() > 1)
            validateLong(specifier);
       	else 
            validateShort(specifier);
        
        this.primary = specifier;
    }
    
    @Override
    public String toString()
    {
        return this.primary;
    }
    
    protected static void validateLong(String specifier)
    {
    	if (!(PRIMARY_SPECIFIER.equalsIgnoreCase(specifier)))
            throw new IllegalArgumentException("Extension type " + specifier + " is not supported.");	
    }
    
    protected static void validateShort(String specifier)
    {
    	char ch = specifier.charAt(0);
    	
    	if (!('p' == ch || 'P' == ch ))
            throw new IllegalArgumentException("Extension type " + specifier + " is not supported.");
    }
}

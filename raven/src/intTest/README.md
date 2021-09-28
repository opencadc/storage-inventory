The NegotiationTest integration tests require specific entries in the 
registry service reg-resource-caps.properties, and the raven raven.properties.

The registry service reg-resource-caps.properties requires the following entries.
```
ivo://negotiation-test-site1 = https://{host name}/{files service}/capabilities
ivo://negotiation-test-site2 = https://{host name}/{files service}/capabilities
ivo://negotiation-test-site3 = https://{host name}/{a different files service}/capabilities
```
The URL's must point to functional files services able to return the capabilities document.

The URL for the first two resourceID's can be the same.
The URL for the last resourceID `ivo://negotiation-test-site3` must be different 
from the URL's for the first two resourceID's. The difference in URL's is used to 
verify raven is prioritizing storage sites configured in raven.properties.

Example:
```
ivo://negotiation-test-site1 = https://test.cadc.dao.nrc.ca/minoc/capabilities
ivo://negotiation-test-site2 = https://test.cadc.dao.nrc.ca/minoc/capabilities
ivo://negotiation-test-site3 = https://test.cadc.dao.nrc.ca/minoc2/capabilities
```

raven.properties requires the following putPreference configuration:
```
org.opencadc.raven.putPreference=@SITE1
@SITE1.resourceID=ivo://negotiation-test-site1
@SITE1.namespaces=cadc:TEST/

org.opencadc.raven.putPreference=@SITE2
@SITE2.resourceID=ivo://negotiation-test-site2
@SITE2.namespaces=cadc:INT-TEST/

org.opencadc.raven.putPreference=@SITE3
@SITE3.resourceID=ivo://negotiation-test-site3
@SITE3.namespaces=cadc:TEST/
```

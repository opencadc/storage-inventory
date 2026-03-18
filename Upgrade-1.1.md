# Upgrading from Storage-Inventory 1.0 to 1.1

# phase 1: upgrade services
1. stop all validation jobs (`ratik`, `tantar`)
2. stop/disable all sync jobs (`fenwick`, `critwall`)
3. global: upgrade services (`luskan`, `raven`, `vault`) to **1.1** and restart
4. for each storage site: upgrade services (`luskan`, `minoc`) to **1.1** and restart
5. check service `/availability`

# phase 2: upgrade sync and validation

## minimal configuration changes: existing global
Assumption: global uses `artifactSelector = all`

1. add new `fenwick.properties` configuration:
```
org.opencadc.fenwick.instanceName = {name}
org.opencadc.fenwick.eventSelector = all
```
where `{name}` could be something simple like "all" or "single" or "the-only-one".

## minimal configuration changes: existing sites
Assumption:  sites use `artifactSelector = filter` 

2. add new `fenwick.properties` configuration:
```
org.opencadc.fenwick.instanceName = {name}
org.opencadc.fenwick.eventSelector = filter
```
where `{name}` could be something simple like "all" or "single" or "the-only-one".

3. add new config file `event-filter.sql` with the same `uri` conditions as are used in
`artifact-filter.sql` (probably just copy the file).

4. upgrade `fenwick`(s) (+config) to version to **1.1** and restart
5. upgrade `critwall` to version to **1.1** and restart
6. check logs for unexpected warnings/errors
7. upgrade `ratik` and `tantar` to **1.1** and re-enable schedule

# Out of scope:
- using new `fenwick` features to run multiple instances

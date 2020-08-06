# cadc-inventory-util (0.1.0)

Storage Inventory library for code shared by agents: critwall, fenwick, and tantar.

## org.opencadc.inventory.util.BucketSelector
Allows selecting a range of Bucket prefixes.  Useful for setting in `tantar` and `critwall`
where buckets are selected by a prefix, but it is too inefficient to set a single bucket
prefix.

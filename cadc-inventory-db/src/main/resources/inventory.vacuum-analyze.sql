-- periodic DB 
VACUUM VERBOSE ANALYZE <schema>.Artifact;
VACUUM VERBOSE ANALYZE <schema>.DeletedArtifactEvent;
VACUUM VERBOSE ANALYZE <schema>.DeletedStorageLocationEvent;
VACUUM VERBOSE ANALYZE <schema>.ObsoleteStorageLocationEvent;
VACUUM VERBOSE ANALYZE <schema>.StorageLocationEvent;
VACUUM VERBOSE ANALYZE <schema>.HarvestState;

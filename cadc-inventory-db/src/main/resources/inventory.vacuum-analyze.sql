-- periodic DB 
VACUUM VERBOSE ANALYZE <schema>.Artifact;
VACUUM VERBOSE ANALYZE <schema>.DeletedArtifact;
VACUUM VERBOSE ANALYZE <schema>.DeletedStorageLocation;
VACUUM VERBOSE ANALYZE <schema>.ObsoleteStorageLocation;
VACUUM VERBOSE ANALYZE <schema>.StorageLocation;
VACUUM VERBOSE ANALYZE <schema>.HarvestState;
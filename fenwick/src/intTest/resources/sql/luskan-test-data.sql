TRUNCATE TABLE inventory.DeletedStorageLocationEvent;
TRUNCATE TABLE inventory.DeletedArtifactEvent;
TRUNCATE TABLE inventory.Artifact;

INSERT INTO inventory.DeletedStorageLocationEvent
(id, lastmodified, metachecksum)
VALUES
('582ED7C2-7C53-4160-A408-9B0C56DB5459'::uuid, '2020-01-01 00:00:00.0', 'md5:66');

INSERT INTO inventory.DeletedArtifactEvent
(id, lastmodified, metachecksum)
VALUES
('26BA99ED-B5CA-44B6-891E-8D0C514DFE43'::uuid, '2020-01-01 00:00:00.0', 'md5:77');

INSERT INTO inventory.Artifact
(uri, uribucket, contentchecksum, contentlastmodified, contentlength, contenttype, contentencoding, sitelocations, storagelocation_storageid, storagelocation_storagebucket, lastmodified, metachecksum, id)
VALUES
('cadc:TEST/THREE/file3.fits', 'aa', 'md5:8888', '2020-01-01 00:00:00.0', 8888, 'application/fits', null, null, null, null, '2020-01-01 00:00:00.0', 'md5:9999', 'B559C18D-76A6-4B7B-9668-B3734087ED99'::uuid);

INSERT INTO inventory.Artifact
(uri, uribucket, contentchecksum, contentlastmodified, contentlength, contenttype, contentencoding, sitelocations, storagelocation_storageid, storagelocation_storagebucket, lastmodified, metachecksum, id)
VALUES
('cadc:TEST/TWO/file2.fits', 'ba', 'md5:888', '2020-01-01 00:00:00.0', 888, 'application/fits', null, null, null, null, '2020-01-01 00:00:00.0', 'md5:2bdcf480b061a8bb77fe4037d2b3cb96', '582ED7C2-7C53-4160-A408-9B0C56DB5459'::uuid);

INSERT INTO inventory.Artifact
(uri, uribucket, contentchecksum, contentlastmodified, contentlength, contenttype, contentencoding, sitelocations, storagelocation_storageid, storagelocation_storagebucket, lastmodified, metachecksum, id)
VALUES
('cadc:TEST/ONE/file1.fits', 'ab', 'md5:88', '2020-01-01 00:00:00.0', 88, 'application/fits', null, null, null, null, '2020-01-01 00:00:00.0', 'md5:ff5b77b6ec71ecb7199c6c738d795244', '26BA99ED-B5CA-44B6-891E-8D0C514DFE43'::uuid);
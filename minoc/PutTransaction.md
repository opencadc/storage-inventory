# File Upload in a Transaction

The normal file upload (HTTP PUT) to /minoc/files is a transaction that is automatically
committed if pre-conditions are satisfied:
* no errors detected
* content length matches value provided at start (optional)
* content checksum matches value provided at start (optional)

Note: where it says Content-MD5 this could/should/will also be the Digest HTTP header.

## PUT

If the caller provides pre-condition metadata with the initial request, the server can 
automatically abort failed uploads; this is the preferred approach. 

upload request:
```
HTTP PUT /minoc/files/{Artifact.uri}
Content-Length={number of bytes} (optional)
Content-MD5={MD5 checksum} (optional)
{body}
```
successful response:
```
201 (Created)
Content-Length={number of bytes stored}
Content-MD5={MD5 checksum of bytes stored}
```
At this point, the file is permanently stored. 

## PUT with Transaction

The *put with transaction* pattern allows a client to upload content, compute the length and 
checksum on-the-fly, and verify the upload before the commit. It works equally well for uploading 
files and streaming dynamic content as far as verifying that the upload was successful.

upload request:
```
HTTP PUT /minoc/files/{Artifact.uri}
X-Put-Txn=true
Content-Length={number of bytes} (optional)
Content-MD5={MD5 checksum} (optional)
{body}
```
successful response:
```
202 (Accepted)
X-Put-Txn={transaction id}
Content-Length={number of bytes stored}
Content-MD5={MD5 checksum of bytes stored}
```

state:
```
HTTP HEAD /minoc/files/{Artifact.uri}
X-Put-Txn={transaction id}
```
response: same as above

commit transaction:
```
HTTP PUT /minoc/files/{Artifact.uri}
Content-Length=0
X-Put-Txn={transaction id}
```
successful commit response:
```
201 (Created)
Content-Length={number of bytes stored}
Content-MD5={MD5 checksum of bytes stored}
```

abort transaction:
```
HTTP DELETE /minoc/files/{Artifact.uri}
X-Put-Txn={transaction id}
```
successful abort response:
```
204 (No Content)
```

The commit step (PUT X-Put-Txn={transaction id} with no body is potentially subtle, but the Content-Length = 0 (no more content)
does mean it won't happen by accident. **TBD**

Responses describe the current state of a transaction: 202 means transaction is open 
and response metadata is for the current state of all stored bytes.

Uncommitted transactions will eventually be aborted by the server; the client can 
help with cleanup by aborting manually. The behaviour of multiple transactions 
targetting the same Artifact.uri is undefined, so aborting the transaction of a failed upload
before retrying could potentially behave in a more predictable way.

## Put with Transaction and Append

With the concept of a transaction one can also define what is means to do a second PUT in 
the same transaction: append. This allows a client to resume an interrupted download.

upload request: as above

response: as above

state: as above

append:
```
HTTP PUT /minoc/files/{Artifact.uri}
X-Put-Txn={transaction id}
Content-Length=?
{body}
```

response:
```
202 (Accepted)
X-Put-Txn={transaction id}
Content-Length={number of bytes stored}
Content-MD5={MD5 checksum of bytes stored}
```

state, commit, and abort: as above

This pattern defines one feature: PUT can append by uploading more content in the same transaction. 
The client can make use of the Content-Length to decide if it needs to send more bytes. The client 
can only feasibly use the Content-MD5 to decide if it should commit or abort.

In the context of large files (~5GiB) the Content-Length header in the initial request 
is required so the implementation can decide (i) if resume will be supported, and (ii) how to
store the data.
* Content-Length not provided: txn, resume?, default behaviour: fail if exceed implementation limit
* Content-Length not provided, X-Large-Stream=true: txn, resume, implementation prepares to accept 
large amount of content, (client probably cannot resume)
* Content-Length provided: txn, resume?

A StorageAdapter implementation must support put with transaction. The implementation
will decide if resume is supported or not. If resume is not supported, the PUT with 
X-Put-Txn and {body} would fail with a 400 (405?).

## Implementation Expectations

The StorageAdapter invocation will have to change to support passing additional info from
minoc into the adapter implementation (txnid, large-stream hint, commit, abort)... probably 
by changing the interface. **TBD**

OpaqueFileSystemStorageAdapter already has a transaction/commit model and can support txn and 
resume for any file with no enforced limit.

SwiftStorageAdapter can support txn but not resume for files stored as single data objects.
The transaction will probably be done via some attribute on the object that causes the iterator to
skip; commit would mean removing the attr. **TBD**

SwiftStorageAdapter can support txn and resume for files stored as segmented large objects. The 
adapter has to create a new chunk periodically (5GiB limit per chunk) and each resume has to 
create a new chunk as well. Uploads with many stop/resume(s) and thus many small chunks will be
stored very inefficiently and at some point this pattern should be rejected -- maybe the adapter
should abort txn if it ends up creating chunks smaller than some limit (1GiB?), except the last 
chunk. There is a limit on number of chunks (default: 1000), container listing may be used
to access chunks, and will be used to validate chunks.

**Future proofing**: there are several potential enhancements that could be added in the future that will 
guide implementation choices, especially in the SwiftStorageAdapter and how CEPH object store is used.

## Deployment Considerations

In order to support resume, a minoc instance must have the MessageDigest instance that digested 
there bytes from the beggining of the PUT and continue to digest bytes with that instance. The two 
possible ways to implement this are (i) keep the instance in memory and require request affinity 
based on the txnid (X-Put-Txn header) or (ii) use a MessageDigest implementation that can be serialised 
so it can be stored at the end of a request and re-instantiated when a new request with that txnid
begins. **TBD**

## A Tale of Two Conflicting Use Cases

1. The most simple put of a small file: client does not provide any metadata (length and md5)
and simply streams the bytes. We want to store that in the lowest overhead way possible because there are 
many such files, which means storing the small file as a single object in object store (cadc-storage-adpater-ceph).

2. Currently works: a TAP (youcat) query with output to VOSpace (vault) also does not provide any metadata and simply
streams the bytes. For a query like `select * from cfht.megapipe` the resulting file was ~40GiB. From the client side, 
this is not resumable. We want to store that in the most flexible way possible because the byte stream may be large, 
so this identical-looking request needs to be stored in multiple chunks in object store (cadc-storage-adpater-ceph).
*This use case justifies the definition of X-Large-Stream hint and associated behaviour.*


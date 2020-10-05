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
checksum on-the-fly, and verify the upload before the commit. 

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

get current state:
```
HTTP HEAD /minoc/files/{Artifact.uri}
X-Put-Txn={transaction id}
```
response: same as above

commit transaction:
```
HTTP PUT /minoc/files/{Artifact.uri}
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

Responses describe the current state of a transaction: 202 means transaction is open 
and response metadata is for the current state of all stored bytes.

Uncommitted transactions will eventually be aborted by the server; the client can 
help with cleanup by aborting manually. The behaviour of multiple transactions 
targetting the same Artifact.uri is undefined, so aborting the transaction of a failed upload
before retrying will behave in a predictable way.

## Put with Transaction and Resume

upload request: as above

response: as above; intermediate metadata for the content that was stored

append more content:
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

get state, commit, and abort: as above

This pattern defines one feature: PUT can append by uploading more content in the same transaction. The client can make use of the
Content-Length to decide if it needs to send more bytes. Te client can only feasibly use the Content-MD5 to decide if it should commit 
or abort.

TBD: In the context of large files (~5GiB) the Content-Length header in the initial request 
is required so the implementation can decide (i) if resume will be supported, and (ii) how to
store the data. implementation limitations give something like:
* Content-Length not provided: txn, resume, default behaviour: fail if exceed implementation limit
* Content-Length not provided, X-Content-Stream=true: txn, (resume), implementation prepares to accept large amount of content, client
  probably cannot resume
* Content-Length provided, {length} <= X: txn, no resume
* Content-Length provided, {length} > X: txn, resume
If resume is not supported, the PUT with X-Put-Txn and {body} would fail with a 400 (405?).

Q. Is it necessary to tell the client that resume is not supported before hand? It means defining a second custom
header and doesn't really add anything.

## A Tale of Two Conflicting Use Cases

1. The most simple put of a small file: client does not provide any metadata (length and md5)
and simply streams the bytes. We want to store that in the lowest overhead way possible because there are 
many such files, which means storing the small file as a single object in object store (cadc-storage-adpater-ceph).

2. Currently works: a TAP (youcat) query with output to VOSpace (vault) also does not provide any metadata and simply
streams the bytes. For a query like `select * from cfht.megapipe` the resulting file was ~40GiB. From the client side, 
this is not resumable. We want to store that in the most flexible way possible because the byte stream may be large, 
so this identical-looking request needs to be stored in multiple chunks in object store (cadc-storage-adpater-ceph).
*This use case justifies the definition of X-Content-Stream=true and associated behaviour.*


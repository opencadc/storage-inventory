# File Upload in a Transaction

The normal file upload (HTTP PUT) to /minoc/files is a transaction that is automatically
committed if pre-conditions are satisfied:
* no errors detected
* content length matches value provided at start (optional)
* content checksum matches value provided at start (optional)

## PUT

If the caller provides pre-condition metadata with the initial request, the server can 
automatically abort failed uploads; this is the preferred approach. 

upload request:
```
HTTP PUT /minoc/files/{Artifact.uri}
content-length={number of bytes in body} (optional)
digest={checksum of bytes} (optional)
{body}
```
successful response:
```
201 (Created)
content-length=0 (no response body)
digest={checksum of bytes stored}
```
At this point, the file is permanently stored. 

## PUT with Transaction

The *put with transaction* pattern allows a client to upload content, compute the length and 
checksum on-the-fly, and verify the upload before the commit. It works equally well for uploading 
files and streaming dynamic content as far as verifying that the upload was successful.

Transactions are created and committed using PUT; they are reverted (to previous state) or aborted
with POST.

upload request:
```
HTTP PUT /minoc/files/{Artifact.uri}
x-put-txn-op=start
content-length={number of bytes in body} (optional)
digest={checksum of bytes} (optional)
{body}
```
successful response:
```
202 (Accepted)
x-put-txn-id={transaction id}
content-length=0 (no response body)
digest={checksum of bytes stored}
```

state:
```
HTTP HEAD /minoc/files/{Artifact.uri}
x-put-txn-id={transaction id}
content-length={current length in bytes}
digest={checksum of bytes stored}
```
response: same as above

commit transaction:
```
HTTP PUT /minoc/files/{Artifact.uri}
x-put-txn-id={transaction ID}
x-put-txn-op=commit
content-length=0 (no more bytes)
```
successful commit response:
```
201 (Created)
content-length=0 (no response body)
digest={checksum of bytes stored}
```

abort transaction:
```
HTTP POST /minoc/files/{Artifact.uri}
x-put-txn-id={transaction id}
x-put-txn-op=abort
```
successful abort response:
```
204 (No Content)
```

Responses describe the current state of a transaction: 202 means transaction is open 
and response metadata is for the current state of all stored bytes.

Uncommitted transactions will eventually be aborted by the server; the client can 
help with cleanup by aborting explicitly. The behaviour of multiple transactions 
targetting the same Artifact.uri is undefined, so aborting the transaction of a failed upload
before retrying could potentially behave in a more predictable way.

## Put with Transaction and Append

With the concept of a transaction one can also define what it means to do a second PUT in 
the same transaction: append. This allows a client to perform an upload broken up over
multiple requests.

upload request:
```
HTTP PUT /minoc/files/{Artifact.uri}
x-put-txn-op=start
x-total-length={total number of bytes} (optional)
content-length={number of bytes in this request} (optional)
digest={checksum of bytes} (optional)
{body}
```

or explicit start transaction with no content:
```
HTTP PUT /minoc/files/{Artifact.uri}
x-put-txn-op=start
x-total-length={total number of bytes} (optional)
content-length=0
digest={checksum of bytes stored} (optional)
```

successful response:
```
202 (Accepted)
x-put-txn-id={transaction id}
x-put-segment-minbytes={minimum segment size} (optional)
x-put-segment-maxbytes={maximum segment size} (optional)
content-length=0 (no response body)
```
with no digest header because there are 0 bytes.

If the initial put has content-length=0 then this starts a transaction and gets server constraints
without sending the first segment. The x-total-length header value helps the server decide how to
store the data object and may effect constraints. The x-put-segment-minbytes and x-put-segment-maxbytes,
if present in the response, convey the min and max segment size for subsequent requests; the minimum applies
to all segments except the last, which can be smaller. 

For example, to upload a 12GiB file to a CEPH-backed minoc, the x-total-length is 12GiB (expressed in
bytes); the server may respond with x-put-segment-minbytes=4GiB (in bytes) and x-put-segment-maxbytes=5GiB
(in bytes). The client can then PUT 5, 5, 2 (GiB), or 5, 4, 3 (GiB), or 4, 4, 4 (GiB) segments. 

When  uploading to a filesystem-backed minoc, the server may respond with x-put-segment-minbytes=1 (byte)
and no x-put-segment-maxbytes, meaning that segments may be any size at all. **Clients cannot simply use the 
min or the max; they must chose a suitable and allowed size that balances overhead, throughput, and robustness.**

state: as above

append:
```
HTTP PUT /minoc/files/{Artifact.uri}
x-put-txn-id={transaction id}
content-length={number of bytes in body}
{body}
```

response:
```
202 (Accepted)
x-put-txn-id={transaction id}
content-length=0 (no response body)
digest={checksum of bytes stored}
```

revert last append:
```
HTTP POST /minoc/files/{Artifact.uri}
x-put-txn-id={transaction id}
x-put-txn-op=revert
```
response:
```
202 (Accepted)
x-put-txn-id={transaction id}
content-length=0 (no response body)
digest={checksum of bytes stored}
```
If the first "chunk" is reverted to 0 bytes, there will be no digest in the response.

if the previous append cannot be reverted:
```
400 (Bad Request)
```

commit and abort: as above

## HTTP header info

* content-length always describes the size of the body of the current request
* x-total-length specifies the total file size in cases where append is going to be used
* x-put-txn-op=start|revert|abort|commit
* x-out-txn-id={transaction ID}
* x-total-length={total length of file in bytes}

* x-put-segment-minbytes={minimum segment size} (optional)
* x-put-segment-maxbytes={maximum segment size} (optional)

This pattern defines one feature: PUT can append by uploading more content in the same transaction. 
The client can make use of the content-length to decide if it needs to send more bytes. The client 
can only feasibly use the checksum to decide if it should commit or abort.

In the context of large files (~5GiB) the total length in the initial request is required so the
implementation can decide (i) if resume will be supported, and (ii) how to store the data.

* total length provided: txn, resumable up to server
* total length not provided: txn, resumable? fail if exceed implementation limit?
* total length length not known because client is streaming but expected to be large: x-total-length=large??, txn, resume unlikely but early detection of fail and abort

A StorageAdapter implementation must support put with transaction. The implementation
will decide if resume is supported or not. If resume is not supported, the PUT with 
x-out-txn-id header and {body} would fail with a 400.

**Future proofing**: there are several potential enhancements that could be added in the future that will 
guide implementation choices, especially in the SwiftStorageAdapter and how CEPH object store is used.

## A Tale of Two Conflicting Use Cases

1. The most simple put of a small file: client does not provide any metadata (length and md5)
and simply streams the bytes. We want to store that in the lowest overhead way possible because there are 
many such files, which means storing the small file as a single object in object store 
(cadc-storage-adapter-swift).

2. Currently works: a TAP (youcat) query with output to VOSpace (vault) does not provide any 
metadata (length and md5) and simply streams the bytes. For a query like `select * from cfht.megapipe` 
the resulting file was ~40GiB. From the client side, this is not resumable. We want to store that in 
the most flexible way possible because the byte stream may be large, so this identical-looking request 
needs to be stored in multiple chunks in object store (cadc-storage-adapter-swift).

*This use case justifies the definition of X-Large-Stream hint and associated behaviour.*


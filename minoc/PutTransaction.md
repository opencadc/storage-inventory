# File Upload in a Transaction

The normal file upload (HTTP PUT) to /minoc/files is a transaction that is automatically
committed if pre-conditions are satisfied:
* no errors detected
* content length matches value provided at start (optional)
* content checksum matches value provided at start (optional)

## PUT
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
Content-Length=
Content-MD5=
```

## PUT with Transaction

If the caller provides pre-condition metadata with the initial request, the server can 
automatically abort failed uploads; this is the preferred approach. The *put with transaction* 
pattern allows a client to upload content, compute the length and checksum on-the-fly, and 
verify the upload before the commit. 

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
Content-Length=
Content-MD5=
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
Content-Length=
Content-MD5=
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
targetting the same URI is undefined, so aborting the transaction of a failed upload
before retrying will behave in a predictable way.

## Put with Transaction and Resume

upload request: as above but Content-Length required?

response: as above; intermediate metadata for the content that was stored

append more content:
```
HTTP PUT /minoc/files/{Artifact.uri}
X-Put-Txn={transaction id}
{body}
```

response:
```
202 (Accepted)
X-Put-Txn={transaction id}
Content-Length=
Content-MD5=
```

get current state:
```
HTTP HEAD /minoc/files/{Artifact.uri}
X-Put-Txn={transaction id}
```

response:
```
202 (Accepted)
X-Put-Txn={transaction id}
Content-Length=
Content-MD5=
```

commit and abort: as above

This pattern defines one feature: PUT can append by uploading more 
content in the same transaction.

TBD: In the context of large files (~5GiB) the Content-Length header in the initial request 
is required so the implementation can decide (i) if resume will be supported, and (ii) how to
store the data. implementation limitations give something like:
* Content-Length not provided: txn, no resume, file size limit then fail
* Content-Length provided, {length} <= X: txn, no resume
* Content-Length provided, {length} > X: txn, resume
If resume is not supported, the PUT with X-Put-Txn and {body} would fail with a 400 (405?).


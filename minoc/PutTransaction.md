# File Upload in a Transaction

The normal file upload (HTTP PUT) to /minoc/files is a transaction that is automatically
committed if pre-conditions are satisfied:
* no errors detected
* content length matches value provided at start (optional)
* content checksum matchaes value provided at start (optional)


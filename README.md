# ephemeral-flight-server

The purpose of this repo is to demonstrate an ephemeral Flight SQL server that is spawned via a context manager. The
server uses:

- TLS encryption
- Basic user / password authentication
- DuckDB as a database

 When the context manager exits, the server is shutdown.
 
For a code example, see the [`ephemeral-server-demo.py`](ephemeral-server-demo.py)
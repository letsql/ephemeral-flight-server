import pathlib
import time

import pyarrow

from demo import BasicAuthServerMiddlewareFactory, Connection, NoOpAuthHandler
from demo.client import DuckDBFlightClient

tls_certificates = []

scheme = "grpc+tls"
host = "localhost"
port = "5005"

root = pathlib.Path(__file__).resolve().parent

certificate_path = root / "tls" / "server.crt"
with open(certificate_path, "rb") as cert_file:
    tls_cert_chain = cert_file.read()

key_path = root / "tls" / "server.key"
with open(key_path, "rb") as key_file:
    tls_private_key = key_file.read()

tls_certificates.append((tls_cert_chain, tls_private_key))
location = "{}://{}:{}".format(scheme, host, port)

with Connection(
    location=location,
    tls_certificates=tls_certificates,
    auth_handler=NoOpAuthHandler(),
    middleware={
        "basic": BasicAuthServerMiddlewareFactory(
            {
                "test": "password",
            }
        )
    },
) as conn:
    time.sleep(5)

    client = DuckDBFlightClient(
        host="localhost",
        port=5005,
        username="test",
        password="password",
        tls_roots=certificate_path,
    )

    # Create a sample table
    data = pyarrow.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    client.upload_data("users", data)

    # List tables
    print("Available tables:", client.list_tables())

    # Get table info
    print("Table schema:", client.get_table_info("users"))

    # Execute a query
    result = client.execute_query("SELECT * FROM users WHERE id > 1")
    print("Query result:", result.to_pandas())

import pathlib
import datetime

import pyarrow
import pandas as pd

from demo import BasicAuthServerMiddlewareFactory, Connection, NoOpAuthHandler
from demo.client import DuckDBFlightClient

def instrument_reader(reader, prefix=""):
    def gen(reader):
        print(f"{prefix}first batch yielded at {datetime.datetime.now()}")
        yield next(reader)
        yield from reader
        print(f"{prefix}last batch yielded at {datetime.datetime.now()}")
    return pyarrow.RecordBatchReader.from_batches(reader.schema, gen(reader))

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

    command = "echo"
    data = pyarrow.Table.from_pandas(pd.DataFrame({"a": range(100_000)}))

    fut, rbr = client.do_exchange_batches(
        command,
        instrument_reader(data.to_reader(max_chunksize=100)),
    )

    first_batch = next(rbr)
    print(f"got first batch at {datetime.datetime.now()}")
    rest = rbr.read_pandas()
    print(f"got rest at {datetime.datetime.now()}")
    print(rest)
    print(f"fut.result(): {fut.result()}")


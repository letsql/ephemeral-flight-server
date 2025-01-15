import datetime
import pathlib

import pandas as pd
import pyarrow

from demo import EphemeralServer, BasicAuth
from demo.client import DuckDBFlightClient


def instrument_reader(reader, prefix=""):
    def gen(reader):
        print(f"{prefix}first batch yielded at {datetime.datetime.now()}")
        yield next(reader)
        yield from reader
        print(f"{prefix}last batch yielded at {datetime.datetime.now()}")

    return pyarrow.RecordBatchReader.from_batches(reader.schema, gen(reader))


root = pathlib.Path(__file__).resolve().parent

certificate_path = root / "tls" / "server.crt"
key_path = root / "tls" / "server.key"

scheme = "grpc+tls"
host = "localhost"
port = "5005"
location = "{}://{}:{}".format(scheme, host, port)

with EphemeralServer(
        location=location,
        certificate_path=certificate_path,
        key_path=key_path,
        auth=BasicAuth("test", "password"),
) as server:
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

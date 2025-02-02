import datetime

import pandas as pd
import pyarrow

from demo import EphemeralServer, BasicAuth
from demo.client import FlightClient
from util import certificate_path, key_path, scheme, host, port


def instrument_reader(reader, prefix=""):
    def gen(reader):
        print(f"{prefix}first batch yielded at {datetime.datetime.now()}")
        yield next(reader)
        yield from reader
        print(f"{prefix}last batch yielded at {datetime.datetime.now()}")

    return pyarrow.RecordBatchReader.from_batches(reader.schema, gen(reader))


location = "{}://{}:{}".format(scheme, host, port)

with EphemeralServer(
    location=location,
    certificate_path=certificate_path,
    key_path=key_path,
    auth=BasicAuth("test", "password"),
) as server:
    client = FlightClient(
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

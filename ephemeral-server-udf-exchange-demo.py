import datetime
import pathlib

import pandas as pd
import pyarrow as pa

from demo import EphemeralServer, BasicAuth
from demo.action import AddExchangeAction
from demo.client import DuckDBFlightClient
from demo.exchanger import UDFExchanger


def instrument_reader(reader, prefix=""):
    def gen(reader):
        print(f"{prefix}first batch yielded at {datetime.datetime.now()}")
        yield next(reader)
        yield from reader
        print(f"{prefix}last batch yielded at {datetime.datetime.now()}")

    return pa.RecordBatchReader.from_batches(reader.schema, gen(reader))


def my_f(df):
    return df[["a", "b"]].sum(axis=1)

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

    udf_exchanger = UDFExchanger(
        my_f,
        schema_in=pa.schema(
            (
                pa.field("a", pa.int64()),
                pa.field("b", pa.int64()),
            )
        ),
        name="x",
        typ=pa.int64(),
        append=True,
    )
    action_respose = client.do_action(
        AddExchangeAction.name, udf_exchanger, options=client._options
    )

    # a small example
    df_in = pd.DataFrame({"a": [1], "b": [2], "c": [100]})
    fut, rbr = client.do_exchange(
        udf_exchanger.command,
        pa.RecordBatchReader.from_stream(df_in),
    )
    df_out = rbr.read_pandas()
    print(f"action_respose: {action_respose}")
    print(f"fut.result: {fut.result()}")
    print(f"df_out:\n{df_out}")

    # demonstrate streaming
    df_in = pd.DataFrame(
        {
            "a": range(100_000),
            "b": range(100_000, 200_000),
            "c": range(200_000, 300_000),
        }
    )
    fut, rbr = client.do_exchange_batches(
        udf_exchanger.command,
        instrument_reader(pa.Table.from_pandas(df_in).to_reader(max_chunksize=100)),
    )
    first_batch = next(rbr)
    print(f"got first batch at {datetime.datetime.now()}")
    rest = rbr.read_pandas()
    print(f"got rest at {datetime.datetime.now()}")
    print(rest)
    print(f"fut.result(): {fut.result()}")

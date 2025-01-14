import datetime
import pathlib

import pyarrow

from demo import BasicAuthServerMiddlewareFactory, Connection, NoOpAuthHandler, make_client
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
):
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


    con = make_client(
        host="localhost",
        port=5005,
        username="test",
        password="password",
        tls_roots=certificate_path,
    )

    t = con.table("users")
    expr = t.filter(t.id > 1).select(t.name)

    assert (res := expr.execute())  is not None
    print(res)








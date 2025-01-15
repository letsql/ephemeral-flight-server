import pathlib

import pyarrow as pa

from demo import EphemeralServer, make_client, BasicAuth

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
    con = make_client(server)

    # Create a sample table
    data = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    t = con.read_in_memory(data, table_name="users")
    expr = t.filter(t.id > 1).select(t.name)

    assert (res := expr.execute()) is not None
    print(res)

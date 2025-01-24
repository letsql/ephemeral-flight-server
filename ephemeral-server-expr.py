import pyarrow as pa

from demo import EphemeralServer, make_con, BasicAuth
from util import certificate_path, key_path, scheme, host, port

location = "{}://{}:{}".format(scheme, host, port)

with EphemeralServer(
    location=location,
    certificate_path=certificate_path,
    key_path=key_path,
    auth=BasicAuth("test", "password"),
) as server:
    con = make_con(server)

    # Create a sample table
    data = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    t = con.read_in_memory(data, table_name="users")
    expr = t.filter(t.id > 1).select(t.name)

    assert (res := expr.execute()) is not None
    print(res)

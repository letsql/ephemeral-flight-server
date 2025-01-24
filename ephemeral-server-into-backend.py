import pyarrow as pa
import letsql as ls

from letsql.expr.relations import into_backend

from demo import EphemeralServer, make_con, BasicAuth
from util import certificate_path, key_path, scheme, host, port



with EphemeralServer(
    location="{}://{}:{}".format(scheme, host, port),
    certificate_path=certificate_path,
    key_path=key_path,
    auth=BasicAuth("test", "password"),
) as main:
    with EphemeralServer(
        location="{}://{}:{}".format(scheme, host, "5006"),
        certificate_path=certificate_path,
        key_path=key_path,
        auth=BasicAuth("test", "password"),
    ) as second:
        main_con = make_con(main)
        second_con = make_con(second)

        # Create a sample table
        data = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        t = main_con.read_in_memory(data, table_name="users")
        expr = t.filter(t.id > 1)

        main_t = into_backend(expr, second_con, name="main_users")
        s = second_con.read_in_memory(data, table_name="users")

        res = s.join(main_t, "id").select("id", name="name_right").pipe(ls.execute)

        assert res is not None
        print(res)
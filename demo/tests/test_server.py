import socket

import letsql as ls
import pandas as pd
import pytest
import pyarrow as pa

from demo import EphemeralServer, BasicAuth, make_con
from util import certificate_path, key_path, scheme, host

def port_in_use(port, host='localhost'):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return False
        except socket.error:
            return True

@pytest.mark.parametrize(
    "connection,port",
    [
        pytest.param(ls.duckdb.connect, 5005, id="duckdb"),
        pytest.param(ls.connect, 5005, id="letsql"),
    ],
)
def test_create_and_list_tables(connection, port):

    assert not port_in_use(port), f"Port {port} already in use"

    with EphemeralServer(
        location="{}://{}:{}".format(scheme, host, port),
        certificate_path=certificate_path,
        key_path=key_path,
        auth=BasicAuth("test", "password"),
        connection=connection,
    ) as main:
        con = make_con(main)

        data = pa.table(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
        ).to_pandas()

        t = con.register(data, table_name="users")
        actual = ls.execute(t)

        assert port_in_use(port)
        assert "users" in con.tables
        assert isinstance(actual, pd.DataFrame)

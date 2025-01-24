import time

import letsql as ls
import pytest

from demo import EphemeralServer, BasicAuth, make_con
@pytest.mark.parametrize(
    "connection,port",
    [
        pytest.param(ls.duckdb.connect, 5005, id="duckdb"),
        pytest.param(ls.connect, 5006, id="letsql"),
        pytest.param(ls.datafusion.connect, 5007, id="datafusion"),
    ],
)
def test_create_and_list_tables(connection, port):
    # check that the port is occupied
    with EphemeralServer(
            location="{}://{}:{}".format(scheme, host, port),
            certificate_path=certificate_path,
            key_path=key_path,
            auth=BasicAuth("test", "password"),
            connection=connection,
    ) as main:
        con = make_con(main)
    assert main.p.exitcode == 0


from util import certificate_path, key_path, scheme, host
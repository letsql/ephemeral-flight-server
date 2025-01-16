import pathlib

import pandas as pd
import letsql as ls
from letsql.expr.relations import into_backend

from demo import EphemeralServer, make_client, BasicAuth
from util import certificate_path, key_path, scheme, host, port

root = pathlib.Path(__file__).resolve().parent
batting_path = root / 'data' / 'batting.parquet'

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
        main_con = make_client(main)
        second_con = make_client(second)

        left = main_con.read_parquet(str(batting_path), table_name="batting")
        right = second_con.read_parquet(str(batting_path), table_name="batting")

        left_t = left[lambda t: t.yearID == 2015]
        right_t = right[lambda t: t.yearID == 2014]

        expr = left_t.join(into_backend(right_t, main_con), "playerID")
        result = (
            expr
            .pipe(ls.execute)
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

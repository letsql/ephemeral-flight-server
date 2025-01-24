import pathlib

import pandas as pd
import letsql as ls
from letsql.expr.relations import into_backend

from demo import EphemeralServer, make_con, BasicAuth
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
        con0 = make_con(main)
        con1 = make_con(second)

        df_groups = {
            "a": pd.DataFrame({"time": [1, 3, 5]}, ),
            "b": pd.DataFrame({"time": [2, 4, 6]}, ),
            "c": pd.DataFrame({"time": [2.5, 4.5, 6.5]}, ),
        }

        for name, df in df_groups.items():
            con0.register(df, f"df-{name}")
        dct = {
            table: ls.expr.relations.into_backend(con0.table(table), con1, f"remote-{table}")
            for table in reversed(list(con0.tables))
        }
        (t, other, *others) = tuple(dct.values())
        for other in others[:1]:
            t = t.asof_join(other, "time").drop("time_right")
        out = ls.execute(t)
        print(out)

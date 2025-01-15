from operator import itemgetter
from typing import Mapping, Any

import pandas as pd
import pyarrow as pa
from ibis import util
from ibis.common.collections import FrozenOrderedDict
from ibis.expr import types as ir, schema as sch
from letsql.backends.duckdb import Backend as DuckDBBackend

from demo.client import DuckDBFlightClient


class Backend(DuckDBBackend):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.con = None

    def do_connect(
        self,
        host="localhost",
        port=8815,
        username="test",
        password="password",
        tls_roots=None,
    ) -> None:
        self.con = DuckDBFlightClient(
            host=host,
            port=port,
            username=username,
            password=password,
            tls_roots=tls_roots,
        )

    def get_schema(
        self,
        table_name: str,
        *,
        catalog: str | None = None,
        database: str | None = None,
    ) -> sch.Schema:
        fields = self.con.get_table_info(table_name)[0]
        names, types, nullables = zip(*map(itemgetter(0, 1, 2), fields))

        type_mapper = self.compiler.type_mapper

        return sch.Schema(
            FrozenOrderedDict(
                {
                    name: type_mapper.from_string(typ, nullable=null == "YES")
                    for name, typ, null in zip(names, types, nullables)
                }
            )
        )

    def read_in_memory(
        self,
        source: pd.DataFrame | pa.Table | pa.RecordBatchReader,
        table_name: str | None = None,
    ) -> ir.Table:
        table_name = table_name or util.gen_name("read_in_memory")
        self.con.upload_data(table_name, source)
        return self.table(table_name)

    def to_pyarrow_batches(
        self,
        expr: ir.Expr,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
        chunk_size: int = 10_000,
        **_: Any,
    ) -> pa.ipc.RecordBatchReader:
        table_expr = expr.as_table()
        sql = self.compile(table_expr, limit=limit, params=params)
        return self.con.execute_batches(sql)

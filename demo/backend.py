from operator import itemgetter
from pathlib import Path
from typing import Mapping, Any, Sequence

import pyarrow as pa
from ibis.expr import types as ir, schema as sch
from letsql.backends.duckdb import Backend as DuckDBBackend

import sqlglot as sg
import sqlglot.expressions as sge

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
        self.con = DuckDBFlightClient(host=host, port=port, username=username, password=password, tls_roots=tls_roots)

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
            {
                name: type_mapper.from_string(typ, nullable=null == "YES")
                for name, typ, null in zip(names, types, nullables)
            }
        )



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
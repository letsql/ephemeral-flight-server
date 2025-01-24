from operator import itemgetter
from pathlib import Path
from typing import Mapping, Any, Iterable

import ibis
import pandas as pd
import pyarrow as pa
import sqlglot as sg
from ibis import util
from ibis.backends.sql import SQLBackend
from ibis.common.collections import FrozenOrderedDict
from ibis.expr import types as ir, schema as sch
from letsql.backends.duckdb import Backend as DuckDBBackend

from demo.action import DropTableAction, DropViewAction, ReadParquetAction, ListTablesAction
from demo.client import FlightClient


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
        self.con = FlightClient(
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

    def list_tables(
        self,
        like: str | None = None,
        database: tuple[str, str] | str | None = None,
        schema: str | None = None,
    ) -> list[str]:

        args = {
            "like": like,
            "database": database,
            "schema": schema,
        }

        return self.con.do_action(ListTablesAction.name, action_body=args, options=self.con._options)

    def drop_table(
        self,
        name: str,
        database: tuple[str, str] | str | None = None,
        force: bool = False,
    ) -> None:
        self.con.do_action(DropTableAction.name, action_body=name, options=self.con._options)

    def drop_view(
        self,
        name: str,
        *,
        database: str | None = None,
        schema: str | None = None,
        force: bool = False,
    ) -> None:
        self.con.do_action(DropViewAction.name, action_body=name, options=self.con._options)

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

        def gen(chunks):
            for chunk in chunks:
                yield chunk.data

        batches = self.con.execute_batches(sql)
        return pa.RecordBatchReader.from_batches(batches.schema, gen(batches))

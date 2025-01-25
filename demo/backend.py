from pathlib import Path
from typing import Mapping, Any, Iterable

import pandas as pd
import pyarrow as pa
from ibis import util
from ibis.expr import types as ir, schema as sch
from letsql.backends.duckdb import Backend as DuckDBBackend

from demo.action import (
    DropTableAction,
    DropViewAction,
    ReadParquetAction,
    ListTablesAction,
)
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
        return self.con.get_table_info(table_name)

    def read_in_memory(
        self,
        source: pd.DataFrame | pa.Table | pa.RecordBatchReader,
        table_name: str | None = None,
    ) -> ir.Table:
        table_name = table_name or util.gen_name("read_in_memory")

        if isinstance(source, pa.Table):
            self.con.upload_data(table_name, source)
        elif isinstance(source, pa.RecordBatchReader):
            self.con.upload_batches(table_name, source)
        return self.table(table_name)

    def read_parquet(
        self,
        source_list: str | Iterable[str],
        table_name: str | None = None,
        **kwargs: Any,
    ) -> ir.Table:
        args = {
            "source_list": source_list,
            "table_name": table_name,
        }
        self.con.do_action(
            ReadParquetAction.name, action_body=args, options=self.con._options
        )
        return self.table(table_name)

    def register(
        self,
        source: str | Path | Any,
        table_name: str | None = None,
        **kwargs: Any,
    ) -> ir.Table:
        if isinstance(source, pd.DataFrame):
            source = pa.Table.from_pandas(source)
            source = pa.RecordBatchReader.from_batches(
                source.schema, source.to_batches()
            )

        if isinstance(source, pa.RecordBatchReader):
            self.con.upload_batches(table_name, source)

        return self.table(table_name)

    @property
    def tables(self):
        res = self.con.do_action(
            ListTablesAction.name, action_body="list_tables", options=self.con._options
        )
        return res[0]

    def drop_table(
        self,
        name: str,
        database: tuple[str, str] | str | None = None,
        force: bool = False,
    ) -> None:
        self.con.do_action(
            DropTableAction.name, action_body=name, options=self.con._options
        )

    def drop_view(
        self,
        name: str,
        *,
        database: str | None = None,
        schema: str | None = None,
        force: bool = False,
    ) -> None:
        self.con.do_action(
            DropViewAction.name, action_body=name, options=self.con._options
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
        # table_expr = expr.as_table()
        # sql = self.compile(table_expr, limit=limit, params=params)

        def gen(chunks):
            for chunk in chunks:
                yield chunk.data

        batches = self.con.execute_batches(expr, params=params, limit=limit, chunk_size=chunk_size)
        return pa.RecordBatchReader.from_batches(batches.schema, gen(batches))

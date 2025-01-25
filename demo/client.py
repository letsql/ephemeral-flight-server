import argparse
import json
import time

from concurrent.futures import ThreadPoolExecutor
from queue import Queue

import pyarrow
import pyarrow.flight

from cloudpickle import dumps, loads

executor = ThreadPoolExecutor()


class FlightClient:
    def __init__(
        self,
        host="localhost",
        port=8815,
        username="test",
        password="password",
        tls_roots=None,
    ):
        """
        Initialize the DuckDB Flight Client

        Args:
            host: Server host
            port: Server port
        """
        kwargs = {}

        if tls_roots:
            with open(tls_roots, "rb") as root_certs:
                kwargs["tls_root_certs"] = root_certs.read()

        self._client = pyarrow.flight.FlightClient(
            f"grpc+tls://{host}:{port}", **kwargs
        )
        self._wait_on_healthcheck()
        token_pair = self._client.authenticate_basic_token(
            username.encode(), password.encode()
        )
        self._options = pyarrow.flight.FlightCallOptions(headers=[token_pair])

    def _wait_on_healthcheck(self):
        while True:
            try:
                self.do_action(
                    "healthcheck",
                    options=pyarrow.flight.FlightCallOptions(timeout=1),
                )
                print("done healthcheck")
                break
            except pyarrow.ArrowIOError as e:
                if "Deadline" in str(e):
                    print("Server is not ready, waiting...")
                else:
                    raise e
            except pyarrow.flight.FlightUnavailableError:
                pass
            except pyarrow.flight.FlightUnauthenticatedError:
                break
            finally:
                n_seconds = 1
                print(f"Flight server unavailable, sleeping {n_seconds} seconds")
                time.sleep(n_seconds)

    def execute_query(self, query):
        """
        Execute SQL query and return results as Arrow table

        Args:
            query: SQL query string

        Returns:
            pyarrow.Table
        """

        batches = self.execute_batches(query)
        return batches.read_all()

    def execute_batches(self, query):
        # Get FlightInfo
        flight_info = self._client.get_flight_info(
            pyarrow.flight.FlightDescriptor.for_command(dumps(query)),
            options=self._options,
        )

        # Get the first endpoint
        endpoint = flight_info.endpoints[0]

        # Get the result
        reader = self._client.do_get(endpoint.ticket, options=self._options)

        return reader

    def upload_data(self, table_name, data):
        """
        Upload data to create or replace a table

        Args:
            table_name: Name of the table to create
            data: pyarrow.Table containing the data
        """
        writer, _ = self._client.do_put(
            pyarrow.flight.FlightDescriptor.for_command(table_name.encode("utf-8")),
            data.schema,
            options=self._options,
        )
        writer.write_table(data)
        writer.close()

    def upload_batches(self, table_name, reader):
        writer, _ = self._client.do_put(
            pyarrow.flight.FlightDescriptor.for_command(table_name.encode("utf-8")),
            reader.schema,
            options=self._options,
        )

        for i, batch in enumerate(reader, 1):
            writer.write_batch(batch)
        writer.done_writing()
        writer.close()

    def list_tables(self):
        """
        List all available tables

        Returns:
            List of table names
        """
        action = pyarrow.flight.Action("list_tables", b"")
        results = list(self._client.do_action(action, options=self._options))
        return [
            loads(result.body.to_pybytes()) for result in results
        ]

    def get_table_info(self, table_name):
        """
        Get schema information for a table

        Args:
            table_name: Name of the table

        Returns:
            Table schema information
        """
        action = pyarrow.flight.Action("table_info", table_name.encode("utf-8"))
        return next(
            map(
                loads,
                (
                    result.body.to_pybytes()
                    for result in self._client.do_action(action, options=self._options)
                ),
            )
        )

    def do_action(self, action_type, action_body="", options=None):
        try:
            action = pyarrow.flight.Action(
                action_type,
                dumps(action_body),
            )
            print("Running action", action_type)
            return tuple(
                map(
                    loads,
                    (
                        result.body.to_pybytes()
                        for result in self._client.do_action(action, options=options)
                    ),
                )
            )
        except pyarrow.lib.ArrowIOError as e:
            print("Error calling action:", e)

    def do_exchange_batches(self, command, reader):
        def do_writes(writer, reader):
            writer.begin(reader.schema)
            i = -1
            for i, batch in enumerate(reader, 1):
                writer.write_batch(batch)
            writer.done_writing()
            return i

        def do_reads(_reader, queue):
            i = -1
            for i, batch in enumerate(_reader, 1):
                queue.put(batch.data)
            queue.put(None)
            return i

        def do_writes_reads(command, reader, queue):
            descriptor = pyarrow.flight.FlightDescriptor.for_command(command)
            writer, _reader = self._client.do_exchange(descriptor, self._options)
            # `with writer` must happen inside a future
            # # so its context remains alive during enclosed writes and reads
            with writer:
                do_writes_fut = executor.submit(do_writes, writer, reader)
                do_reads_fut = executor.submit(do_reads, _reader, queue)
                (n_writes, n_reads) = (do_writes_fut.result(), do_reads_fut.result())
            return {"n_writes": n_writes, "n_reads": n_reads}

        def queue_to_rbr(schema, queue):
            def queue_to_gen(queue):
                while (value := queue.get()) is not None:
                    yield value

            return pyarrow.RecordBatchReader.from_batches(schema, queue_to_gen(queue))

        def get_output_schema(command, reader):
            (dct,) = self.do_action("query-exchange", command, options=self._options)
            assert dct["schema-in-condition"](reader.schema)
            output_schema = dct["calc-schema-out"](reader.schema)
            return output_schema

        queue = Queue()
        output_schema = get_output_schema(command, reader)
        fut = executor.submit(do_writes_reads, command, reader, queue)
        rbr = queue_to_rbr(output_schema, queue)
        return fut, rbr

    do_exchange = do_exchange_batches


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--tls-roots", default=None, help="Path to trusted TLS certificate(s)"
    )
    parser.add_argument("--host", default="localhost", help="Host endpoint")
    parser.add_argument("--port", default=5005, help="Host port")
    args = parser.parse_args()

    client = FlightClient(host=args.host, port=args.port, tls_roots=args.tls_roots)

    # Create a sample table
    data = pyarrow.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    client.upload_data("users", data)

    # List tables
    print("Available tables:", client.list_tables())

    # Get table info
    print("Table schema:", client.get_table_info("users"))

    # Execute a query
    result = client.execute_query("SELECT * FROM users WHERE id > 1")
    print("Query result:", result.to_pandas())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)

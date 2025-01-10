import argparse
import json

import pyarrow
import pyarrow.flight


class DuckDBFlightClient:
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
        token_pair = self._client.authenticate_basic_token(
            username.encode(), password.encode()
        )
        self._options = pyarrow.flight.FlightCallOptions(headers=[token_pair])

    def execute_query(self, query):
        """
        Execute SQL query and return results as Arrow table

        Args:
            query: SQL query string

        Returns:
            pyarrow.Table
        """
        # Get FlightInfo
        flight_info = self._client.get_flight_info(
            pyarrow.flight.FlightDescriptor.for_command(query.encode("utf-8")),
            options=self._options,
        )

        # Get the first endpoint
        endpoint = flight_info.endpoints[0]

        # Get the result
        reader = self._client.do_get(endpoint.ticket, options=self._options)
        return reader.read_all()

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

    def list_tables(self):
        """
        List all available tables

        Returns:
            List of table names
        """
        action = pyarrow.flight.Action("list_tables", b"")
        results = list(self._client.do_action(action, options=self._options))
        return [
            json.loads(result.body.to_pybytes().decode("utf-8")) for result in results
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
        results = list(self._client.do_action(action, options=self._options))
        return [
            json.loads(result.body.to_pybytes().decode("utf-8")) for result in results
        ]


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--tls-roots", default=None, help="Path to trusted TLS certificate(s)"
    )
    parser.add_argument("--host", default="localhost", help="Host endpoint")
    parser.add_argument("--port", default=5005, help="Host port")
    args = parser.parse_args()

    client = DuckDBFlightClient(
        host=args.host, port=args.port, tls_roots=args.tls_roots
    )

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

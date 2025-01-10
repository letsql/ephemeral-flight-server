import argparse
import base64
import json
import secrets

import duckdb
import pyarrow as pa
import pyarrow.flight


class BasicAuthServerMiddlewareFactory(pa.flight.ServerMiddlewareFactory):
    """
    Middleware that implements username-password authentication.

    Parameters
    ----------
    creds: Dict[str, str]
        A dictionary of username-password values to accept.
    """

    def __init__(self, creds):
        self.creds = creds
        # Map generated bearer tokens to users
        self.tokens = {}

    def start_call(self, info, headers):
        """Validate credentials at the start of every call."""
        # Search for the authentication header (case-insensitive)
        auth_header = None
        for header in headers:
            if header.lower() == "authorization":
                auth_header = headers[header][0]
                break

        if not auth_header:
            raise pa.flight.FlightUnauthenticatedError("No credentials supplied")

        # The header has the structure "AuthType TokenValue", e.g.
        # "Basic <encoded username+password>" or "Bearer <random token>".
        auth_type, _, value = auth_header.partition(" ")

        if auth_type == "Basic":
            # Initial "login". The user provided a username/password
            # combination encoded in the same way as HTTP Basic Auth.
            decoded = base64.b64decode(value).decode("utf-8")
            username, _, password = decoded.partition(":")
            if not password or password != self.creds.get(username):
                raise pa.flight.FlightUnauthenticatedError(
                    "Unknown user or invalid password"
                )
            # Generate a secret, random bearer token for future calls.
            token = secrets.token_urlsafe(32)
            self.tokens[token] = username
            return BasicAuthServerMiddleware(token)
        elif auth_type == "Bearer":
            # An actual call. Validate the bearer token.
            username = self.tokens.get(value)
            if username is None:
                raise pa.flight.FlightUnauthenticatedError("Invalid token")
            return BasicAuthServerMiddleware(value)

        raise pa.flight.FlightUnauthenticatedError("No credentials supplied")


class BasicAuthServerMiddleware(pa.flight.ServerMiddleware):
    """Middleware that implements username-password authentication."""

    def __init__(self, token):
        self.token = token

    def sending_headers(self):
        """Return the authentication token to the client."""
        return {"authorization": f"Bearer {self.token}"}


class NoOpAuthHandler(pa.flight.ServerAuthHandler):
    """
    A handler that implements username-password authentication.

    This is required only so that the server will respond to the internal
    Handshake RPC call, which the client calls when authenticate_basic_token
    is called. Otherwise, it should be a no-op as the actual authentication is
    implemented in middleware.
    """

    def authenticate(self, outgoing, incoming):
        pass

    def is_valid(self, token):
        return ""


class FlightServer(pyarrow.flight.FlightServerBase):
    def __init__(
        self,
        db_conn,
        location=None,
        tls_certificates=None,
        verify_client=False,
        root_certificates=None,
        auth_handler=None,
        middleware=None,
    ):
        super(FlightServer, self).__init__(
            location=location,
            auth_handler=auth_handler,
            tls_certificates=tls_certificates,
            verify_client=verify_client,
            root_certificates=root_certificates,
            middleware=middleware,
        )
        self._conn = db_conn
        self._location = location

    def _make_flight_info(self, query):
        """
        Create Flight info for a given SQL query

        Args:
            query: SQL query string
        """
        # Execute query to get schema and metadata
        result = self._conn.execute(query).arrow()
        descriptor = pyarrow.flight.FlightDescriptor.for_command(query)

        endpoints = [pyarrow.flight.FlightEndpoint(query, [self._location])]

        return pyarrow.flight.FlightInfo(
            result.schema, descriptor, endpoints, result.num_rows, result.nbytes
        )

    def get_flight_info(self, context, descriptor):
        """
        Get info about a specific query
        """
        query = descriptor.command.decode("utf-8")
        return self._make_flight_info(query)

    def do_get(self, context, ticket):
        """
        Execute SQL query and return results
        """
        query = ticket.ticket.decode("utf-8")
        try:
            # Execute query and convert to Arrow table
            result = self._conn.execute(query).arrow()
            return pyarrow.flight.RecordBatchStream(result)
        except Exception as e:
            raise pyarrow.flight.FlightServerError(f"Error executing query: {str(e)}")

    def do_put(self, context, descriptor, reader, writer):
        """
        Handle data upload - creates or updates a table
        """
        table_name = descriptor.command.decode("utf-8")
        data = reader.read_all()

        try:
            # Convert Arrow table to DuckDB table
            self._conn.register("temp_table", data)
            self._conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_table"
            )
            self._conn.execute("DROP VIEW temp_table")
        except Exception as e:
            raise pyarrow.flight.FlightServerError(f"Error creating table: {str(e)}")

    def list_actions(self, context):
        """
        List available custom actions
        """
        return [
            ("list_tables", "List all available tables"),
            ("table_info", "Get table schema and info"),
        ]

    def do_action(self, context, action):
        """
        Handle custom actions
        """
        if action.type == "list_tables":
            tables = self._conn.execute("SHOW TABLES").fetchall()
            for table in tables:
                yield pyarrow.flight.Result(json.dumps(table[0]).encode("utf-8"))

        elif action.type == "table_info":
            table_name = action.body.to_pybytes().decode("utf-8")
            schema = self._conn.execute(f"DESCRIBE {table_name}").fetchall()
            yield pyarrow.flight.Result(json.dumps(schema).encode("utf-8"))

        else:
            raise NotImplementedError


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tls", nargs=2, default=None, metavar=("CERTFILE", "KEYFILE"))
    args = parser.parse_args()
    tls_certificates = []

    scheme = "grpc+tls"
    host = "localhost"
    port = "5005"

    with open(args.tls[0], "rb") as cert_file:
        tls_cert_chain = cert_file.read()
    with open(args.tls[1], "rb") as key_file:
        tls_private_key = key_file.read()

    tls_certificates.append((tls_cert_chain, tls_private_key))

    location = "{}://{}:{}".format(scheme, host, port)

    server = FlightServer(
        duckdb.connect(":memory:"),
        location,
        tls_certificates=tls_certificates,
        auth_handler=NoOpAuthHandler(),
        middleware={
            "basic": BasicAuthServerMiddlewareFactory(
                {
                    "test": "password",
                }
            )
        },
    )
    print("Serving on", location)
    server.serve()


if __name__ == "__main__":
    main()

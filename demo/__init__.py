import threading
import time
from multiprocessing import Process, Queue

import letsql as ls

from demo.backend import Backend
from demo.server import BasicAuthServerMiddlewareFactory, FlightServer, NoOpAuthHandler

DEFAULT_AUTH_MIDDLEWARE = {
    "basic": BasicAuthServerMiddlewareFactory(
        {
            "test": "password",
        }
    )
}


class ServerWorker:
    def __init__(
        self,
        location=None,
        tls_certificates=None,
        verify_client=False,
        root_certificates=None,
        auth_handler=NoOpAuthHandler(),
        middleware=None,
        connection=None,
    ):
        if middleware is None:
            middleware = DEFAULT_AUTH_MIDDLEWARE

        self.started = False
        self.server = FlightServer(
            connection,
            location,
            tls_certificates=tls_certificates,
            verify_client=verify_client,
            root_certificates=root_certificates,
            auth_handler=auth_handler,
            middleware=middleware,
        )

    def _serve(self):
        if not self.started:
            print("Server is starting...")
            self.server.serve()
        self.started = True

    def _shutdown(self):
        print("Server is shutting down...")
        self.server.shutdown()

class BasicAuth:
    def __init__(self, username, password):
        self.username = username
        self.password = password


def to_basic_auth_middleware(basic_auth: BasicAuth) -> dict:
    assert basic_auth is not None

    return {
        "basic": BasicAuthServerMiddlewareFactory(
            {
                basic_auth.username: basic_auth.password,
            }
        )
    }


class EphemeralServer:
    def __init__(
        self,
        location=None,
        certificate_path=None,
        key_path=None,
        verify_client=False,
        root_certificates=None,
        auth: BasicAuth = None,
        connection = ls.duckdb.connect,
    ):
        self.location = location
        self.certificate_path = certificate_path
        self.key_path = key_path
        self.auth = auth

        tls_certificates = []

        with open(certificate_path, "rb") as cert_file:
            tls_cert_chain = cert_file.read()

        with open(key_path, "rb") as key_file:
            tls_private_key = key_file.read()

        tls_certificates.append((tls_cert_chain, tls_private_key))

        self.server = FlightServer(
            connection,
            location,
            tls_certificates=tls_certificates,
            verify_client=verify_client,
            root_certificates=root_certificates,
            auth_handler=NoOpAuthHandler(),
            middleware=to_basic_auth_middleware(auth),
        )


    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.server.__exit__(*args)

    def _shutdown(self):
        """Shut down after a delay."""
        print("Server is shutting down...")
        self.server.shutdown()
        time.sleep(2)

    def close(self):
        self._shutdown()

        # threading.Thread(target=self._shutdown).start()

def make_client(
    con: EphemeralServer,
) -> Backend:
    from urllib.parse import urlparse

    url = urlparse(con.location)

    instance = Backend()
    instance.do_connect(
        host=url.hostname,
        port=url.port,
        username=con.auth.username,
        password=con.auth.password,
        tls_roots=con.certificate_path,
    )
    return instance

def make_con(
    se: EphemeralServer,
) -> Backend:
    from urllib.parse import urlparse

    url = urlparse(se.location)

    instance = Backend()
    instance.do_connect(
        host=url.hostname,
        port=url.port,
        username=se.auth.username,
        password=se.auth.password,
        tls_roots=se.certificate_path,
    )
    return instance

__all__ = ["EphemeralServer", "make_client", "make_con", "BasicAuth"]

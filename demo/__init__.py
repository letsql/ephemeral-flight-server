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

    def handle(self, commands):
        while True:
            command = commands.get()
            print(command)
            if command == "serve":
                pass
                # self._serve()
            elif command == "shutdown":
                try:
                    threading.Thread(target=self._shutdown).start()
                except Exception as e:
                    print("this", e)
                break
            else:
                raise Exception(f"Unknown command, {command}")

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

        def server_process(cmd_q):
            worker = ServerWorker(
                location=location,
                tls_certificates=tls_certificates,
                verify_client=verify_client,
                root_certificates=root_certificates,
                auth_handler=NoOpAuthHandler(),
                middleware=to_basic_auth_middleware(auth),
                connection = connection
            )
            worker.handle(cmd_q)

        self.commands = Queue()
        self.p = Process(target=server_process, args=(self.commands,))
        self.p.start()

    def __enter__(self):
        print("Server started...")
        self.commands.put("serve")
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.commands.put("shutdown")
        # self.p.terminate()
        self.p.join()
        # res = self.commands.get()
        # self.p.join()


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

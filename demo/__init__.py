import time
from multiprocessing import Process, Queue

import duckdb

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
    ):
        if middleware is None:
            middleware = DEFAULT_AUTH_MIDDLEWARE

        self.started = False
        self.server = FlightServer(
            duckdb.connect(":memory:"),
            location,
            tls_certificates=tls_certificates,
            verify_client=verify_client,
            root_certificates=root_certificates,
            auth_handler=auth_handler,
            middleware=middleware,
        )

    def _serve(self):
        if not self.started:
            self.server.serve()
        self.started = True

    def _shutdown(self):
        """Shut down after a delay."""
        print("Server is shutting down...")
        time.sleep(2)
        self.server.shutdown()

    def handle(self, commands):
        while True:
            command = commands.get()
            if command == "serve":
                self._serve()
            elif command == "shutdown":
                self._shutdown()
                break


class Connection:
    def __init__(
        self,
        location=None,
        tls_certificates=None,
        verify_client=False,
        root_certificates=None,
        auth_handler=NoOpAuthHandler(),
        middleware=None,
    ):
        def server_process(cmd_q):
            worker = ServerWorker(
                location=location,
                tls_certificates=tls_certificates,
                verify_client=verify_client,
                root_certificates=root_certificates,
                auth_handler=auth_handler,
                middleware=middleware,
            )
            worker.handle(cmd_q)

        self.commands = Queue()
        self.p = Process(target=server_process, args=(self.commands,))
        self.p.start()

    def __enter__(self):
        print("Server started...")
        self.commands.put(("serve",))
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        print("Server shutting down...")
        self.commands.put(("shutdown",))
        self.p.terminate()


def make_client(
    host="localhost",
    port=8815,
    username="test",
    password="password",
    tls_roots=None,
) -> Backend:
    instance = Backend()
    instance.do_connect(host=host, port=port, username=username, password=password, tls_roots=tls_roots)
    return instance


__all__ = ["Connection", "make_client"]

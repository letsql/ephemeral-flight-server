import pathlib

root = pathlib.Path(__file__).resolve().parent
certificate_path = root / "tls" / "server.crt"
key_path = root / "tls" / "server.key"

scheme = "grpc+tls"
host = "localhost"
port = "5005"


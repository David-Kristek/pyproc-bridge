import time
import socket
from pyproc_bridge.node import IPCNode


class Supervisor(IPCNode):
    """Listens on a port, accepts one worker connection."""
    def __init__(self, host="127.0.0.1", port=5874):
        super().__init__()
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((host, port))
        self._server_sock.listen(1)
        self.host, self.port = host, port

    def wait_for_worker(self, timeout=None):
        """Accept a single worker connection and start the IPC threads.

        If ``timeout`` is given and no worker connects within that many seconds,
        raise ``TimeoutError``; the listening socket stays open so the caller
        may retry.
        """
        self._server_sock.settimeout(timeout)
        try:
            conn, addr = self._server_sock.accept()
        except socket.timeout as e:
            raise TimeoutError("no worker connected before timeout") from e
        finally:
            self._server_sock.settimeout(None)
        print(f"[Supervisor] Worker connected from {addr}")
        self._start_threads(conn)

    def close(self):
        super().close()
        self._server_sock.close()


class Worker(IPCNode):
    """Connects out to the supervisor."""
    def connect(self, host="127.0.0.1", port=5874, retries=25, backoff=0.2):
        last_err = None
        for _ in range(retries):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((host, port))
            except OSError as e:
                last_err = e
                sock.close()
                time.sleep(backoff)
                continue
            self._start_threads(sock)
            return
        raise ConnectionError(
            f"could not reach supervisor at {host}:{port}"
        ) from last_err

import json
import socket
import struct
import threading
import queue
import time


class IPCNode:
    """Base class for event-based IPC communication."""

    MAX_MSG_SIZE = 16 * 1024 * 1024  # 16 MB safety cap

    def __init__(self):
        self.handlers = {}
        self.sock = None
        self._running = False
        self._send_queue = queue.Queue()
        self._threads = []

    def on(self, event_name):
        """Decorator to register an event handler."""
        def decorator(func):
            self.handlers[event_name] = func
            return func
        return decorator

    def emit(self, event_name, data=None):
        """Non-blocking event emission."""
        if self._running:
            self._send_queue.put({"event": event_name, "data": data})

    def _recv_exact(self, n):
        data = bytearray()
        while len(data) < n:
            packet = self.sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)

    def _listen_loop(self):
        while self._running:
            try:
                header = self._recv_exact(4)
                if not header:
                    break
                msg_len = struct.unpack('!I', header)[0]

                if msg_len > self.MAX_MSG_SIZE:
                    print(f"[IPC] Rejecting oversized message: {msg_len} bytes")
                    break

                payload = self._recv_exact(msg_len)
                if not payload:
                    break

                msg = json.loads(payload.decode('utf-8'))
                event_name = msg.get("event")
                data = msg.get("data")

                if event_name in self.handlers:
                    try:
                        self.handlers[event_name](data)
                    except Exception as e:
                        print(f"[IPC] Handler error for '{event_name}': {e}")
                else:
                    print(f"[IPC] Warning: Unhandled event '{event_name}'")
            except Exception as e:
                if self._running:
                    print(f"[IPC] Listener error: {e}")
                break
        self._running = False

    def _write_loop(self):
        while self._running:
            try:
                msg = self._send_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            try:
                payload = json.dumps(msg).encode('utf-8')
                header = struct.pack('!I', len(payload))
                self.sock.sendall(header + payload)
            except Exception as e:
                if self._running:
                    print(f"[IPC] Writer error: {e}")
                self._running = False
                break

    def _start_threads(self, sock):
        self.sock = sock
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._running = True

        t1 = threading.Thread(target=self._listen_loop, daemon=True)
        t2 = threading.Thread(target=self._write_loop, daemon=True)
        t1.start()
        t2.start()
        self._threads.extend([t1, t2])

    def wait(self):
        """Block the caller until the connection closes (threads exit).

        Propagates KeyboardInterrupt so the caller can clean up.
        """
        while self._running:
            for t in self._threads:
                t.join(timeout=0.2)

    def flush(self, timeout=2.0):
        """Best-effort wait for queued outbound messages to be handed to the
        socket. Call this before ``close()`` when the last message emitted must
        not be dropped by the shutdown."""
        deadline = time.monotonic() + timeout
        while (
            self._running
            and not self._send_queue.empty()
            and time.monotonic() < deadline
        ):
            time.sleep(0.02)
        # The writer may have pulled the final item off the queue but not yet
        # finished sendall(); give it a brief moment to drain the socket buffer.
        time.sleep(0.1)

    def close(self):
        self._running = False
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
                self.sock.close()
            except Exception:
                pass
        current = threading.current_thread()
        for t in self._threads:
            if t is not current:
                t.join(timeout=1.0)

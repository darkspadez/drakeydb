import asyncio
import random


class Proxy:
    def __init__(self, host, port, remote_host, remote_port):
        self.host = host
        self.port = port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.stop_connections = []
        self.server = None
        self._handler_tasks = set()
        self._serve_task = None
        self._forwarding = asyncio.Event()
        self._forwarding.set()
        self._conn_gates = []
        self._response_override_lock = asyncio.Lock()
        self._next_response_override = None

    async def override_next_response(self, request_marker, replacement):
        """Replace the next simple-line response after a request containing `request_marker`."""
        if not request_marker:
            raise ValueError("request marker must not be empty")
        if not replacement.endswith(b"\r\n"):
            raise ValueError("replacement must be a complete RESP line")
        async with self._response_override_lock:
            if self._next_response_override is not None:
                raise RuntimeError("a response override is already pending")
            self._next_response_override = (request_marker, replacement)

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self._handler_tasks.add(task)
        try:
            await self._handle_impl(reader, writer)
        finally:
            self._handler_tasks.discard(task)

    async def _handle_impl(self, reader, writer):
        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                self.remote_host, self.remote_port
            )
        except (OSError, asyncio.CancelledError):
            writer.close()
            return

        gate = asyncio.Event()
        gate.set()
        self._conn_gates.append(gate)

        response_override = None
        request_tail = b""

        async def forward_requests(reader, writer):
            nonlocal request_tail, response_override
            while True:
                await self._forwarding.wait()
                await gate.wait()
                data = await reader.read(1024)
                if not data:
                    break

                async with self._response_override_lock:
                    pending = self._next_response_override
                    if pending is not None:
                        request_marker, replacement = pending
                        request_data = request_tail + data
                        if request_marker in request_data:
                            response_override = replacement
                            self._next_response_override = None
                            request_tail = b""
                        else:
                            tail_size = len(request_marker) - 1
                            request_tail = request_data[-tail_size:] if tail_size else b""

                writer.write(data)
                await writer.drain()
            writer.close()

        async def forward_responses(reader, writer):
            nonlocal response_override
            response_buffer = b""
            while True:
                await self._forwarding.wait()
                await gate.wait()
                data = await reader.read(1024)
                if not data:
                    break

                if response_override is not None:
                    response_buffer += data
                    line_end = response_buffer.find(b"\r\n")
                    if line_end == -1:
                        continue
                    data = response_override + response_buffer[line_end + 2 :]
                    response_override = None
                    response_buffer = b""

                writer.write(data)
                await writer.drain()
            writer.close()

        task1 = asyncio.ensure_future(forward_requests(reader, remote_writer))
        task2 = asyncio.ensure_future(forward_responses(remote_reader, writer))

        def cleanup():
            task1.cancel()
            task2.cancel()
            writer.close()
            remote_writer.close()
            if gate in self._conn_gates:
                self._conn_gates.remove(gate)

        self.stop_connections.append(cleanup)

        try:
            await asyncio.gather(task1, task2)
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        finally:
            cleanup()
            if cleanup in self.stop_connections:
                self.stop_connections.remove(cleanup)

    async def start(self):
        self.server = await asyncio.start_server(self.handle, self.host, self.port)

        if self.port == 0:
            _, port = self.server.sockets[0].getsockname()[:2]
            self.port = port

    async def start_serving(self):
        await self.start()
        self._serve_task = asyncio.create_task(self.serve(self.server))

    async def serve(self, server=None):
        server = server or self.server
        if server is None:
            return
        async with server:
            await server.serve_forever()

    async def __aenter__(self):
        await self.start_serving()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def pause(self):
        """
        Stop forwarding bytes without closing anything. Sockets stay ESTABLISHED on both
        sides while data stops moving, which is what either a lost network path or a peer
        that stopped reading looks like to the sender (as opposed to drop_connection, which
        is an immediate reset).
        """
        self._forwarding.clear()

    def pause_one(self, index=0):
        """
        Stall a single connection and leave the rest forwarding. With one replication
        flow per shard, this freezes exactly one flow.
        """
        gates = list(self._conn_gates)
        if not gates:
            return False
        gates[index % len(gates)].clear()
        return True

    def connection_count(self):
        """Number of currently proxied connections."""
        return len(self._conn_gates)

    def resume(self):
        """Undo pause()/pause_one(): resume forwarding on all connections."""
        self._forwarding.set()
        for gate in list(self._conn_gates):
            gate.set()

    def drop_connection(self):
        """
        Randomly drop one connection
        """
        if self.stop_connections:
            cb = random.choice(self.stop_connections)
            self.stop_connections.remove(cb)
            cb()

    async def close(self, task=None):
        # A Proxy is reused across close()/start_serving() cycles (see test_partial_sync), so
        # leave it forwarding: a paused proxy would silently stall every future connection.
        self.resume()

        if task is None:
            task = self._serve_task
        if task is self._serve_task:
            self._serve_task = None

        if self.server is not None:
            self.server.close()
            self.server = None

        for cb in self.stop_connections:
            cb()
        self.stop_connections = []

        # Yield so that accepted-but-not-yet-started handler tasks begin
        # executing and register themselves in _handler_tasks.
        await asyncio.sleep(0)

        # Cancel all handler tasks, including ones that haven't registered
        # their cleanup callbacks yet (race between accept and close).
        # Loop until no tasks remain so late-starting handlers are caught.
        while self._handler_tasks:
            tasks = list(self._handler_tasks)
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        if task is not None:
            try:
                await task
            except asyncio.exceptions.CancelledError:
                pass

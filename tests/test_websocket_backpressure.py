import asyncio
from types import SimpleNamespace

from app import main


class BlockingWebSocket:
    def __init__(self):
        self.sent = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def send_text(self, payload):
        self.sent.append(payload)
        if payload == "first":
            self.started.set()
            await self.release.wait()


def test_websocket_broadcast_keeps_only_latest_pending_snapshot():
    async def scenario():
        websocket = BlockingWebSocket()
        main.active_clients.add(websocket)

        main.broadcast_snapshot(SimpleNamespace(model_dump_json=lambda: "first"))
        sender = main.ws_send_tasks[websocket]
        await websocket.started.wait()

        main.broadcast_snapshot(SimpleNamespace(model_dump_json=lambda: "second"))
        main.broadcast_snapshot(SimpleNamespace(model_dump_json=lambda: "third"))
        websocket.release.set()
        await sender

        assert websocket.sent == ["first", "third"]
        assert websocket not in main.ws_send_tasks
        assert websocket in main.active_clients

        main.active_clients.discard(websocket)
        main.pending_ws_payloads.pop(websocket, None)
        main.ws_send_tasks.pop(websocket, None)

    asyncio.run(scenario())

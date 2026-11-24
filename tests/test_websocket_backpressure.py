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


def test_websocket_initial_snapshot_does_not_race_live_broadcast(monkeypatch):
    async def scenario():
        class InitialSendWebSocket:
            def __init__(self):
                self.sent = []
                self.initial_started = asyncio.Event()
                self.release_initial = asyncio.Event()

            async def send_text(self, payload):
                self.sent.append(payload)
                if payload == "initial":
                    self.initial_started.set()
                    await self.release_initial.wait()

        websocket = InitialSendWebSocket()
        initial = SimpleNamespace(model_dump_json=lambda: "initial")
        latest = SimpleNamespace(model_dump_json=lambda: "latest")
        monkeypatch.setattr(main.orchestrator, "latest_snapshot", initial)

        registration = asyncio.create_task(main._register_websocket(websocket))
        await websocket.initial_started.wait()

        # A live update during the initial send must not start a competing
        # sender because the client is not registered until that send ends.
        main.broadcast_snapshot(latest)
        assert websocket not in main.active_clients
        assert websocket.sent == ["initial"]

        monkeypatch.setattr(main.orchestrator, "latest_snapshot", latest)
        websocket.release_initial.set()
        await registration
        sender = main.ws_send_tasks.get(websocket)
        if sender is not None:
            await sender

        assert websocket.sent == ["initial", "latest"]
        assert websocket in main.active_clients
        assert websocket not in main.ws_send_tasks

        main.active_clients.discard(websocket)
        main.pending_ws_payloads.pop(websocket, None)
        main.ws_send_tasks.pop(websocket, None)

    asyncio.run(scenario())

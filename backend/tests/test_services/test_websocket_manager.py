"""Tests for WebSocketManager."""

import pytest
from backend.utils.websocket_manager import WebSocketManager


@pytest.mark.asyncio
async def test_connect_and_disconnect():
    """Test connecting and disconnecting WebSocket."""
    manager = WebSocketManager()
    job_id = "test-job"
    
    # Mock WebSocket
    class MockWebSocket:
        async def accept(self):
            pass
    
    ws = MockWebSocket()
    
    await manager.connect(ws, job_id)
    assert job_id in manager.active_connections
    assert ws in manager.active_connections[job_id]
    
    manager.disconnect(job_id, ws)
    assert job_id not in manager.active_connections or len(manager.active_connections[job_id]) == 0


@pytest.mark.asyncio
async def test_send_event_no_connections():
    """Test sending event when no connections exist."""
    manager = WebSocketManager()
    
    # Should not raise error
    await manager.send_event("nonexistent-job", {"type": "test", "data": {}})


@pytest.mark.asyncio
async def test_sequence_numbers():
    """Test sequence number generation."""
    manager = WebSocketManager()
    job_id = "test-job"
    
    # Mock WebSocket
    class MockWebSocket:
        async def accept(self):
            pass
        async def send_text(self, text):
            pass
    
    ws = MockWebSocket()
    await manager.connect(ws, job_id)
    
    # Send multiple events
    for i in range(5):
        await manager.send_event(job_id, {"type": "test", "data": {}})
    
    # Sequence numbers should increment
    assert manager.sequence_numbers[job_id] == 5


@pytest.mark.asyncio
async def test_send_status_tick():
    """Test sending status tick event."""
    manager = WebSocketManager()
    job_id = "test-job"
    
    # Mock WebSocket
    class MockWebSocket:
        messages = []
        async def accept(self):
            pass
        async def send_text(self, text):
            self.messages.append(text)
    
    ws = MockWebSocket()
    await manager.connect(ws, job_id)
    
    await manager.send_status_tick(
        job_id=job_id,
        phase=1,
        step="1.1",
        step_name="Test Step",
        scope={"entity": "Customer"},
        message="Processing Customer entity"
    )
    
    assert len(ws.messages) > 0
    import json
    event = json.loads(ws.messages[-1])
    assert event["type"] == "status_tick"
    assert event["data"]["message"] == "Processing Customer entity"




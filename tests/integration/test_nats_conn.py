import pytest
from nats.aio.client import Client as NatsClient

from rf_shared.nats_client import NatsConnection


@pytest.mark.asyncio
async def test_simple_connection_and_close():
    """
    Tests a basic connection without JetStream, and ensures clean closure.
    """
    conn = NatsConnection(
        nats_url="nats://password@127.0.0.1:4222",
    )
    try:
        # ARRANGE: We have a NatsConnection instance.

        # ACT: Connect without requesting JetStream.
        await conn.connect(jetstream=False, max_reconnect_attempts=1, connect_timeout=1)

        # ASSERT: Check the state after connecting.
        assert conn.nc is not None, "NATS client object should be created."
        assert isinstance(conn.nc, NatsClient), "nc should be a NATS Client instance."
        assert conn.nc.is_connected, "Client should report as connected."
        assert conn.js is None, "JetStream context should NOT be created."

        # ACT: Close the connection.
        await conn.close()

        # ASSERT: Check the state after closing.
        assert conn.nc.is_closed, "Client should report as closed."

    finally:
        # Ensure cleanup happens even if assertions fail.
        if conn.nc and not conn.nc.is_closed:
            await conn.close()

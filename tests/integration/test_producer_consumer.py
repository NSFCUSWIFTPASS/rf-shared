import pytest
import pytest_asyncio
import datetime
import uuid
from pathlib import Path
import nats
from typing import Tuple

from rf_shared.nats_client import NatsProducer, NatsConsumer
from rf_shared.models import MetadataRecord, Envelope

NATS_URL = "nats://password@localhost:4222"


@pytest.fixture
def mock_metadata() -> MetadataRecord:
    return MetadataRecord(
        hostname="hcro-rpi-001",
        timestamp=datetime.datetime(
            2024, 4, 2, 23, 14, 50, 9919, tzinfo=datetime.timezone.utc
        ),
        source_path=Path("/test/dummy_file_path.sc16"),
        serial="3227508",
        organization="hcro_db_test",
        gcs="43.1534N77.6044W",
        frequency=915000000,
        interval=10,
        length=1.0,
        gain=35,
        sampling_rate=26000000,
        bit_depth=16,
        group="snzfqW",
        checksum="abc",
    )


@pytest_asyncio.fixture(scope="function")
async def nats_stream() -> Tuple[nats.js.client.JetStreamContext, str, str]:
    """
    A pytest fixture that sets up and tears down a temporary JetStream stream for a test.
    """
    stream_name = f"test-stream-{uuid.uuid4()}"
    subject = f"test.subject.{uuid.uuid4()}"
    setup_nc = None
    js = None
    try:
        setup_nc = await nats.connect(NATS_URL)
        js = setup_nc.jetstream()
        print(f"\n[SETUP] Creating stream '{stream_name}' for subject '{subject}'")
        await js.add_stream(name=stream_name, subjects=[subject])
        yield js, stream_name, subject
    finally:
        if setup_nc and js:
            print(f"\n[TEARDOWN] Deleting stream '{stream_name}'")
            await js.delete_stream(name=stream_name)
        if setup_nc:
            await setup_nc.close()


# --- The Main Integration Test ---


@pytest.mark.asyncio
async def test_producer_sends_consumer_receives(nats_stream, mock_metadata):
    """
    Full integration test:
    1. Producer connects and publishes a serialized MetadataRecord.
    2. Consumer connects, subscribes, and fetches the message.
    3. Verifies the received record is identical to the sent one.
    """
    js, test_stream_name, test_subject = nats_stream
    test_durable_name = "test-durable-consumer"

    # --- 1. Instantiate the Producer and Consumer ---
    producer = NatsProducer(
        connect_options={"servers": NATS_URL},
        subject=test_subject,
        mode="jetstream",  # Test JetStream mode
    )
    consumer = NatsConsumer(
        connect_options={"servers": NATS_URL},
    )

    try:
        # --- 2. Act Phase: Connect, Publish, Fetch ---
        await producer.connect()
        await consumer.connect()

        fetch_single_msg = await consumer.jetstream_subscribe(
            test_stream_name,
            test_subject,
            test_durable_name,
        )

        # --- Application layer (the test) is now responsible for serialization ---
        envelope_to_send = Envelope.from_metadata(mock_metadata)
        payload_to_send = envelope_to_send.model_dump_json().encode()

        # Use the generic publish method. It will use the default_subject.
        await producer.publish(payload_to_send)

        received_msg = await fetch_single_msg(timeout=1)

        # --- 3. Assert Phase: Verify the Data (No changes needed here) ---
        assert received_msg is not None, "Consumer did not receive any message."

        await received_msg.ack()
        received_envelope = Envelope.model_validate_json(received_msg.data)

        assert received_envelope.payload == mock_metadata.model_dump(mode="json")
        assert received_envelope.source_path == mock_metadata.source_path
        assert isinstance(received_envelope.message_id, uuid.UUID)

    finally:
        # --- 4. Teardown Phase (Connections) ---
        if producer.nc:
            await producer.close()
        if consumer.nc:
            await consumer.close()


@pytest.mark.asyncio
async def test_consumer_timeout(nats_stream):
    js, test_stream_name, test_subject = nats_stream
    test_durable_name = "test-durable-consumer"

    # --- Be explicit about needing JetStream ---
    consumer = NatsConsumer(
        connect_options={"servers": NATS_URL},
    )

    try:
        await consumer.connect()
        fetch_single_msg = await consumer.jetstream_subscribe(
            test_stream_name,
            test_subject,
            test_durable_name,
        )

        received_msg = await fetch_single_msg(timeout=1)
        assert received_msg is None, "Consumer should timeout and receive None."

    finally:
        if consumer.nc:
            await consumer.close()

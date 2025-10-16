import pytest
import pytest_asyncio
import json
import datetime
import uuid
from pathlib import Path
import nats
from typing import Tuple

from rf_shared.nats_client import NatsProducer, NatsConsumer
from rf_shared.models import MetadataRecord, Envelope
from rf_shared.interfaces import ILogger

NATS_URL = "nats://password@localhost:4222"


# --- Test Fixtures and Mocks ---


class MockLogger(ILogger):
    def _log(self, level, msg, *args, **kwargs):
        print(f"[{level}] {msg}")

    def info(self, msg, *args, **kwargs):
        self._log("INFO", msg)

    def debug(self, msg, *args, **kwargs):
        self._log("DEBUG", msg)

    def warning(self, msg, *args, **kwargs):
        self._log("WARNING", msg)

    def error(self, msg, *args, **kwargs):
        self._log("ERROR", msg)

    def critical(self, msg, *args, **kwargs):
        self._log("CRITICAL", msg)


@pytest.fixture
def mock_logger() -> ILogger:
    return MockLogger()


@pytest.fixture
def mock_metadata() -> MetadataRecord:
    return MetadataRecord(
        hostname="hcro-rpi-001",
        timestamp=datetime.datetime(
            2024, 4, 2, 23, 14, 50, 9919, tzinfo=datetime.timezone.utc
        ),
        source_sc16_path=Path("/test/dummy_file_path.sc16"),
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

    Yields:
        A tuple containing:
        - js (JetStream): The JetStream context object.
        - stream_name (str): The unique name of the created stream.
        - subject (str): The unique subject bound to the stream.
    """
    stream_name = f"test-stream-{uuid.uuid4()}"
    subject = f"test.subject.{uuid.uuid4()}"

    # Setup client for managing the stream
    setup_nc = None
    try:
        setup_nc = await nats.connect(NATS_URL)
        js = setup_nc.jetstream()

        # --- SETUP ---
        print(f"\n[SETUP] Creating stream '{stream_name}' for subject '{subject}'")
        await js.add_stream(name=stream_name, subjects=[subject])

        yield js, stream_name, subject

    finally:
        # --- TEARDOWN ---
        if setup_nc and js:
            print(f"\n[TEARDOWN] Deleting stream '{stream_name}'")
            await js.delete_stream(name=stream_name)
        if setup_nc:
            await setup_nc.close()


# --- The Main Integration Test ---


@pytest.mark.asyncio
async def test_producer_sends_consumer_receives(
    nats_stream, mock_logger, mock_metadata
):
    """
    Full integration test:
    2. Producer connects and publishes a MetadataRecord.
    3. Consumer connects, subscribes, and fetches the record.
    4. Verifies the received record is identical to the sent one.
    """
    js, test_stream_name, test_subject = nats_stream
    test_durable_name = "test-durable-consumer"

    # --- 1. Instantiate the Producer and Consumer ---
    producer = NatsProducer(
        mock_logger,
        test_subject,
        connect_options={"servers": NATS_URL},
    )
    consumer = NatsConsumer(
        mock_logger,
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

        mock_logger.info("Publishing mock metadata record...")
        await producer.publish_metadata(mock_metadata)

        mock_logger.info("Fetching message from consumer...")
        received_msg = await fetch_single_msg(timeout=1)

        # --- 3. Assert Phase: Verify the Data ---
        assert received_msg is not None, "Consumer did not receive any message."

        # Acknowledge the message so it's not redelivered
        await received_msg.ack()

        data_dict = json.loads(received_msg.data)
        received_envelope = Envelope.from_dict(data_dict)

        mock_logger.info("Verifying sent and received records are identical...")

        assert (
            received_envelope.payload == mock_metadata.to_dict()
        ), "Payload data does not match!"

        assert (
            received_envelope.source_path == mock_metadata.source_sc16_path
        ), "Source path does not match!"

        assert isinstance(
            received_envelope.message_id, uuid.UUID
        ), "message_id is not a valid UUID object!"

    finally:
        # --- 4. Teardown Phase (Connections) ---
        mock_logger.info("Closing producer and consumer connections...")
        if producer.nc:
            await producer.close()
        if consumer.nc:
            await consumer.close()


@pytest.mark.asyncio
async def test_consumer_timeout(nats_stream, mock_logger):
    js, test_stream_name, test_subject = nats_stream
    test_durable_name = "test-durable-consumer"

    consumer = NatsConsumer(
        mock_logger,
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

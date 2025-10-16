import pytest
import json
import datetime
import uuid
from pathlib import Path
import nats

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


# --- The Main Integration Test ---


@pytest.mark.asyncio
async def test_producer_sends_consumer_receives(mock_logger, mock_metadata):
    """
    Full integration test:
    1. Sets up a temporary JetStream stream.
    2. Producer connects and publishes a MetadataRecord.
    3. Consumer connects, subscribes, and fetches the record.
    4. Verifies the received record is identical to the sent one.
    5. Cleans up the stream.
    """
    # Use unique names for the test to ensure isolation
    test_stream_name = f"test-stream-{uuid.uuid4()}"
    test_subject = f"test.subject.{uuid.uuid4()}"
    test_durable_name = "test-durable-consumer"

    # --- 1. Setup Phase: Create the JetStream Stream ---
    setup_nc = await nats.connect(NATS_URL)
    js = setup_nc.jetstream()
    try:
        await js.add_stream(name=test_stream_name, subjects=[test_subject])
        mock_logger.info(f"Test setup: Created stream '{test_stream_name}'")

        # --- Instantiate the Producer and Consumer ---
        producer = NatsProducer(
            mock_logger,
            test_subject,
            connect_options={"servers": NATS_URL},
        )
        consumer = NatsConsumer(
            mock_logger,
            test_stream_name,
            test_subject,
            test_durable_name,
            connect_options={"servers": NATS_URL},
        )

        try:
            # --- 2. Act Phase: Connect, Publish, Fetch ---
            await producer.connect()
            await consumer.connect()

            mock_logger.info("Publishing mock metadata record...")
            await producer.publish_metadata(mock_metadata)

            mock_logger.info("Fetching message from consumer...")
            received_msg = await consumer.fetch_single_msg(timeout=5)

            # --- 3. Assert Phase: Verify the Data ---
            assert received_msg is not None, "Consumer did not receive any message."

            # Acknowledge the message so it's not redelivered
            await consumer.ack(received_msg)

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

    finally:
        # --- 5. Teardown Phase (Stream) ---
        mock_logger.info(f"Test teardown: Deleting stream '{test_stream_name}'")
        await js.delete_stream(name=test_stream_name)
        await setup_nc.close()

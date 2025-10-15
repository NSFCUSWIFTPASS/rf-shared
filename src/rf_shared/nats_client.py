import asyncio
import json
import nats

from rf_shared.interfaces import ILogger
from rf_shared.models import MetadataRecord


class NatsConnection:
    def __init__(self, nats_url: str):
        self.nats_url = nats_url

        self.nc = None
        self.js = None

    async def connect(self, jetstream: bool = False, **kwargs):
        """Establishes a connection to the NATS server and sets up JetStream."""
        self.nc = await nats.connect(self.nats_url, **kwargs)

        if jetstream:
            self.js = self.nc.jetstream()

    async def close(self):
        """Closes the NATS connection."""
        if self.nc:
            await self.nc.close()


class NatsConsumer:
    def __init__(
        self,
        logger: ILogger,
        nats_url: str,
        stream_name: str,
        subject: str,
        durable_name: str,
    ):
        self.logger = logger
        self.nats_url = nats_url
        self.stream_name = stream_name
        self.subject = subject
        self.durable_name = durable_name
        self.sub = None

        self._conn = NatsConnection(nats_url)

    async def connect(self):
        try:
            await self._conn.connect(jetstream=True)
            self.logger.info(f"Connected to NATS at {self.nats_url}")

            self.sub = await self._conn.js.pull_subscribe(
                stream=self.stream_name,
                subject=self.subject,
                durable=self.durable_name,
            )
            self.logger.info(
                f"Subscribed to stream '{self.stream_name}' with subject '{self.subject}'",
            )

        except Exception as e:
            self.logger.error(f"NATS connection failed: {e}")
            raise

    async def close(self):
        await self._conn.close()
        self.logger.info("NATS consumer connection closed.")

    async def fetch_single_msg(self, timeout=3):
        """Try to fetch a message. returns None on timeout."""
        if not self._conn.js or not self.sub:
            raise ConnectionError("NATS is not connected. Call connect() first.")

        try:
            msgs = await self.sub.fetch(batch=1, timeout=timeout)
            return msgs[0] if msgs else None

        except asyncio.TimeoutError:
            return None

        except Exception as e:
            self.logger.error(f"NATS fetch error: {e}")
            return None

    async def ack(self, msg):
        try:
            await msg.ack()

        except Exception as e:
            self.logger.error(f"Failed to ack message: {e}")


class NatsProducer:
    def __init__(
        self,
        logger: ILogger,
        nats_url: str,
        subject: str,
    ):
        self.logger = logger
        self.nats_url = nats_url
        self.subject = subject

        self._conn = NatsConnection(nats_url)

    async def connect(self):
        try:
            await self._conn.connect(jetstream=True)
            self.logger.info(f"Connected to NATS at {self.nats_url}")

        except Exception as e:
            self.logger.error(f"Unexpected error connecting to NATS: {e}")
            raise

    async def close(self):
        await self._conn.close()
        self.logger.info("NATS producer connection closed.")

    async def publish_raw(self, subject: str, payload: bytes):
        if not self._conn.js:
            raise ConnectionError("NATS is not connected. Call connect() first.")

        await self._conn.js.publish(subject, payload)

    async def publish_metadata(self, record: MetadataRecord):
        payload = json.dumps(record.to_dict()).encode()
        await self.publish_raw(self.subject, payload)

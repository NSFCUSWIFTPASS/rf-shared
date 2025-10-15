import asyncio
import json
import nats

from rf_shared.interfaces import ILogger
from rf_shared.models import MetadataRecord


class NatsConsumer:
    def __init__(
        self,
        logger: ILogger,
        stream_name: str,
        subject: str,
        durable_name: str,
        connect_options: dict,
    ):
        self.logger = logger
        self.stream_name = stream_name
        self.subject = subject
        self.durable_name = durable_name
        self.sub = None

        self.nc = None
        self.js = None

        self._connect_options = connect_options

    async def connect(self):
        try:
            self.nc = await nats.connect(**self._connect_options)
            self.js = self.nc.jetstream()

            self.logger.info(
                f"Connected to NATS at {self._connect_options.get('servers')}"
            )

            self.sub = await self.js.pull_subscribe(
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
        if self.nc:
            await self.nc.close()
        self.logger.info("NATS consumer connection closed.")

    async def fetch_single_msg(self, timeout=3):
        """Try to fetch a message. returns None on timeout."""
        if not self.js or not self.sub:
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
        subject: str,
        connect_options: dict,
    ):
        self.logger = logger
        self.subject = subject

        self.nc = None
        self.js = None

        self._connect_options = connect_options

    async def connect(self):
        try:
            self.nc = await nats.connect(**self._connect_options)
            self.js = self.nc.jetstream()

            self.logger.info(
                f"Connected to NATS at {self._connect_options.get('servers')}"
            )

        except Exception as e:
            self.logger.error(f"Unexpected error connecting to NATS: {e}")
            raise

    async def close(self):
        if self.nc:
            await self.nc.close()
        self.logger.info("NATS consumer connection closed.")

    async def publish_raw(self, subject: str, payload: bytes):
        if not self.js:
            raise ConnectionError("NATS is not connected. Call connect() first.")

        await self.js.publish(subject, payload)

    async def publish_metadata(self, record: MetadataRecord):
        payload = json.dumps(record.to_dict()).encode()
        await self.publish_raw(self.subject, payload)

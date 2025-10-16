import json
import nats
from typing import Awaitable, Callable

from rf_shared.interfaces import ILogger
from rf_shared.models import MetadataRecord, Envelope, ReceivedMessage


class NatsConsumer:
    def __init__(
        self,
        logger: ILogger,
        connect_options: dict,
    ):
        self.logger = logger
        self._subscriptions = []

        self.nc = None
        self.js = None

        self._connect_options = connect_options

    async def connect(self):
        """
        Connects to NATs.
        """
        try:
            self.nc = await nats.connect(**self._connect_options)

            self.logger.info(
                f"Connected to NATS at {self._connect_options.get('servers')}"
            )

        except Exception as e:
            self.logger.error(f"NATS connection failed: {e}")
            raise

    async def close(self):
        if self.nc:
            await self.nc.close()
        self.logger.info("NATS consumer connection closed.")

    async def jetstream_subscribe(
        self, stream_name: str, subject: str, durable_name: str
    ) -> Callable[..., Awaitable[ReceivedMessage | None]]:
        """
        Subscribes to a JetStream stream and returns a fetch function.
        """
        if not self.nc:
            raise ConnectionError("NATS is not connected.")

        self.js = self.nc.jetstream()

        sub = await self.js.pull_subscribe(
            stream=stream_name, subject=subject, durable=durable_name
        )
        self._subscriptions.append(sub)
        self.logger.info(f"Subscribed to JS stream '{stream_name}'")

        async def fetch_one(timeout=3) -> ReceivedMessage | None:
            try:
                msgs = await sub.fetch(1, timeout=timeout)
                if not msgs:
                    return None

                nats_msg = msgs[0]

                return ReceivedMessage(data=nats_msg.data, ack=nats_msg.ack)

            except nats.errors.TimeoutError:
                return None

        return fetch_one

    async def core_subscribe(
        self, subject: str, callback: Callable[[ReceivedMessage], Awaitable[None]]
    ):
        """
        Subscribes to a core NATS subject with the provided async callback.
        """
        if not self.nc:
            raise ConnectionError("NATS is not connected.")

        async def message_handler_adapter(nats_msg):
            app_message = ReceivedMessage(data=nats_msg.data)

            try:
                await callback(app_message)
            except Exception as e:
                self.logger.error(
                    f"Error in callback for subject '{subject}': {e}",
                    exc_info=True,
                )

        sub = await self.nc.subscribe(subject, cb=message_handler_adapter)
        self._subscriptions.append(sub)
        self.logger.info(f"Subscribed to core subject '{subject}' with a callback.")


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
        envelope = Envelope.from_metadata(record)
        envelope_dict = envelope.to_dict()
        payload = json.dumps(envelope_dict).encode()

        await self.publish_raw(self.subject, payload)

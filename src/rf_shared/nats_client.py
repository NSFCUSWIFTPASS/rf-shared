import nats
import asyncio
import logging
from typing import Awaitable, Callable

from rf_shared.models import ReceivedMessage

logger = logging.getLogger(__name__)


async def _default_error_cb(e):
    logger.error(f"NATS Error: {e}")


async def _default_disconnected_cb():
    logger.warning("NATS disconnected... attempting to reconnect.")


async def _default_reconnected_cb():
    logger.info("NATS reconnected successfully.")


async def _default_closed_cb():
    logger.error("NATS connection closed permanently.")


class NatsConsumer:
    def __init__(
        self,
        connect_options: dict,
    ):
        self._subscriptions = []

        self.nc = None
        self.js = None

        self._connect_options = connect_options

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self):
        """
        Connects to NATs.
        """
        options = {
            "max_reconnect_attempts": -1,
            "reconnect_time_wait": 2,
            "error_cb": _default_error_cb,
            "disconnected_cb": _default_disconnected_cb,
            "reconnected_cb": _default_reconnected_cb,
            "closed_cb": _default_closed_cb,
        }

        options.update(self._connect_options)

        try:
            self.nc = await nats.connect(**options)
            logger.info(f"Connected to NATS at {self._connect_options.get('servers')}")

        except Exception as e:
            logger.error(f"NATS initial connection failed: {e}")
            raise

    async def close(self):
        if self.nc:
            await self.nc.close()
        logger.info("NATS consumer connection closed.")

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
        logger.info(f"Subscribed to JS stream '{stream_name}'")

        async def fetch_one(timeout=3) -> ReceivedMessage | None:
            try:
                msgs = await sub.fetch(1, timeout=timeout)

                if not msgs:
                    return None

                nats_msg = msgs[0]
                return ReceivedMessage(data=nats_msg.data, ack=nats_msg.ack)

            except nats.errors.TimeoutError:
                return None
            except (nats.errors.ConnectionClosedError, nats.errors.NoRespondersError):
                logger.debug("Connection unstable during fetch, retrying...")
                await asyncio.sleep(1)
                return None
            except Exception as e:
                logger.error(f"Unexpected error fetching message: {e}")
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
                logger.error(
                    f"Error in callback for subject '{subject}': {e}",
                    exc_info=True,
                )

        sub = await self.nc.subscribe(subject, cb=message_handler_adapter)
        self._subscriptions.append(sub)
        logger.info(f"Subscribed to core subject '{subject}' with a callback.")


class NatsProducer:
    def __init__(
        self,
        subject: str,
        connect_options: dict,
        mode: str = "jetstream",
    ):
        self.subject = subject

        if mode not in ["jetstream", "core"]:
            raise ValueError("mode must be 'jetstream' or 'core'")
        self.mode = mode

        self.nc = None
        self.js = None
        self._connect_options = connect_options

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self):
        """Connects to NATS and conditionally initializes JetStream."""
        try:
            self.nc = await nats.connect(**self._connect_options)
            logger.info(f"Connected to NATS at {self._connect_options.get('servers')}")

            if self.mode == "jetstream":
                self.js = self.nc.jetstream()
                logger.info("NATS Producer configured for JetStream.")
            else:
                logger.info("NATS Producer configured for Core NATS.")
        except Exception as e:
            logger.error(f"Unexpected error connecting to NATS: {e}")
            raise

    async def close(self):
        if self.nc and not self.nc.is_closed:
            await self.nc.close()
        logger.info("NATS producer connection closed.")

    async def publish(self, payload: bytes, subject: str | None = None):
        """
        Publishes a raw payload.
        Uses the provided subject, or falls back to the producer's default_subject.
        """
        if not self.nc or self.nc.is_closed:
            raise ConnectionError("NATS is not connected. Call connect() first.")

        target_subject = subject or self.subject

        logger.debug(f"Publishing {len(payload)} bytes to subject '{target_subject}'")

        if self.mode == "jetstream":
            if not self.js:
                raise ConnectionError(
                    "JetStream is not initialized. Check producer mode."
                )
            await self.js.publish(target_subject, payload)
        else:  # 'core' mode
            await self.nc.publish(target_subject, payload)

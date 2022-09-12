import logging
import os
import json
from re import A
from typing import Any, Callable, Dict

import aio_pika
import asyncio
import aiormq

from sognojq.my_retry import retry
from sognojq.context import DoOnce

logger = logging.getLogger()
logger.setLevel("ERROR")
logging.basicConfig(
    format="%(asctime)s: [%(filename)s:%(lineno)d] %(levelname)s: %(message)s",
)


class AmqpConnector:
    """Porvides functionality to asynchronously configure an AMQP message broker.

    Parameters
    ----------
    amqp_host: str, default = "127.0.0.1"
        Host address of amqp broker.
    amqp_port: int, default = 5672
        Port for http AMQP communication.
    amqp_username: str, default = "guest"
        Name of the user used for authentication of this connection.
    amqp_password: str, default = "guest"
        Password for the amqp broker correspondig to the chosen user.
    """

    def __init__(
        self,
        amqp_host: str = "127.0.0.1",
        amqp_port: int = 5672,
        amqp_username: str = "guest",
        amqp_password: str = "guest",
    ):
        self.amqp_host = amqp_host
        self.amqp_port = amqp_port
        self.amqp_username = amqp_username
        self.amqp_password = amqp_password
        self._amqp_connection = None
        self._amqp_channel = None
        # locks have to be created in loop
        self.connection_config_lock = None
        self.channel_config_lock = None

    def _get_amqp_auth(self):
        return {
            "login": self.amqp_username,
            "password": self.amqp_password,
        }

    def connection_ok(self):
        return self._amqp_connection is not None and not self._amqp_connection.is_closed

    @retry(
        exceptions=(
            ConnectionError,
            ConnectionRefusedError,
            ConnectionAbortedError,
            ConnectionResetError,
            aiormq.exceptions.IncompatibleProtocolError,
            asyncio.exceptions.IncompleteReadError,
        ),
        tries=int(os.getenv("MAX_RETRIES_INTERNAL", -1)),
        delay=1,
        jitter=1,
        max_delay=60,
        logger=logger,
    )
    async def get_connection(self):
        """Creates connection if necessary or uses the available one and returns it for subsequent use."""
        if not self.connection_config_lock:
            self.connection_config_lock = DoOnce()
        await self.connection_config_lock.wait_if_locked()
        if self.connection_ok():
            return self._amqp_connection
        async with self.connection_config_lock:
            amqp_connection = await aio_pika.connect_robust(
                host=self.amqp_host,
                port=self.amqp_port,
                **self._get_amqp_auth(),
            )
            if amqp_connection is not None:
                self._amqp_connection = amqp_connection
                logger.info(f"... {str(self)} connected.")
            else:
                raise ConnectionError(f"Could not connect {str(self)}")
            return self._amqp_connection

    def channel_ok(self):
        """Checks if a channel exists and is open."""
        return self._amqp_channel is not None and not self._amqp_channel.is_closed

    @retry(
        exceptions=(
            ConnectionError,
            ConnectionRefusedError,
            ConnectionAbortedError,
            ConnectionResetError,
            aiormq.exceptions.IncompatibleProtocolError,
            asyncio.exceptions.IncompleteReadError,
        ),
        tries=int(os.getenv("MAX_RETRIES_INTERNAL", -1)),
        delay=1,
        jitter=1,
        max_delay=60,
        logger=logger,
    )
    async def get_channel(self):
        """Creates channel if necessary or uses the available one and returns it for subsequent use."""
        logger.debug("trying to create channel")
        if not self.channel_config_lock:
            self.channel_config_lock = DoOnce()
        await self.channel_config_lock.wait_if_locked()
        if self.channel_ok():
            return self._amqp_channel
        connection = await self.get_connection()
        async with self.channel_config_lock:
            logger.info("Trying to configure channel ...")
            amqp_channel = await connection.channel()
            # do not overwrite channel if it can not connect because it already is connected
            if amqp_channel is not None:
                self._amqp_channel = amqp_channel
                await self._amqp_channel.set_qos(prefetch_count=1)
            else:
                raise ConnectionError(
                    f"could not open a channel to broker for {str(self)}."
                )
        logger.debug(self._amqp_channel)
        return self._amqp_channel

    @retry(
        exceptions=(
            ConnectionError,
            ConnectionRefusedError,
            ConnectionAbortedError,
            ConnectionResetError,
            aiormq.exceptions.IncompatibleProtocolError,
            asyncio.exceptions.IncompleteReadError,
        ),
        tries=int(os.getenv("MAX_RETRIES_INTERNAL", -1)),
        delay=1,
        jitter=1,
        max_delay=60,
        logger=logger,
    )
    async def _create_exchange(
        self,
        exchange_name: str = "",
        exchange_type: str = "topic",
        durable: bool = True,
    ) -> aio_pika.Exchange:
        channel = await self.get_channel()
        logger.info(f"channel: {channel}")
        logger.info(f"Creating exchange {exchange_name} ...")
        amqp_exchange = await channel.declare_exchange(
            name=exchange_name, type=exchange_type, durable=durable
        )

        if not amqp_exchange:
            raise ConnectionError(
                f"Could not create AMQP exchange for some unknwon reason for {str(self)}."
            )
        logger.info("... exchange configured")
        return amqp_exchange

    async def _create_queue(
        self,
        queue_name: str = "default",
        durable: bool = True,
        # exchange_name: str = None,
        # routing_key="#",
    ):
        channel = await self.get_channel()
        queue = await channel.declare_queue(
            name=queue_name,
            durable=durable,
            arguments={
                "x-queue-type": "quorum",
                "x-max-in-memory-length": 300,  # TODO this should be adjustable
            },
        )
        # if exchange_name is not None:
        #     await queue.bind(
        #         exchange=exchange_name,
        #         routing_key=routing_key,
        #     )
        logger.info("... queue configured")
        return queue

    async def _bind(self, queue_name, exchange_name, routing_key="#"):
        channel = await self.get_channel()
        queue, exchange = await asyncio.gather(
            channel.get_queue(queue_name), channel.get_exchange(exchange_name)
        )
        await queue.bind(exchange, routing_key)
        return self

    async def _unbind(self, queue_name, exchange_name, routing_key="#"):
        channel = await self.get_channel()
        get_queue_task = asyncio.create_task(channel.get_queue(queue_name))
        get_channel_task = asyncio.create_task(channel.get_exchange(exchange_name))
        (await get_queue_task).unbind(await get_channel_task, routing_key)
        return self


class AmqpListener(AmqpConnector):
    """Porvides functionality to asynchronously listen to an AMQP message broker.

    Parameters
    ----------
    amqp_host: str, default = "127.0.0.1"
        Host address of amqp broker.
    amqp_port: int, default = 5672
        Port for http AMQP communication.
    amqp_username: str, default = "guest"
        Name of the user used for authentication of this connection.
    amqp_password: str, default = "guest"
        Password for the amqp broker correspondig to the chosen user.
    amqp_queue_name: str, default = "default"
        Name of the queue thorugh which will be listened.
    """

    def __init__(
        self,
        amqp_host: str = "127.0.0.1",
        amqp_port: int = 5672,
        amqp_username: str = "AMQP_USERNAME",
        amqp_password: str = "AMQP_PASSWORD",
        amqp_queue_name: str = "default",
    ):
        super().__init__(
            amqp_host=amqp_host,
            amqp_port=amqp_port,
            amqp_username=amqp_username,
            amqp_password=amqp_password,
        )
        self.amqp_queue_name = amqp_queue_name
        self.amqp_queue = None

        self.queue_config_lock = None

    @retry(
        exceptions=(
            ConnectionError,
            ConnectionRefusedError,
            ConnectionAbortedError,
            ConnectionResetError,
            aiormq.exceptions.IncompatibleProtocolError,
            asyncio.exceptions.IncompleteReadError,
        ),
        tries=int(os.getenv("MAX_RETRIES_INTERNAL", -1)),
        delay=1,
        jitter=1,
        max_delay=60,
        logger=logger,
    )
    async def get_queue(self):
        """Creates queue if necessary or uses the available one and returns it for subsequent use."""
        if not self.queue_config_lock:
            self.queue_config_lock = DoOnce()
        await self.queue_config_lock.wait_if_locked()
        if self.amqp_queue:
            return self.amqp_queue
        async with self.queue_config_lock:
            self.amqp_queue = await self._create_queue(queue_name=self.amqp_queue_name)
        return self.amqp_queue

    async def bind_to_exchange(self, exchange_name="", routing_key="#"):
        """Bind the used queue to an existing exchange.

        Parameters
        ----------
        exchange_name: str, default = ""
            Name of the exchange to be bound to.
        routing_key: str, default = "#"
            Key used for filtering messages while routing from exchange to router (see AMQP Topic Exchange)
        """
        queue = await self.get_queue()
        await queue.bind(exchange_name, routing_key)
        return self

    async def get_message(self) -> aio_pika.IncomingMessage:
        """Repeatedly request a message from the queue until one is recieved.
        Be careful to manually acknowledge the message or use message.process() as async context.

        Returns
        -------
        aio_pika.IncomingMessage
            Message object recieved from the queue

        """
        queue = await self.get_queue()
        msg = None
        while msg is None:
            print("msg was None, trying again")
            msg = await queue.get(fail=False, timeout=1)
            await asyncio.sleep(5)
        return msg

    def add_message_processor(self, callback: Callable[[str], Any]):
        """Add a callback function that is executed every time a message is recieved."""
        if asyncio.iscoroutine(callable):

            async def on_message_recieved(msg):
                async with msg.process():
                    return await callback(msg.body.decode("utf-8"))

        else:

            async def on_message_recieved(msg):
                async with msg.process():
                    return callback(msg.body.decode("utf-8"))

        self._on_message_recieved = on_message_recieved

    async def process_messages(self):
        queue = await self.get_queue()
        await queue.consume(self._on_message_recieved)
        while True:
            await asyncio.sleep(1)


class AmqpPublisher(AmqpConnector):
    """Provides functionality to asynchronously send messages to a AMQP message broker."""

    def __init__(
        self,
        amqp_host: str = "127.0.0.1",
        amqp_port: int = 5672,
        amqp_username: str = "AMQP_USERNAME",
        amqp_password: str = "AMQP_PASSWORD",
        topic_prefix: str = "",
        amqp_exchange_name: str = "",
    ):
        super().__init__(
            amqp_host=amqp_host,
            amqp_port=amqp_port,
            amqp_username=amqp_username,
            amqp_password=amqp_password,
        )
        self.topic_prefix = topic_prefix
        self.amqp_exchange_name = amqp_exchange_name
        self.amqp_exchange = None
        self.exchange_config_lock = None
        self.amqp_topic_prefix = topic_prefix

    @retry(
        exceptions=(
            ConnectionError,
            ConnectionRefusedError,
            ConnectionAbortedError,
            ConnectionResetError,
            aiormq.exceptions.IncompatibleProtocolError,
            asyncio.exceptions.IncompleteReadError,
        ),
        tries=int(os.getenv("MAX_RETRIES_INTERNAL", -1)),
        delay=1,
        jitter=1,
        max_delay=60,
        logger=logger,
    )
    async def get_exchange(self):
        if not self.exchange_config_lock:
            self.exchange_config_lock = DoOnce()
        await self.exchange_config_lock.wait_if_locked()
        if self.amqp_exchange:
            return self.amqp_exchange
        async with self.exchange_config_lock:
            self.amqp_exchange = await self._create_exchange(self.amqp_exchange_name)
        return self.amqp_exchange

    @retry(
        exceptions=(
            aio_pika.exceptions.ConnectionClosed,
            aio_pika.exceptions.ChannelClosed,
            aiormq.exceptions.ChannelInvalidStateError,
            aiormq.exceptions.IncompatibleProtocolError,
            asyncio.exceptions.IncompleteReadError,
            RuntimeError,
        ),
        tries=int(os.getenv("MAX_RETRIES_INTERNAL", -1)),
        delay=1,
        jitter=1,
        max_delay=60,
        logger=logger,
    )
    async def publish(self, key: str, data) -> bool:
        """Publishes the message on the exchange set in __init__().

        Parameters
        ----------
        key: str
            Routing key under which the message is published. Chain topics with '.'
        data:
            Json-serializable object that makes up the body of the message.

        Returns
        -------
        bool:
            True if submission was successful.

        Raises
        ------
        ConnectionError:
            If AMQP broker can not be reached.
        """
        exchange = await self.get_exchange()
        if self.amqp_topic_prefix.strip():  # not whitespace only
            key = f"{self.amqp_topic_prefix.strip()}.{key}"
        logger.debug(f"publishing: topic:{key} | body:{json.dumps(data, default=str)}")
        # try:
        #     key, data = await self.sanitize_message(key, data)
        # except (ValueError, KeyError, TypeError) as err:
        #     logger.error(f"Could not sanitize data {key}:{data}", stack_info=True)
        #     return
        logger.debug(self._amqp_connection)
        logger.debug(self._amqp_channel)
        logger.debug(self.amqp_exchange)
        return await exchange.publish(
            aio_pika.Message(body=json.dumps(data, default=str).encode()),
            routing_key=str(key),
        )

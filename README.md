# sogno-job-queue
Provides utility classes to be used in a frontend and backend in tandem with a rabbitmq-broker.


## Publisher
The publisher provides everything needed to publish messages on an exchange. Routing logic is not included here bu in the [Listenener](#listener)

### Example
```
from amqp import AmqpPublisher
import asyncio

publisher = AmqpPublisher(
    amqp_username="user", 
    amqp_password="testpw", 
    amqp_exchange_name="test_exchange",
    amqp_host="127.0.0.1",
    amqp_port=5672
)

async def send():
    for i in range(1000):
        await publisher.publish("job.test", i)
        await asyncio.sleep(1)

asyncio.run(send())
```

This sends 1000 integers to the exchange "test_exchange" under the topic "job.test" 

## Listener
This class provides both an active request for a message as well as an passive consumer usablitiy. It is assumed that the listener also establishes the routing logic if not already configured from a different place.

### Example (passive)
```
from amqp import AmqpListener
import asyncio

listener = AmqpListener(
    amqp_username="user",
    amqp_password="testpw",
    amqp_queue_name="test_queue",
    amqp_host="127.0.0.1",
    amqp_port=5672,
)


async def listen():
    await listener.bind_to_exchange(exchange_name="test_exchange")
    listener.add_message_processor(print)
    await listener.process_messages()

asyncio.run(listen())
```

Again we create an AmqpListener object with everything needed for configuration of the broker. In the `listen()` coroutine the queue is bound to the already existing exchange.
Then we add a callback function (in this case `print()`) that takes the message body as string as an argument. It will be called every time a message is recieved and can be either sync or async. `.process_messages()` starts an infinite loop of consuming messages, if the callback function returns without error the message will be ackknowledeged automatically.

### Excample (aktive)
```
from amqp import AmqpListener
import asyncio

listener = AmqpListener(
    amqp_username="user",
    amqp_password="testpw",
    amqp_queue_name="test_queue",
    amqp_port=5672,
)


async def listen():
    await listener.bind_to_exchange(exchange_name="test_exchange")
    for i in range(1000):
        msg = await listener.get_message()
        async with msg.process():
            print(msg)
        await asyncio.sleep(1)


asyncio.run(listen())
```

Instead of passively listening we can actively request a message from a queue. If there is no message this is repeated until a message is available.
Returned is an `aio_pika.IncommingMessage` object that can be processed.
The message has to be acknowledged manually, done here using the `.process()` context manager. 


## Known Issues
None of these are considered pressing issue, if they hinder development for somebody feel free to open an issue or contact us.

- Asynchronous but not threadsafe.
- Exchange has to be declared (from elsewhere) before queue is bound.
- Listener fails once and succeds in retry in testsetup (Error message suggests a problem with setup on windows)
- Passive listening only gives access to the message body (this was chosen to avoid the need to dive into the implementation of the amqp client).
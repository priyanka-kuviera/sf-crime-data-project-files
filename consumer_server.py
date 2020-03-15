from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer
import asyncio



BROKER_URL = 'localhost:9092'
TOPIC_NAME = 'Crime_data_topic'


async def consume(topic_name):
    c = Consumer({'bootstrap.servers': BROKER_URL, 'group.id': 0})
    c.subscribe([topic_name])

    while True: 
        messages = c.consume(5, timeout=1.0)
        print(f"consumed {len(messages)} messages")
        for message in messages:
            print(f'consumed message: {message.value()}')

    await asyncio.sleep(0.01)


def main():
    client = AdminClient({'bootstrap.servers': BROKER_URL})
    try:
        asyncio.run(consume(TOPIC_NAME))
    except KeyboardInterrupt as e: 
        print('shutting down')



if __name__ == '__main__':
    main()




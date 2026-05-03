from aiokafka import AIOKafkaProducer
import json

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

producer: AIOKafkaProducer | None = None


async def start_kafka():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    await producer.start()


async def stop_kafka():
    global producer
    if producer:
        await producer.stop()


async def send_message(topic: str, message: dict):
    await producer.send_and_wait(
        topic,
        json.dumps(message).encode("utf-8"),
    )
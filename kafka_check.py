from confluent_kafka import Producer, Consumer, KafkaError
import time
import sys

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TEST_TOPIC = 'test-topic'

def verify_kafka():
    # 1. Initialize Producer
    p_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(p_conf)

    # 2. Force topic creation/metadata fetch
    print(f"Connecting to {KAFKA_BOOTSTRAP_SERVERS}...")

    # 3. Produce with Callback
    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    producer.produce(TEST_TOPIC, key='sync', value='Connectivity established', callback=delivery_report)
    producer.flush()

    # 4. Initialize Consumer with unique group to ensure 'earliest' works
    c_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f'verify-group-{int(time.time())}',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(c_conf)
    consumer.subscribe([TEST_TOPIC])

    # 5. Polling Loop (Max 5 retries)
    print("Consumer: Polling for message...")
    retries = 5
    while retries > 0:
        msg = consumer.poll(timeout=3.0)
        if msg is None:
            print(f"Retrying... ({retries} attempts left)")
            retries -= 1
            continue

        if msg.error():
            print(f"Consumer Error: {msg.error()}")
            break

        print(f"SUCCESS: Received '{msg.value().decode('utf-8')}' from Kafka")
        break

    consumer.close()

if __name__ == "__main__":
    verify_kafka()

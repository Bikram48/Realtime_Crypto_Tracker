from confluent_kafka import Consumer, KafkaException

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "crypto_consumer_group",
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(["crypto_prices"])

print("Waiting for messages...")
try:
    while True:
        msg = consumer.poll(1.0)  # Wait for message
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        print(f"Received message: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()


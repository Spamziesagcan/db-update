import json
from kafka import KafkaConsumer

# Configure the Kafka consumer
consumer = KafkaConsumer(
    'your_server_name.public.your_table_name',  # This should match the topic name created by Debezium
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Starting to listen for messages...")

for message in consumer:
    # The message value from Debezium is a JSON object with the change data
    payload = message.value['payload']
    print(f"Received message: {payload}")

    # You can now process the change event
    if payload['op'] == 'c':
        print(f"A new record was created: {payload['after']}")
    elif payload['op'] == 'u':
        print(f"A record was updated. Before: {payload['before']}, After: {payload['after']}")
    elif payload['op'] == 'd':
        print(f"A record was deleted: {payload['before']}")
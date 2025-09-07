import json
import logging
import os
import sys
import requests
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURATION ---
KAFKA_TOPIC = 'db_server.realtime_orders.orders' # Debezium topic name: server.database.table
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
API_WEBSOCKET_URL = "http://localhost:8000/internal/broadcast" # A new internal endpoint

def format_debezium_event(event: dict) -> dict:
    """Transforms a Debezium event into the format our frontend expects."""
    payload = event.get('payload', {})
    if not payload:
        return None

    op = payload.get('op')
    before = payload.get('before')
    after = payload.get('after')
    
    # Map Debezium op codes to our action names
    action_map = {'c': 'INSERT', 'u': 'UPDATE', 'd': 'DELETE'}
    action = action_map.get(op)
    if not action:
        return None

    # Determine old and new data based on the action
    old_data = before if action in ['UPDATE', 'DELETE'] else None
    new_data = after if action in ['INSERT', 'UPDATE'] else None
    
    # The order_id is present in 'after' for inserts/updates and 'before' for deletes
    order_id = (after or before).get('id')

    # Construct the message in the exact format the frontend needs
    return {
        'event_type': 'order_change',
        'action': action,
        'order_id': order_id,
        'timestamp': datetime.fromtimestamp(payload['ts_ms'] / 1000).isoformat(),
        'old_data': old_data,
        'new_data': new_data,
    }

def start_consumer():
    """Starts the Kafka consumer and listens for messages."""
    logging.info(f"Connecting to Kafka brokers at {KAFKA_BROKERS} on topic {KAFKA_TOPIC}...")
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        group_id='order_change_notifier',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    logging.info("Kafka consumer connected successfully. Waiting for messages...")
    for message in consumer:
        try:
            formatted_message = format_debezium_event(message.value)
            if formatted_message:
                logging.info(f"Broadcasting formatted message for order_id: {formatted_message['order_id']}")
                # Instead of calling manager.broadcast directly, we send an HTTP request
                # This decouples the consumer from the FastAPI app's internal state
                response = requests.post(API_WEBSOCKET_URL, json=formatted_message)
                response.raise_for_status() # Raise an exception for bad status codes

        except Exception as e:
            logging.error(f"Error processing message or broadcasting: {e}")

if __name__ == "__main__":
    start_consumer()
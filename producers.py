import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves
import csv
from kafka import KafkaProducer
import json

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convert Python objects to JSON
)

# Open and read your dataset (CSV file)
with open('netflix.csv', mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)  # Use DictReader to get a dictionary for each row
    for row in reader:
        # Send each row as a JSON object
        print(f"Sending: {row}")  # Print the row to verify it's valid JSON
        producer.send('project1', value=row)  # Specify your Kafka topic here

# Flush the producer to ensure all data is sent
producer.flush()


from kafka import KafkaConsumer
import json
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", 'localhost:9092,localhost:9093,localhost:9094').split(",") 
TOPIC_NAME = 'my_favorite_topic'
GROUP_ID   = 'my_favorite_group'
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", 'admin')
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", 'p@ssw0rd')

def main():
    consumer = KafkaConsumer(
      TOPIC_NAME,
      group_id=GROUP_ID,
      auto_offset_reset='earliest',
      # security_protocol="SASL_SSL",
      # sasl_mechanism="SCRAM-SHA-512",
      # sasl_plain_username=SASL_USERNAME,
      # sasl_plain_password=SASL_PASSWORD,
      bootstrap_servers=BOOTSTRAP_SERVERS,
      value_deserializer = json.loads)
      # api_version=(2, 8, 1))

    for msg in consumer:
      print(f"Message Received: ({msg})")
    
    consumer.close()

if __name__ == '__main__':
    main()
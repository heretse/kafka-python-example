from kafka import KafkaProducer
import json
import os
import reactivex as rx
import uuid

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", 'localhost:9092,localhost:9093,localhost:9094').split(",") 
TOPIC_NAME = 'my_favorite_topic'
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", 'admin')
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", 'p@ssw0rd')

def main():
    producer = KafkaProducer(
    #   security_protocol="SASL_SSL",
    #   sasl_mechanism="SCRAM-SHA-512",
    #   sasl_plain_username=SASL_USERNAME,
    #   sasl_plain_password=SASL_PASSWORD,
      bootstrap_servers=BOOTSTRAP_SERVERS,
      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    #   api_version=(2, 8, 1))

    def work(x):
        producer.send(TOPIC_NAME, {'id': str(uuid.uuid4()), 'message': 'Hello kafka'})
        producer.flush()

    def done():
        producer.close()

    rx.range(1, 10).subscribe(
        on_next = work,
        on_error = lambda e: print("Error : {0}".format(e)),
        on_completed = done,
    )

if __name__ == '__main__':
    main()
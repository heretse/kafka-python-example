
from kafka import KafkaConsumer
import json
import multiprocessing
import os
import random
import reactivex as rx
import time
from reactivex.scheduler import ThreadPoolScheduler
from reactivex import operators as ops
from threading import current_thread


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", 'localhost:9092,localhost:9093,localhost:9094').split(",") 
TOPIC_NAME = 'my_favorite_topic'
GROUP_ID   = 'my_favorite_group'
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", 'admin')
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", 'p@ssw0rd')

def main():
    # calculate number of CPUs, then create a ThreadPoolScheduler with that number of threads
    optimal_thread_count = multiprocessing.cpu_count()
    pool_scheduler = ThreadPoolScheduler(optimal_thread_count)

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

    def intense_calculation(value):
      # sleep for a random short duration between 0.5 to 2.0 seconds to simulate a long-running calculation
      time.sleep(random.randint(5, 20) * 0.1)
      return value

    for msg in consumer:
      rx.just(msg).pipe(
        ops.map(lambda s: intense_calculation(s)), ops.subscribe_on(pool_scheduler)
      ).subscribe(
        on_next = lambda i: print("PROCESS : {0}, Message Received : {1}".format(current_thread().name, i)),
        on_error = lambda e: print("Error : {0}".format(e)),
        on_completed = lambda: print("Job Done!"),
      )

    consumer.close()

if __name__ == '__main__':
    main()
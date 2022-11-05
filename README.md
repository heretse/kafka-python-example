# Kafka Python with RxPy Example

## Description

This example is to demonstrate how to produce message to Kafka and consume message from Kafka with [RxPY](https://github.com/ReactiveX/RxPY)

## Usage 
### Running Kafka on local
```
docker-compose up -d 
```
### Install required packages
```
pip install -r requirements.txt
```
### Run the code `src/producer.py` to produce message to Kafka with the topic `my_favorite_topic`. 
```
python src/producer.py
```
### Run the code `src/consumer.py` to consume the topic `my_favorite_topic` from Kafka with Rx multithreading. 
```
python src/consumer.py
```
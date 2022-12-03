from json import dumps

from kafka import KafkaProducer

from constants import TOPIC

producer = KafkaProducer(bootstrap_servers=["localhost:9092"], value_serializer=lambda x: dumps(x).encode("utf-8"))
for n in range(100, 200):
    print("Sending", n)
    my_data = {"num": n}
    future = producer.send(TOPIC, value=my_data)
    future.get()

from json import loads
from time import sleep

from kafka import KafkaConsumer

from constants import TOPIC

consumer = KafkaConsumer(
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)


# def on_assign_callback(
#     consumer,
#     partitions,
# ):
#     """Modify assigned partitions to read up to MAX_HISTORY old messages"""
#     for partition in partitions:
#         min_offset, max_offset = consumer.get_watermark_offsets(partition)
#         # desired_offset = max_offset - MAX_HISTORY
#         # if desired_offset <= min_offset:
#         #     desired_offset = OFFSET_BEGINNING
#         print("min_offset, max_offset", min_offset, max_offset)
#         partition.offset = -2
#     consumer.assign(partitions)

consumer.subscribe([TOPIC])
consumer.poll()
consumer.seek_to_beginning()

i = 1

for message in consumer:
    message = message.value
    print(f"{i} GOT:", message)
    i += 1

from pykafka.simpleconsumer import OffsetType
import logging
from pykafka import KafkaClient


logging.getLogger("pykafka.broker").setLevel('ERROR')

consumer_kafka_client = KafkaClient(hosts="localhost:9092")

topic = consumer_kafka_client.topics['police.dept.srvc.calls']

consumer = topic.get_balanced_consumer(
    consumer_group = b'kafka_test',
    auto_commit_enable = False,
    auto_offset_reset = OffsetType.EARLIEST,
    zookeeper_connect = 'localhost:2181'
)

for msg in consumer:
    if msg is not None:
        print("Message", msg.offset, msg.value)
from confluent_kafka import Producer

from config.config import settings


class TimeProducer():
    CONF = {'bootstrap.servers': settings.get('KAFKA_BOOTSTRAP_SERVER')}
    TOPIC = settings.get('OUTPUT_TOPIC')

    def __init__(self):
        self.producer = Producer(**self.CONF)

    def produce(self, msg):
        self.producer.produce(self.TOPIC, msg)
        self.producer.flush()
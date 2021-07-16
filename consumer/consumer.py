
from datetime import datetime, timezone
import time 

from rfc3339 import rfc3339
from confluent_kafka import Consumer, KafkaException

from config.config import settings
from logger.logger import logger
from producer import TimeProducer





class TimeConsumer:
    TOPICS = [settings.get('INPUT_TOPIC')]
    CONFIG = {
        'bootstrap.servers': settings.get('KAFKA_BOOTSTRAP_SERVER'),
        'group.id': settings.get('KAFKA_GROUP_ID'),
        'session.timeout.ms': settings.get('KAFKA_SESSION_TIMEOUT_MS'),
        'auto.offset.reset': settings.get('KAFKA_AUTO_OFFSET_RESET')
    }
    
    def __init__(self):
        self.consumer = Consumer(self.CONFIG, logger=logger)
        self.producer = TimeProducer()
    
    def process_msg(self):
        try:
            time_in_millis = float(self.msg.value())
            dt = datetime.fromtimestamp(time_in_millis, tz=timezone.utc)
            rfc = dt.isoformat("T","milliseconds")
            self.producer.produce(rfc)
        except Exception as exp:
            print(exp)
        
    def consume(self):
        self.consumer.subscribe(self.TOPICS)
        try:
            while True:
                self.msg = self.consumer.poll(timeout=1.0)
                if self.msg is None:
                    continue
                if self.msg.error():
                    raise KafkaException(self.msg.error())
                else:
                    self.process_msg()
                   
        except KeyboardInterrupt:
            logger.info('Aborted by user')


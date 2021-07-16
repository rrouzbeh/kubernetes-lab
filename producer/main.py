import datetime
from time import sleep

import delorean

from producer import TimeProducer

def current_milli_time() -> int:
    return delorean.Delorean(datetime.datetime.utcnow(), timezone="UTC").epoch


def main():
    producer = TimeProducer()
    while True:
        sleep(0.1)
        print(current_milli_time())
        producer.produce(current_milli_time())
        

if __name__ == "__main__":
    main()
from typing import Callable
from confluent_kafka import Producer

from utils.delivery_report import delivery_report


class AirKafkaProducer(object):

    def __init__(
            self,
            topic: str,
            network: str,
            conf: dict | None = None,
            callback: Callable | None = delivery_report
    ):
        self.topic = topic
        self.callback = callback

        if conf is not None:
            self.conf = conf
            self.conf['bootstrap.servers'] = f'{network}:9092'
        else:
            self.conf = {
                'bootstrap.servers': f'{network}:9092',  # hostname in docker network
                'linger.ms': 50,
                'batch.size': 32768,
                'compression.type': 'lz4'
            }

        self._producer = Producer(self.conf)

    def produce(self, value: str) -> None:
        """Produce message to kafka

        Args:
            value (dict[str, Any]): json data
        """
        self._producer.produce(
            topic=self.topic,
            value=value,
            callback=self.callback
        )

    def poll(self, timeout: float = 0) -> None:
        self._producer.poll(timeout)

    def flush(self) -> None:
        self._producer.flush()

"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "schema.registry.url": "http://localhost:8081"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)


        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})

        #if self.topic_name in Producer.existing_topics:
        if self.check_topic_exists(client, self.topic_name):
            print(f"the topic {self.topic_name} has already been created")
            logger.info(f"the topic {self.topic_name} has already been created - skipping")
            return
        
        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas
                )
            ]
        )
        
        for topic, future in futures.items():
            try:
                future.result()
                print(f"topic created {self.topic_name}")
                logger.info(f"topic created successfully, {self.topic_name}")
            except Exception as e:
                print(f"failed to create topic {self.topic_name}: {e}")
                logger.error(f"failed to create topic {self.topic_name}")


    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        #self.producer.close()
        logger.info("closing the producer")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
    
    def check_topic_exists(self, client, topic_name):
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics()
        topics = topic_metadata.topics
        return topic_name in topics

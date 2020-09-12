import redis
from kafka import KafkaConsumer
from kafka import KafkaProducer
import globals
import json

# Redis initialize
redis_obj = redis.StrictRedis(
    host=globals.REDIS_HOSTNAME,
    port=globals.REDIS_PORT,
    password=globals.REDIS_PASSWORD,
    ssl=True
)

# Kafka initialize
consumer_obj = KafkaConsumer(
    globals.RECEIVE_TOPIC,
    bootstrap_servers=[globals.KAFKA_HOSTNAME + ':' + globals.KAFKA_PORT],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism='PLAIN',
    sasl_plain_username=globals.KAFKA_USERNAME,
    sasl_plain_password=globals.KAFKA_PASSWORD
)

producer_obj = KafkaProducer(
    bootstrap_servers=[globals.KAFKA_HOSTNAME + ':' + globals.KAFKA_PORT],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism='PLAIN',
    sasl_plain_username=globals.KAFKA_USERNAME,
    sasl_plain_password=globals.KAFKA_PASSWORD
)


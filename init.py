from kafka import KafkaConsumer
import globals
import json
import logging
logging.basicConfig(
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.ERROR
    )
from logstash_async.handler import AsynchronousLogstashHandler
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

# Get you a test logger
error_logger = logging.getLogger('python-logstash-logger')
# Set it to whatever level you want - default will be info
error_logger.setLevel(logging.DEBUG)
# Create a handler for it
async_handler = AsynchronousLogstashHandler(globals.LOGSTASH_HOSTNAME,int(globals.LOGSTASH_PORT), database_path=None)
# Add the handler to the logger
error_logger.addHandler(async_handler)

def ERR_LOGGER(msg):
    msg = globals.RECEIVE_TOPIC +" "+ msg
    error_logger.error(msg)
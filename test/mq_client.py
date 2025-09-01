
from nioflux_mq.client.client import NioFluxMQClient

client = NioFluxMQClient(host='127.0.0.1', port=34291)

print(client.register_topic('topic_0'))

print(client.register_consumer('consumer_0'))

print(client.produce(b'message_0', 'topic_0', ttl=1))

print(client.consume('consumer_0', 'topic_0'))

print(client.advance('consumer_0', 'topic_0'))

print(client.retreat('consumer_0', 'topic_0'))

print(client.topics)

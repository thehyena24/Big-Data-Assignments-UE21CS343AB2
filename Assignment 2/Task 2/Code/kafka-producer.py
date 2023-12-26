from kafka import KafkaProducer
from json import dumps
import sys

# topic1 -> Comment
# topic2 -> Like
# topic3 -> Share

comment_topic = sys.argv[1]
like_topic = sys.argv[2]
share_topic = sys.argv[3]

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: x.encode('ascii'))

# producer = KafkaProducer()

for line in sys.stdin:
    record = line.strip()
    message = record

    if record[0] == 'c':
        producer.send(comment_topic, value = message)
        producer.send(share_topic, value = message)


    elif record[0] == 'l':
        producer.send(like_topic, value = message)
        producer.send(share_topic, value = message)


    elif record[0] == 's':
        producer.send(share_topic, value = message)


    elif record[0] == 'E':
        message = "EOF"

        producer.send(comment_topic, value = message)
        producer.send(like_topic, value = message)
        producer.send(share_topic, value = message)

producer.flush()
producer.close()
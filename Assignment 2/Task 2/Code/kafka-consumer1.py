from kafka import KafkaConsumer
from json import loads, dumps
import sys

# topic1 -> Comment
# topic2 -> Like
# topic3 -> Share

comment_topic = sys.argv[1]
like_topic = sys.argv[2]
share_topic = sys.argv[3]

result = {}

consumer = KafkaConsumer(
    comment_topic,
     bootstrap_servers=['localhost:9092'],
     value_deserializer=lambda m: m.decode('ascii'))

# consumer = KafkaConsumer(comment_topic)

for message in consumer:
    value = message.value
    topic = message.topic

    comment = ""

    if value == "EOF":
        break

    value = value.split(" ", 4)

    user_commenting = value[1]
    user_who_posted = value[2]
    post_id = value[3]

    if len(value) == 5:
        comment = value[4][1:-1]

    if user_who_posted not in result.keys():
        result[user_who_posted] = []
        result[user_who_posted].append(comment)
        # print(result)

    elif user_who_posted in result.keys():
        result[user_who_posted].append(comment)
        # print(result)

consumer.close()

myKeys = list(result.keys())
myKeys.sort()
sorted_dict = {i: result[i] for i in myKeys}

sorted_dict = dumps(sorted_dict, indent = 4)
print(sorted_dict)
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
    like_topic,
     bootstrap_servers=['localhost:9092'],
     value_deserializer=lambda m: m.decode('ascii'))

# consumer = KafkaConsumer(comment_topic)

for message in consumer:
    value = message.value

    if value == "EOF":
        break

    value = value.split()

    user_liking = value[1]
    user_who_posted = value[2]
    post_id = value[3]

    if user_who_posted not in result.keys():
        result[user_who_posted] = {}
        result[user_who_posted][post_id] = 0
        result[user_who_posted][post_id] += 1
        # print(result)

    elif user_who_posted in result.keys():
        if post_id not in result[user_who_posted].keys():
            result[user_who_posted][post_id] = 0
            result[user_who_posted][post_id] += 1
            # print(result)

        elif post_id in result[user_who_posted].keys():
            result[user_who_posted][post_id] += 1


consumer.close()

myKeys = list(result.keys())
myKeys.sort()
sorted_dict = {i: result[i] for i in myKeys}

sorted_dict = dumps(sorted_dict, indent = 4)
print(sorted_dict)
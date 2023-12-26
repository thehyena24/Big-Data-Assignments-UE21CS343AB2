from kafka import KafkaConsumer
from json import loads, dumps
import sys

# topic1 -> Comment
# topic2 -> Like
# topic3 -> Share

comment_topic = sys.argv[1]
like_topic = sys.argv[2]
share_topic = sys.argv[3]

users = set()
comments = {}
likes = {}
shares = {}

result = {}

consumer = KafkaConsumer(
    share_topic,
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: m.decode('ascii'))


for message in consumer:
    value = message.value
    # topic = message.topic

    if value == "EOF":
        break

    if value[0] == 'c':
        comment = ""
        value = value.split(" ", 4)

        user_commenting = value[1]
        user_who_posted = value[2]
        post_id = value[3]

        if len(value) == 5:
            comment = value[4][1:-1]

        users.add(user_who_posted)

        if user_who_posted not in comments.keys():
            comments[user_who_posted] = 0
            comments[user_who_posted] += 1

        elif user_who_posted in comments.keys():
            comments[user_who_posted] += 1

    if value[0] == 'l':
        value = message.value

        value = value.split()

        user_liking = value[1]
        user_who_posted = value[2]
        post_id = value[3]

        users.add(user_who_posted)

        if user_who_posted not in likes.keys():
            likes[user_who_posted] = 0
            likes[user_who_posted] += 1

        elif user_who_posted in likes.keys():
            likes[user_who_posted] += 1



    if value[0] == 's':
        value = value.split(" ", 4)
        shared_users = []

        user_commenting = value[1]
        user_who_posted = value[2]
        post_id = value[3]

        if len(value) > 4:
            shared_users = value[4].split()

        users.add(user_who_posted)

        if user_who_posted not in shares.keys():
            shares[user_who_posted] = 0
            shares[user_who_posted] += len(shared_users)

        elif user_who_posted in shares.keys():
            shares[user_who_posted] += len(shared_users)

consumer.close()

for u in users:

    if u in comments.keys():
        no_c = comments[u]

    else:
        no_c = 0

    if u in likes.keys():
        no_l = likes[u]

    else:
        no_l = 0

    if u in shares.keys():
        no_s = shares[u]

    else:
        no_s = 0

    popularity = (no_l + 20 * (no_s) + 5 * (no_c)) / 1000

    result[u] = popularity

# print("\n\n")

myKeys = list(result.keys())
myKeys.sort()
sorted_dict = {i: result[i] for i in myKeys}

sorted_dict = dumps(sorted_dict, indent = 4)
print(sorted_dict)
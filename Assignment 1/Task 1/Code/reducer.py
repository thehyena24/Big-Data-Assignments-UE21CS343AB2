#!/usr/bin/env python3
import sys
import json

current_name = None
current_strike_rate_sum = 0.0
current_count = 0

for line in sys.stdin:
    record = line.strip().split(",")
    record[1] = float(record[1])

    if current_name == None:
        current_name = record[0]
        current_strike_rate_sum += record[1]
        current_count += 1

    elif record[0] != current_name:
        output = {}

        output["name"] = current_name
        output["strike_rate"] = round(current_strike_rate_sum / current_count, 3)
        print(json.dumps(output))

        current_name = record[0]
        current_strike_rate_sum = 0
        current_strike_rate_sum += record[1]
        current_count = 1

    else:
        current_strike_rate_sum += record[1]
        current_count += 1

    # print(output)

final_output = {}
final_output["name"] = current_name
final_output["strike_rate"] = round(current_strike_rate_sum / current_count, 3)
print(json.dumps(final_output))
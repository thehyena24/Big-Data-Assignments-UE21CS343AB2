#!/usr/bin/env python3
import json
import sys

i = 0

for line in sys.stdin:
    if line[0] != "[" and line[0] != "]":
        record = line.strip()
        # .rstrip(line[-2])
        record = record.split("}")

        record = record[0] + ' }'

        i += 1
        # print(i)
        # print(record)
        
        record = json.loads(record)
        
        # print(record)
        
        runs = float(record["runs"])
        balls = float(record["balls"])

        if balls != 0:
            strike_rate = (runs / balls) * 100
            strike_rate = round(strike_rate, 3)

        else:
            strike_rate = 0.000
            strike_rate = round(strike_rate, 3)

        output = record["name"] + "," + str(strike_rate)

        print(output)
#!/usr/bin/env python3
import sys

current_product_id = None
current_agg_qty = 0

for line in sys.stdin:
    line = line.strip()
    record = line.split()

    if current_product_id == None:
        current_product_id = record[0]

    if current_product_id != record[0]:
        output = current_product_id + "\t" + str(current_agg_qty)
        print(output)
        current_product_id = record[0]
        current_agg_qty = int(record[1])

    else:
        current_agg_qty += int(record[1])

final_output = current_product_id + "\t" + str(current_agg_qty)
print(final_output)
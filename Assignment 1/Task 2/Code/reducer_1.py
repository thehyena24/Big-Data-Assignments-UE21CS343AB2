#!/usr/bin/env python3
import sys

current_product_id = None
current_customer_id = None

for line in sys.stdin:
    temp = line.split()

    if len(temp) == 2:
        current_product_id = temp[0]
        current_customer_id = temp[1]

    elif len(temp) == 3 and temp[0] == current_product_id and temp[1] == current_customer_id:
        print(line.strip())

#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    
    record = line.split()
    
    product_id = record[0]
    quantity = record[2]

    output = product_id + " " + quantity

    print(output)
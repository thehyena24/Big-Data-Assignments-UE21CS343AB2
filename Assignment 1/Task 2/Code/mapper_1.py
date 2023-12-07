#!/usr/bin/env python3
import sys

for line in sys.stdin:
    record = line.strip()

    if record[0] == "o":
        record = record.split()
        
        product_id = record[3]
        customer_id = record[2]
        quantity = record[4]

        output = product_id + " " + customer_id + " " + quantity
        print(output)
    
    if record[0] == "r":
        temp = record.split()

        if int(temp[4]) < 3:
            record = "0" + record
            record = record.split()

            product_id = record[2]
            customer_id = record[3]

            output = product_id + " " + customer_id
            print(output)
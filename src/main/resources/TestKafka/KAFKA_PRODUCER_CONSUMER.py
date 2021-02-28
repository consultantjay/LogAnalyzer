"""
This module help us to Produce/Retrive data from Kafka.
This module contains integration of python and kafka.
@author:Jayvant
@version:1.0
@DISCLAIMER:Following versions may undergo complete makeover
"""

#
import time
from kafka import KafkaProducer

#
print("Started ")

# Using readlines()
file1 = open('./apache-access-log.txt', 'r')
Lines = file1.readlines()
producer = KafkaProducer(bootstrap_servers='localhost:9092')
count = 0

# Strips the newline character
for line in Lines:
    count += 1
    producer.send('LogPublisher',  key=bytes("Key"+str(count),'utf-8'), value=bytes(line.strip(),'utf-8'))
#   p.produce('LogPublisher', key="Key"+str(count), value=str(line.strip()))
#    p.flush(30)
    print("Line{}: {}".format(count, line.strip()))
    time.sleep(5)
    
    
#Consumer Code to test messaage

"""from kafka import KafkaConsumer
consumer = KafkaConsumer('MyFirstTopic')
for message in consumer:
    print (message)"""
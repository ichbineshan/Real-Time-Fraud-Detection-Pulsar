import pulsar
import pandas as pd
from time import sleep
from json import dumps
import random
import os

filepath = os.path.join(os.getcwd(),'transaction_requests.csv')

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer('tj')

df= pd.read_csv(filepath)

# uncomment this for infinite simulation
# while True:
#     req = df.sample(1).to_dict(orient='records')[0]
#     producer.send(dumps(req).encode('utf-8'))
#     sleep(0.1)


# Convert the entire DataFrame to a list of dictionaries
data = df.to_dict(orient='records')

# Shuffle the list of dictionaries to send the rows in random order
random.shuffle(data)

# Send each row to the Pulsar producer
for row in data:
    producer.send(dumps(row).encode('utf-8'))
    # sleep(0.1)

for row in data:
    producer.send(dumps(row).encode('utf-8'))
    
producer.flush()
producer.close()
client.close()

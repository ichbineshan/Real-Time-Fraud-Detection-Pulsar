from flask import Flask, render_template
from inference import get_prediction
import numpy as np
import random 
from time import sleep
from json import dumps, loads
import pulsar
import threading

client = pulsar.Client('pulsar://localhost:6650')
consumer2 = client.subscribe('tr2', 'sub2')

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

def background_job():
    while True:
        msg = consumer2.receive()
        try:
            data = loads(msg.data().decode('utf-8'))
            val = data['merchant']
            val = val.replace('fraud_', '')
            data['merchant'] = val
            result = get_prediction(data)
            if result == 0 and random.random() > 0.90:
                result = 1
            with app.app_context():
                app.result = result
                app.trans_date_trans_time = data['trans_date_trans_time']
                app.cc_num = data['cc_num']
                app.merchant = data['merchant']
                app.category = data['category']
                app.amt = data['amt']
                app.first = data['first']
                app.last = data['last']
                app.gender = data['gender']
                app.street = data['street']
                app.city = data['city']
                app.state = data['state']
                app.zip = data['zip']
                app.lat = data['lat']
                app.long = data['long']
                app.city_pop = data['city_pop']
                app.job = data['job']
                app.dob = data['dob']
                app.trans_num = data['trans_num']
                app.unix_time = data['unix_time']
                app.merch_lat = data['merch_lat']
                app.merch_long = data['merch_long']
            consumer2.acknowledge(msg)
        except Exception:
            pass

def start_background_job():
    thread = threading.Thread(target=background_job)
    thread.daemon = True
    thread.start()

if __name__ == '__main__':
    start_background_job()
    app.run(debug=True)

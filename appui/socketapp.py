from flask import Flask, render_template, Response
from flask_socketio import SocketIO
from inference import get_prediction
import numpy as np
import random 
from json import dumps,loads
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
consumer = None

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('connect')
def test_connect():
    print('Client connected')

@socketio.on('disconnect')
def test_disconnect():
    print('Client disconnected')

def get_data():
    consumer = client.subscribe('tr2', 'my-subscription100')
    while True:
        msg = consumer.receive()
        try:
            data = loads(msg.data().decode('utf-8'))
            val = data['merchant']
            val = val.replace('fraud_','')
            data['merchant']= val
            result = get_prediction(data)
            if result==0 and random.random()>0.90:
                result=1
            yield {
                'result': result,
                'trans_date_trans_time': data['trans_date_trans_time'],
                'cc_num': data['cc_num'],
                'merchant': data['merchant'],
                'category': data['category'],
                'amt': data['amt'],
                'first': data['first'],
                'last': data['last'],
                'gender': data['gender'],
                'street': data['street'],
                'city': data['city'],
                'state': data['state'],
                'zip': data['zip'],
                'lat': data['lat'],
                'long': data['long'],
                'city_pop': data['city_pop'],
                'job': data['job'],
                'dob': data['dob'],
                'trans_num': data['trans_num'],
                'unix_time': data['unix_time'],
                'merch_lat': data['merch_lat'],
                'merch_long': data['merch_long']
            }
        except Exception:
            break
    consumer.close()    

@socketio.on('s')
def stream():
    msg = consumer.receive()
    data = loads(msg.data().decode('utf-8'))
    val = data['merchant']
    val = val.replace('fraud_','')
    data['merchant']= val
    result = get_prediction(data)
    if result==0 and random.random()>0.90:
        result=1
    return socketio.emit({
        'result': result,
        'trans_date_trans_time': data['trans_date_trans_time'],
        'cc_num': data['cc_num'],
        'merchant': data['merchant'],
        'category': data['category'],
        'amt': data['amt'],
        'first': data['first'],
        'last': data['last'],
        'gender': data['gender'],
        'street': data['street'],
        'city': data['city'],
        'state': data['state'],
        'zip': data['zip'],
        'lat': data['lat'],
        'long': data['long'],
        'city_pop': data['city_pop'],
        'job': data['job'],
        'dob': data['dob'],
        'trans_num': data['trans_num'],
        'unix_time': data['unix_time'],
        'merch_lat': data['merch_lat'],
        'merch_long': data['merch_long']})
    #return Response(get_data(), mimetype='text/event-stream')

if __name__=='__main__':
    socketio.run(app, debug=True, host='0.0.0.0')

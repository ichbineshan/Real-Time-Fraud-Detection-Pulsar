# Real-Time-Fraud-Detection-Pulsar
A Real Time Fraud Detection System which utilizes Apache Pulsar's streaming capabilities and machine learning for decision making, flagging simulated transaction requests as fraud or non-fraud.  

Install Apache Pulsar with appropriate Java Dependencies and then start the server.
Once the server is started as a daemon process, run the producer script and then the consumer script integrated in the flask app.  

COMMANDS :  
/home/ubuntu/apache-pulsar-3.0.0/bin/pulsar-daemon start standalone && \   
python3 /home/ubuntu/Pulsar_Real_Time_Fraud_Detection/PythonProducerScript.py && \  
cd /home/ubuntu/Pulsar_Real_Time_Fraud_Detection/appui && \  
/home/ubuntu/Pulsar_Real_Time_Fraud_Detection/venv-3.8/bin/flask --app app run --host=0.0.0.0  



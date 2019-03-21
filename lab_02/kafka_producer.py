import json
from time import sleep
from kafka import *
from datetime import *


if __name__ == '__main__':    

    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    while True:
        message={
            "amount": 1000,
            "client_id": 1234,
            "order_date": datetime.now().isoformat()
        }        
        producer.send('ilya.kobelev', message)
        producer.flush()
        print('Message is sent')
        sleep(10)

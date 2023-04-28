import websocket
from kafka import KafkaProducer
import json

# Connect to the WebSocket
ws = websocket.create_connection("wss://ws.coincap.io/trades/binance")

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Create 6 Kafka topics
topics = ['ethereum', 'dogecoin', 'tether', 'bitcoin', 'idex','goldmaxcoin','others']

# Keep the WebSocket connection alive and send messages to Kafka
while True:
    try:
        # Receive data from the WebSocket
        data = ws.recv()

        # Convert the data to a JSON object
        json_data = json.loads(data)
        # Choose the topic to send the message to based on the symbol
        symbol = json_data['base']
        if symbol == 'ethereum':
            topic = topics[0]
        elif symbol == 'dogecoin':
            topic = topics[1]
        elif symbol == 'tether':
            topic = topics[2]
        elif symbol == 'bitcoin':
            topic = topics[3]
        elif symbol == 'idex':
            topic = topics[4]
        elif symbol == 'goldmaxcoin':
            topic = topics[5]
        else:
            topic = topics[6]

        # Send the JSON object to the Kafka topic
        producer.send(topic, value=json.dumps(json_data).encode('utf-8'))

    except websocket.WebSocketConnectionClosedException:
        print('WebSocket connection closed')
        break
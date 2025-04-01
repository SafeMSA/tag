import pika
import socket
import time
import json
from datetime import datetime

# Constants
SUBSCRIBER_QUEUE = f"subscriber_queue_{socket.gethostname()}"
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 9093
RESPONSE_QUEUE = 'response_queue'  # Name of the queue to send the response to
USER = 'myuser'
PASSWORD = 'mypassword'

# Connect to RabbitMQ and declare the necessary queues
def connect_to_rabbitmq():
    credentials = pika.PlainCredentials(USER, PASSWORD)
    
    while True:
        try:
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST, 
                port=RABBITMQ_PORT,
                credentials=credentials, 
                blocked_connection_timeout=1
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Declare the subscriber queue
            channel.queue_declare(queue=SUBSCRIBER_QUEUE, durable=True)
            # Declare the response queue
            channel.queue_declare(queue=RESPONSE_QUEUE, durable=True)
            # Bind subscriber queue to exchange
            channel.queue_bind(exchange='notifications', queue=SUBSCRIBER_QUEUE)

            return connection, channel
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker) as error:
            print(f"Error connecting to RabbitMQ: {error}. Retrying in 5 seconds...")
            time.sleep(5)

# Send response to another queue
def send_response(channel, message):
    channel.basic_publish(
        exchange='',  # No exchange, direct to queue
        routing_key=RESPONSE_QUEUE,  # Directly to the response queue
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make the message persistent
        )
    )
    print(f"Sent response: {message}")

# Callback to handle incoming messages
def callback(ch, method, properties, body):
    message = json.loads(body.decode())  # Parse incoming JSON message
    print(f"Received: {message}")
    
    # Extract id and timestamp from the received message
    message_id = message.get("id")
    timestamp = message.get("time_sent")

    print(str(timestamp))

    # Calculate the time difference between when the message was sent and when it was received
    message_time = datetime.fromisoformat(timestamp)  # Convert the timestamp to a datetime object
    current_time = datetime.now()  # Get the current time
    time_diff = (current_time - message_time)
    
    # Prepare the response as a JSON object
    response = {
        "id": message_id,
        "tag": socket.gethostname(),
        "time_diff": time_diff,
    }

    # Send the response
    send_response(ch, response)
    
    # Acknowledge the received message
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    while True:
        try:
            # Connect to RabbitMQ
            connection, channel = connect_to_rabbitmq()

            # Start consuming messages from the subscriber queue
            channel.basic_consume(queue=SUBSCRIBER_QUEUE, on_message_callback=callback)

            print("Waiting for messages. To exit press CTRL+C.")
            channel.start_consuming()

        except Exception as e:
            print(f"Error: {e}. Reconnecting...")
            time.sleep(5)

if __name__ == '__main__':
    main()
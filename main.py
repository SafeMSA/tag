import pika
import socket

UNIQUE_ID = 'subscriber_queue_{socket.gethostname()}'
RABBITMQ_HOST = 'rabbitmq1'

def callback(ch, method, properties, body):
    print(f" [x] Received: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge message

# Connect to RabbitMQ
credentials = pika.PlainCredentials('myuser', 'mypassword')
parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Declare queue (durable so it survives restarts)
channel.queue_declare(queue=UNIQUE_ID, durable=True)

# Bind queue to the exchange
channel.queue_bind(exchange='notifications', queue=UNIQUE_ID)

# Consume messages
channel.basic_consume(queue=UNIQUE_ID, on_message_callback=callback)

print(" [*] Waiting for messages. To exit press CTRL+C")
channel.start_consuming()

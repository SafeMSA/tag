import pika
import socket

unique_id = 'subscriber_queue_{socket.gethostname()}'

def callback(ch, method, properties, body):
    print(f" [x] Received: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge message

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Declare queue (durable so it survives restarts)
channel.queue_declare(queue=unique_id, durable=True)

# Bind queue to the exchange
channel.queue_bind(exchange='notifications', queue=unique_id)

# Consume messages
channel.basic_consume(queue=unique_id, on_message_callback=callback)

print(" [*] Waiting for messages. To exit press CTRL+C")
channel.start_consuming()

import pika
import socket
import time

UNIQUE_ID = "subscriber_queue_" + str(socket.gethostname())
RABBITMQ_HOST = ['rabbitmq1', 'rabbitmq2', 'rabbitmq3']

def callback(ch, method, properties, body):
    print(f" Received: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge message

def connect_to_rabbitmq():
    #Attempts to connect to RabbitMQ, retrying until successful.
    credentials = pika.PlainCredentials('myuser', 'mypassword')

    while True:
        for host in RABBITMQ_HOST:
            try:
                print("Trying to connect to " + str(host))
                parameters = pika.ConnectionParameters(host=host, credentials=credentials, blocked_connection_timeout=1)
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                # Declare queue (durable so it survives restarts)
                channel.queue_declare(queue=UNIQUE_ID, durable=True)
                # Bind queue to the exchange
                channel.queue_bind(exchange='notifications', queue=UNIQUE_ID)
                print("Connected to " + str(host))
                return connection, channel
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker):
                print("RabbitMQ not available, retrying in 1 seconds...")
                time.sleep(1)

while True:
    try:
        connection, channel = connect_to_rabbitmq()

        # Consume messages
        channel.basic_consume(queue=UNIQUE_ID, on_message_callback=callback)

        print(" [*] Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()

    except Exception as e:
        channel.stop_consuming()
        print(e)

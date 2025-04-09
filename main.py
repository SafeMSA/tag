import pika
import socket
import time
import json
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import random

# Constants
SUBSCRIBER_QUEUE = f"subscriber_queue_{socket.gethostname()}"
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 9093
RESPONSE_QUEUE = 'response_queue'  # Name of the queue to send the response to
USER = 'myuser'
PASSWORD = 'mypassword'

# Control flags
consume_messages = True

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
    if not consume_messages:  # If consuming is paused, don't process the message
        return

    message = json.loads(body.decode())  # Parse incoming JSON message
    print(f"Received: {message}")
    
    # Extract id and timestamp from the received message
    message_id = message.get("id")
    timestamp = message.get("time_sent")

    # Calculate the time difference between when the message was sent and when it was received
    message_time = datetime.fromisoformat(timestamp)  # Convert the timestamp to a datetime object
    current_time = datetime.now()  # Get the current time
    time_diff = (current_time - message_time).total_seconds()
    
    # Prepare the response as a JSON object
    response = {
        "id": message_id,
        "tag": socket.gethostname(),
        "time_diff": time_diff,
        "time_sent": timestamp
    }

    # Send the response
    send_response(ch, json.dumps(response))
    
    # Acknowledge the received message
    ch.basic_ack(delivery_tag=method.delivery_tag)



# State constants
IDLE = 'idle'
WALKING = 'walking'
DRIVING = 'driving'

# Wi-Fi Connectivity Simulation
class TagSimulator:
    def __init__(self, tag_id, conection_change_callback):
        self.tag_id = tag_id
        self.state = IDLE  # Starting state (initially idle)
        self.state_durations = {
            IDLE: (30, 60),     # idle for 30-60 seconds
            WALKING: (10, 20),  # walking for 20-40 seconds
            DRIVING: (10, 20),  # driving for 10-20 seconds
        }
        self.connectivity = self.calculate_connectivity()
        self.previous_connectivity = self.connectivity
        self.callback = conection_change_callback
    
    def transition_state(self):
        """ Transition between states based on probability """
        if self.state == IDLE:
            # 60% stay idle, 30% walking, 10% driving
            transition_prob = random.random()
            if transition_prob < 0.6:
                self.state = IDLE
            elif transition_prob < 0.9:
                self.state = WALKING
            else:
                self.state = DRIVING
        
        elif self.state == WALKING:
            # 50% stay walking, 40% idle, 10% driving
            transition_prob = random.random()
            if transition_prob < 0.5:
                self.state = WALKING
            elif transition_prob < 0.9:
                self.state = IDLE
            else:
                self.state = DRIVING
        
        elif self.state == DRIVING:
            # 80% driving, 20% walking (driving to walking)
            transition_prob = random.random()
            if transition_prob < 0.8:
                self.state = DRIVING
            else:
                self.state = WALKING

    def calculate_connectivity(self):
        """ Calculate the connectivity based on the current state """
        if self.state == IDLE:
            return 1.0  # Stable connection (100%)
        if self.state == WALKING:
            return random.choices([0.0, 1.0], [0.2, 0.8])[0]
        elif self.state == DRIVING:
            # Intermittent connection: 20% chance of having connection at any moment
            return random.choices([0.0, 1.0], [0.8, 0.2])[0]
        
    def check_connectivity_change(self):
        """ Check if the connectivity has changed, and trigger callback if it has """
        if self.connectivity != self.previous_connectivity:
            # Trigger callback if connectivity state changes
            if self.callback:
                self.callback(self.connectivity)
            # Update previous connectivity state
            self.previous_connectivity = self.connectivity

    def simulate(self):
        """ Simulate the state transitions and connectivity over time """
        while True:
            # Transition to the next state
            self.transition_state()
            
            # Calculate connectivity for the current state
            self.connectivity = self.calculate_connectivity()

            # Check for any connectivity change and call callback
            self.check_connectivity_change()
            
            # Output the current state and connectivity
            print(f"Tag {self.tag_id} is currently {self.state} with connectivity: {self.connectivity:.2f}")
            
            # Wait for the next state transition (based on duration of current state)
            min_duration, max_duration = self.state_durations[self.state]
            duration = random.randint(min_duration, max_duration)
            time.sleep(duration)

def connectivity_callback(connectivity):
    """Callback for connectivity change"""
    global consume_messages
    if connectivity == 0.0:
        print("Connectivity lost, stopping message consumption...")
        consume_messages = False  # Stop consuming messages
    else:
        print("Connectivity restored, starting message consumption...")
        consume_messages = True  # Start consuming messages

def main():
    with ThreadPoolExecutor(max_workers=2) as executor:
        tag = TagSimulator(socket.gethostname(), connectivity_callback)
        executor.submit(tag.simulate)
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
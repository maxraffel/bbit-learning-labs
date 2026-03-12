from consumer_interface import mqConsumerInterface
import pika
import os

class mqConsumer(mqConsumerInterface):
    def __init__(
            self, binding_key: str, exchange_name: str, queue_name: str
    ):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name

        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()

        #Build our connection to the RMQ Connection.



        # Establish Channel

        # Create Queue if not already present
        self.channel.queue_declare(queue=self.queue_name)


        # Create the exchange if not already present
        exchange = self.channel.exchange_declare(
            exchange=self.exchange_name, exchange_type="topic"
        )

        #We can then publish data to that exchange using the basic_publish method
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key="Routing Key",
            body="First message during consumer setup, hello brian!",
        )

        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue=self.queue_name,
            routing_key=self.binding_key,
            exchange=self.exchange_name,
        )

        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            self.queue_name, self.on_message_callback, auto_ack=False
        )

        
    def on_message_callback(self, channel, method_frame, header_frame, body):
         # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag, False)

        #Print message (The message is contained in the body parameter variable)
        print(body)

    def startConsuming(self):
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")
        # Start consuming messages
        self.channel.start_consuming()


    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        # Close Channel
        self.channel.close()
        self.connection.close
        # Close Connection

from producer_interface import mqProducerInterface
import pika
import os


class mqProducer(mqProducerInterface): 

    def __init__(self, routing_key, exchange_name): 
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self):
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="topic")
    

    def publishOrder(self, message):
        self.message = message
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_key, body=self.message)
        self.channel.close()
        self.connection.close()
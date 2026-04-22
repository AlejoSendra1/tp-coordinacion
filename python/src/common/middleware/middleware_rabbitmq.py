import os
import sys
import pika
import pika.exceptions as exceptions
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name: str, prefetch_amount: int = None):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name) 

        self.queue_name = queue_name
        self.channel = channel
        self.connection = connection

        if prefetch_amount is not None:
            self.channel.basic_qos(prefetch_count=prefetch_amount)

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                        routing_key=self.queue_name,
                        body=message)
        except (exceptions.ChannelClosedByClient, exceptions.ChannelClosedByBroker):
            raise(MessageMiddlewareDisconnectedError)
        except:
            raise(MessageMiddlewareMessageError)
    
    def close(self):
        try:
            self.connection.close()
        except:
            raise(MessageMiddlewareCloseError)

    
    def stop_consuming(self):
        try: 
            self.channel.stop_consuming()
        except (exceptions.ChannelClosedByClient, exceptions.ChannelClosedByBroker):
            raise(MessageMiddlewareDisconnectedError)
        except:
            raise(MessageMiddlewareMessageError)


    def start_consuming(self, on_message_callback):

        # para poder wrappear la funcion dada por argumento a 
        # una funcion que recibe los 4 args como obliga la libreria
        def callback(ch, method, _, body):   # Una funcion es un poco mas simpatica que una lambda
            ack_func = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack_func = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body,ack_func, nack_func)

        try: 
            self.channel.basic_consume(queue=self.queue_name,
                            on_message_callback=callback)
            self.channel.start_consuming()
        except (exceptions.ChannelClosedByClient, exceptions.ChannelClosedByBroker):
            raise(MessageMiddlewareDisconnectedError)
        except:
            raise(MessageMiddlewareMessageError)
            
    
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys, prefetch_amount: int = None):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()

        channel.exchange_declare(exchange=exchange_name,
            exchange_type='topic') 

        self.exchange_name = exchange_name
        self.channel = channel
        self.connection = connection
        self.routing_keys = routing_keys      

        if prefetch_amount is not None:
            self.channel.basic_qos(prefetch_count=prefetch_amount)

        

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name,
                        routing_key=routing_key,
                        body=message)
                
        except (exceptions.ChannelClosedByClient, exceptions.ChannelClosedByBroker):
            raise(MessageMiddlewareDisconnectedError)
        except:
            raise(MessageMiddlewareMessageError)
        
    def close(self):
        try:
            self.connection.close()
        except:
            raise(MessageMiddlewareCloseError)
    
    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except (exceptions.ChannelClosedByClient, exceptions.ChannelClosedByBroker):
            raise(MessageMiddlewareDisconnectedError)
        except Exception as e:
            raise(e)
    
    def start_consuming(self, on_message_callback):
        def callback(ch, method, _, body):
            on_message_callback(body, 
                                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                                lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
                                )
            
        try:
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue

            for routing_key in self.routing_keys:    
                self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name,  routing_key=routing_key)

            self.channel.basic_consume(
                queue=queue_name, on_message_callback=callback)
            self.channel.start_consuming()    

        except (exceptions.ChannelClosedByClient, exceptions.ChannelClosedByBroker):
            raise(MessageMiddlewareDisconnectedError)
        except:
            raise(MessageMiddlewareMessageError)

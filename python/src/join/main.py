import bisect
import os
import logging
import signal

from common import middleware, message_protocol, fruit_item
from common.node.node import Node

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])

INSTANCE_NAME = 'Join'

class JoinFilter(Node):

    def __init__(self):
        self.fruit_top = dict()
        self.eof_notifications = dict()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def _process_fruit_record(self, msg) -> None:
        logging.error(f"Unexpected record Type message received in Join node") 


    def _process_fruit_top(self, msg):

        msg_client_id = msg[message_protocol.internal.MsgField.CLIENT_ID]
        parcial_fruit_top = msg[message_protocol.internal.MsgField.DATA]

        logging.info(f"Processing partial top msg of Client: {msg_client_id} partial top: {parcial_fruit_top}")

        self.fruit_top.setdefault(msg_client_id,[])
        for fruit in parcial_fruit_top:
            fruit_name, amount = fruit
            bisect.insort(self.fruit_top[msg_client_id], fruit_item.FruitItem(fruit_name, amount))
        

    def _process_eof(self, msg):

        msg_client_id = msg[message_protocol.internal.MsgField.CLIENT_ID]
        sender = msg[message_protocol.internal.MsgField.SENDER]

        logging.info(f"Processing EOF msg of Client: {msg_client_id} , sent by: {sender}")

        if self.eof_notifications.get(msg_client_id,None) is None:
            self.eof_notifications[msg_client_id] = set()

        self.eof_notifications[msg_client_id].add(sender)
        if len(self.eof_notifications[msg_client_id]) < AGGREGATION_AMOUNT:
            return

        self.fruit_top.setdefault(msg_client_id, []) # en caso de que nunca haya procesado algo de ese cliente
        fruit_chunk = list(self.fruit_top[msg_client_id][-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        ###
        logging.info(f"Top a enviar al cliente: {fruit_top}")    
        self.output_queue.send(message_protocol.internal.serialize_fruit_top(msg_client_id,fruit_top,INSTANCE_NAME))
        
        del self.fruit_top[msg_client_id]
        del self.eof_notifications[msg_client_id]

    def start(self):
        def callback (message, ack, nack):
            try:
                self.process_data_messsage(message)
                ack()
            except:
                nack()
        self.input_queue.start_consuming(callback)

    def graceful_shutdown(self):
        self.input_queue.stop_consuming()
        self.input_queue.close()

        self.output_queue.close()

def main():
    try:
        logging.basicConfig(level=logging.INFO)

        join_filter = JoinFilter()
        signal.signal(signal.SIGTERM, join_filter.graceful_shutdown)

        join_filter.start()

    except Exception as e:
        logging.exception(e)

    finally:
        join_filter.graceful_shutdown()

    return 0


if __name__ == "__main__":
    main()

import os
import logging
import signal

from common import middleware, message_protocol, fruit_item
from common.node.node import Node

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

INSTANCE_NAME = f'{AGGREGATION_PREFIX}_{ID}'

class AggregationFilter(Node): 

    def __init__(self):
        self.fruit_top = dict()
        self.eof_notifications = dict()

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f'{ID}']
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def _process_fruit_record(self, msg):
        logging.info("Processing data message")

        msg_client_id = msg[message_protocol.internal.MsgField.CLIENT_ID]
        fruit, amount = msg[message_protocol.internal.MsgField.DATA]

        self.fruit_top.setdefault(msg_client_id, [])

        for i in range(len(self.fruit_top[msg_client_id])):
            if self.fruit_top[msg_client_id][i].fruit == fruit:
                self.fruit_top[msg_client_id][i] = self.fruit_top[msg_client_id][i] + fruit_item.FruitItem(
                    fruit, amount
                )
                return
        self.fruit_top[msg_client_id].append(fruit_item.FruitItem(fruit, amount))

    def _process_fruit_top(self, msg):
        logging.error(f"Unexpected fruit top Type message received in aggregation node") 

    def _process_eof(self, msg):

        msg_client_id = msg[message_protocol.internal.MsgField.CLIENT_ID]
        sender = msg[message_protocol.internal.MsgField.SENDER]

        logging.info(f"Processing EOF msg of Client: {msg_client_id} , sent by: {sender}")

        if self.eof_notifications.get(msg_client_id,None) is None:
            self.eof_notifications[msg_client_id] = set()

        self.eof_notifications[msg_client_id].add(sender)
        if len(self.eof_notifications[msg_client_id]) < SUM_AMOUNT:
            return

        self.fruit_top.setdefault(msg_client_id, []) # en caso de que nunca haya procesado algo de ese cliente
        self.fruit_top[msg_client_id].sort()
        fruit_chunk = list(self.fruit_top[msg_client_id][-TOP_SIZE:])

        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )

        logging.info(f"Partial top instance to send: {fruit_top}")    
        self.output_queue.send(message_protocol.internal.serialize_fruit_top(msg_client_id,fruit_top,INSTANCE_NAME))
        self.output_queue.send(message_protocol.internal.serialize_eof_message(msg_client_id,INSTANCE_NAME,False))

        #se van liberando los recursos
        del self.fruit_top[msg_client_id]
        del self.eof_notifications[msg_client_id]

    def start(self):
        def callback (message, ack, nack):
            try:
                self.process_data_messsage(message)
                ack()
            except:
                nack()

        self.input_exchange.start_consuming(callback)

    def graceful_shutdown(self):
        self.input_exchange.stop_consuming()
        self.input_exchange.close()

        self.output_queue.close()

def main():
    try:
        logging.basicConfig(level=logging.INFO)

        aggregation_filter = AggregationFilter()
        signal.signal(signal.SIGTERM, aggregation_filter.graceful_shutdown)

        aggregation_filter.start()

    except Exception as e:
        logging.exception(e)

    finally:
        aggregation_filter.graceful_shutdown()

    return 0


if __name__ == "__main__":
    main()

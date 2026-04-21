import bisect
import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

INSTANCE_NAME = 'Join'
class JoinFilter:

    def __init__(self):
        self.fruit_top = dict()
        self.eof_notifications = dict()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        
        msg = message_protocol.internal.deserialize(message)
        msg_type = msg[message_protocol.internal.MsgField.MSG_TYPE]
        msg_client = msg[message_protocol.internal.MsgField.CLIENT_ID]

        if msg_type == message_protocol.internal.MsgType.FRUIT_TOP:
            self._process_fruit_top(msg_client , msg[message_protocol.internal.MsgField.DATA]) 
        elif msg_type == message_protocol.internal.MsgType.END_OF_RECODS:
            self._process_eof(msg_client, msg[message_protocol.internal.MsgField.SENDER])
        else:
            raise("Error llego cualquier cosa al join")
        #self.output_queue.send(message_protocol.internal.serialize(fruit_top))
        ack()

    def _process_fruit_top(self, client_id, fruit_top):
        self.fruit_top.setdefault(client_id,[])
        for fruit in fruit_top:
            self._modify_partial_top(client_id,*fruit)

    def _modify_partial_top(self, client_id, fruit_name, amount):
        bisect.insort(self.fruit_top[client_id], fruit_item.FruitItem(fruit_name, amount))

    def _process_eof(self,client_id, sender):
        logging.info("Received EOF")

        if self.eof_notifications.get(client_id,None) is None:
            self.eof_notifications[client_id] = set()

        self.eof_notifications[client_id].add(sender)
        if len(self.eof_notifications[client_id]) < AGGREGATION_AMOUNT:
            return

        self.fruit_top.setdefault(client_id, []) # en caso de que nunca haya procesado algo de ese cliente
        fruit_chunk = list(self.fruit_top[client_id][-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        ###
        logging.info(f"Top a enviar al cliente: {fruit_top}")    
        self.output_queue.send(message_protocol.internal.serialize_fruit_top(client_id,fruit_top,INSTANCE_NAME))
        
        del self.fruit_top[client_id]
        del self.eof_notifications[client_id]

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()

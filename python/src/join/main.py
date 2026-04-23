import bisect
import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
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

        signal.signal(signal.SIGTERM, self.graceful_shutdown)


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
            logging.error(f"Unexpected msg type received") 
            
        ack()

    def _process_fruit_top(self, client_id, fruit_top):
        self.fruit_top.setdefault(client_id,[])
        for fruit in fruit_top:
            self._modify_partial_top(client_id,*fruit)

    def _modify_partial_top(self, client_id, fruit_name, amount):
        bisect.insort(self.fruit_top[client_id], fruit_item.FruitItem(fruit_name, amount))

    def _process_eof(self,client_id, sender):
        logging.info(f"Processing EOF msg of Client: {client_id} , sent by: {sender}")

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

    def graceful_shutdown(self):
        self.input_exchange.stop_cosuming()
        self.input_exchange.close()

        self.output_queue.close()

def main():
    try:
        logging.basicConfig(level=logging.INFO)

        join_filter = JoinFilter()
        signal.signal(signal.SIGTERM, join_filter.graceful_shutdown)

        join_filter.start()

    except:
        pass

    finally:
        join_filter.graceful_shutdown()

    return 0


if __name__ == "__main__":
    main()

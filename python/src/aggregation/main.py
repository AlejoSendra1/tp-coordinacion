import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

INSTANCE_NAME = f'{AGGREGATION_PREFIX}_{ID}'

class AggregationFilter: 

    def __init__(self):
        self.fruit_top = dict()
        self.eof_notifications = dict()

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f'{ID}']
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        self.fruit_top.setdefault(client_id, [])

        for i in range(len(self.fruit_top[client_id])):
            if self.fruit_top[client_id][i].fruit == fruit:
                self.fruit_top[client_id][i] = self.fruit_top[client_id][i] + fruit_item.FruitItem(
                    fruit, amount
                )
                return
        self.fruit_top[client_id].append(fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id, sender):
        logging.info(f"Processing EOF msg of Client: {client_id} , sent by: {sender}")
        if self.eof_notifications.get(client_id,None) is None:
            self.eof_notifications[client_id] = set()

        self.eof_notifications[client_id].add(sender)
        if len(self.eof_notifications[client_id]) < SUM_AMOUNT:
            return

        self.fruit_top.setdefault(client_id, []) # en caso de que nunca haya procesado algo de ese cliente
        self.fruit_top[client_id].sort()
        fruit_chunk = list(self.fruit_top[client_id][-TOP_SIZE:])

        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )

        logging.info(f"Partial top instance to send: {fruit_top}")    
        self.output_queue.send(message_protocol.internal.serialize_fruit_top(client_id,fruit_top,INSTANCE_NAME))
        self.output_queue.send(message_protocol.internal.serialize_eof_message(client_id,INSTANCE_NAME,False))

        #se van liberando los recursos
        del self.fruit_top[client_id]
        del self.eof_notifications[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        
        msg = message_protocol.internal.deserialize(message)
        msg_type = msg[message_protocol.internal.MsgField.MSG_TYPE]
        msg_client = msg[message_protocol.internal.MsgField.CLIENT_ID]

        if msg_type == message_protocol.internal.MsgType.FRUIT_RECORD:
            self._process_data(msg_client ,*msg[message_protocol.internal.MsgField.DATA])
        elif msg_type == message_protocol.internal.MsgType.END_OF_RECODS:
            self._process_eof(msg_client, msg[message_protocol.internal.MsgField.SENDER])
        else:
            logging.error(f"Unexpected msg type received") 

        ack()


    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    def graceful_shutdown(self):
        self.input_exchange.stop_cosuming()
        self.input_exchange.close()

        self.output_queue.close()

def main():
    try:
        logging.basicConfig(level=logging.INFO)

        aggregation_filter = AggregationFilter()
        signal.signal(signal.SIGTERM, aggregation_filter.graceful_shutdown)

        aggregation_filter.start()

    except:
        pass

    finally:
        aggregation_filter.graceful_shutdown()

    return 0


if __name__ == "__main__":
    main()

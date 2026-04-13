import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = dict()

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        self.fruit_top.setdefault(client_id, [])

        for i in range(len(self.fruit_top[client_id])):
            if self.fruit_top[client_id][i].fruit == fruit:
                self.fruit_top[client_id][i] = self.fruit_top[client_id][i] + fruit_item.FruitItem(
                    fruit, amount
                )
                logging.info(f"Added: {amount} , from client: {client_id}, to fruit: {fruit}")
                return
        logging.info(f"Added new fruit: {fruit}, from client: {client_id}")    
        bisect.insort(self.fruit_top[client_id], fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        logging.info(f"estado de los registros del cliente {client_id}")    
        for fitem in self.fruit_top[client_id]:
            logging.info(f"fruta: {fitem.fruit} cantidad: {fitem.amount} cliente: {client_id}")    

        fruit_chunk = list(self.fruit_top[client_id][-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        logging.info(f"Top a enviar al cliente: {fruit_top}")    
        self.output_queue.send(message_protocol.internal.serialize_fruit_top(client_id,fruit_top))
        del self.fruit_top[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        
        msg = message_protocol.internal.deserialize(message)
        msg_type = msg[message_protocol.internal.MsgField.MSG_TYPE]
        msg_client = msg[message_protocol.internal.MsgField.CLIENT_ID]

        if msg_type == message_protocol.internal.MsgType.FRUIT_RECORD:
            self._process_data(msg_client ,*msg[message_protocol.internal.MsgField.DATA])
        elif msg_type == message_protocol.internal.MsgType.END_OF_RECODS:
            self._process_eof(msg_client)
        else:
            raise("Error llego cualquier cosa al Agregation")
        ack()


    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()

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

INSTANCE_NAME = f'{AGGREGATION_PREFIX}_{ID}'

class AggregationFilter: # comenzar a poner logs sobre eof de clientes ya cerrados y quien manda

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [AGGREGATION_PREFIX]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = dict()
        self.eof_notifications = dict()

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        self.fruit_top.setdefault(client_id, [])

        for i in range(len(self.fruit_top[client_id])):
            if self.fruit_top[client_id][i].fruit == fruit:
                self.fruit_top[client_id][i] = self.fruit_top[client_id][i] + fruit_item.FruitItem(
                    fruit, amount
                )
                #logging.info(f"Added: {amount} , from client: {client_id}, to fruit: {fruit}") #BORRAR
                return
        #logging.info(f"Added new fruit: {fruit}, from client: {client_id}")    #BORRAR
        bisect.insort(self.fruit_top[client_id], fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id, sender):
        logging.info("Received EOF")
        
        logging.info(f"eof enviado por: {sender} del cliente: {client_id}")
        if self.eof_notifications.get(client_id,None) is None:
            self.eof_notifications[client_id] = set()

        self.eof_notifications[client_id].add(sender)
        if len(self.eof_notifications[client_id]) < SUM_AMOUNT:
            return

        logging.info("ordenando arreglo")
        self.fruit_top[client_id].sort()
        logging.info(f'la lista final para el cliente es:')
        for fitem in self.fruit_top[client_id]:
            logging.info(f'registro: ({fitem.fruit},{fitem.amount})')


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
        logging.info(f"Top enviado al cliente")    
        ###
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

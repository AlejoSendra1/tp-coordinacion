import os
import logging
import zlib

from common import middleware, message_protocol

MOM_HOST = os.environ["MOM_HOST"]
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumOutput:
    def __init__(self, instance_name: str):
        self.instance_name = instance_name
        self.sum_eof_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, [SUM_PREFIX]
        )
        self.data_output_exchanges = dict()

        for id in range(AGGREGATION_AMOUNT):
            self.data_output_exchanges[id] = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f'{id}']
            )
        
    def send_fuits(self, client_id: str, final_fruit_item):
        aggretor_addr = get_aggregator_addr(client_id, final_fruit_item.fruit)
        logging.info(f"fruta: {final_fruit_item.fruit} del cliente: {client_id} a enviarse al aggregation num: {aggretor_addr}")
        self.data_output_exchanges[aggretor_addr].send(
            message_protocol.internal.serialize_fruit_register_message(
                client_id,
                [final_fruit_item.fruit, final_fruit_item.amount],
                self.instance_name
            )
        )

    def send_eof(self, client_id: str, must_propagate: bool):
        logging.info(f"Broadcasting EOF of client {client_id} message to aggregators")
        for data_output_exchange in self.data_output_exchanges.values():
            data_output_exchange.send(message_protocol.internal.serialize_eof_message(client_id, self.instance_name))

        if must_propagate:
            logging.info(f"Broadcasting EOF {client_id} message to sums from: {self.instance_name}")
            self.sum_eof_exchange.send(message_protocol.internal.serialize_eof_message(client_id, self.instance_name, False))

    def graceful_shutdown(self):
        self.sum_eof_exchange.stop_consuming()
        self.sum_eof_exchange.close()

        for _,exchange in self.data_output_exchanges.items():
            exchange.stop_consuming()
            exchange.close()

def get_aggregator_addr(client_id: str, fruit_name: str):
    hash_result = zlib.adler32(f"{client_id},{fruit_name}".encode())
    logging.info(f'Aggregation ID of intance to receive the msg: {hash_result % AGGREGATION_AMOUNT}')
    return hash_result % AGGREGATION_AMOUNT
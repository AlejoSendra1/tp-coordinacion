import os
import logging
import threading

from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumOutput:
    def __init__(self, instance_name):
        self.instance_name = instance_name
        self.sum_eof_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, [SUM_PREFIX]
        )

        self.data_output_exchanges = [] # no es mejor una working queue ?
        
        for _ in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [AGGREGATION_PREFIX]
            )
            self.data_output_exchanges.append(data_output_exchange)

    def send_fuits(self, client_id, final_fruit_item):
        #logging.info(f"Broadcasting fruits message to aggregators")
        for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize_fruit_register_message(
                        client_id,
                        [final_fruit_item.fruit, final_fruit_item.amount],
                        self.instance_name
                    )
                )

    def send_eof(self, client_id , must_propagate: bool):
        logging.info(f"Broadcasting EOF of client {client_id} message to aggregators")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize_eof_message(client_id, self.instance_name, True))

        if must_propagate:
            logging.info(f"Broadcasting EOF {client_id} message to sums from: {self.instance_name}")
            self.sum_eof_exchange.send(message_protocol.internal.serialize_eof_message(client_id, self.instance_name, False))
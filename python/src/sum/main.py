import os
import logging

import sum_input
import sum_output
from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

INSTANCE_NAME = f'{SUM_PREFIX}_{ID}'

class SumFilter:
    def __init__(self):
        self.input = sum_input.SumInput(INSTANCE_NAME)
        self.output = sum_output.SumOutput(INSTANCE_NAME)
        self.amount_by_fruit = dict()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        self.amount_by_fruit.setdefault(client_id, dict())
        self.amount_by_fruit[client_id][fruit] = self.amount_by_fruit[client_id].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))
        #logging.info(f"amount agregada al cliente: {client_id} estado actual de {fruit}: {self.amount_by_fruit[client_id][fruit].amount} ")    

    def _process_eof(self, client_id, must_propagate):
        logging.info(f"Broadcasting data messages")
        self.amount_by_fruit.setdefault(client_id, dict())
        
        for final_fruit_item in self.amount_by_fruit[client_id].values():
            self.output.send_fuits(client_id, final_fruit_item)
        
        self.output.send_eof(client_id, must_propagate)
        del self.amount_by_fruit[client_id]

    def process_data_messsage(self, msg):
        logging.info("process data msg")
        msg_type = msg[message_protocol.internal.MsgField.MSG_TYPE]
        msg_client = msg[message_protocol.internal.MsgField.CLIENT_ID]

        if msg_type == message_protocol.internal.MsgType.FRUIT_RECORD:
            self._process_data(msg_client ,*msg[message_protocol.internal.MsgField.DATA])
        elif msg_type == message_protocol.internal.MsgType.END_OF_RECODS:
            
            must_propagate = msg[message_protocol.internal.MsgField.PROPAGATE]
            self._process_eof(msg_client, must_propagate)
        else:
            raise("Error llego cualquier cosa al Sum") # TO DO modificar con una excepcion 
        

    def start(self):
        self.input.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0

if __name__ == "__main__":
    main()

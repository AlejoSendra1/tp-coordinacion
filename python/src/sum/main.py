import os
import logging
import signal

import sum_input
import sum_output
from common.node.node import Node
from common import message_protocol, fruit_item

ID = int(os.environ["ID"])
SUM_PREFIX = os.environ["SUM_PREFIX"]

INSTANCE_NAME = f'{SUM_PREFIX}_{ID}'

class SumFilter(Node):
    def __init__(self):
        self.input = sum_input.SumInput(INSTANCE_NAME)
        self.output = sum_output.SumOutput(INSTANCE_NAME)
        self.amount_by_fruit = dict()

    def _process_fruit_record(self, msg) -> None:
        logging.info(f"Process fruit record")
        msg_client_id = msg[message_protocol.internal.MsgField.CLIENT_ID]
        fruit, amount = msg[message_protocol.internal.MsgField.DATA]
         
        self.amount_by_fruit.setdefault(msg_client_id, dict())
        self.amount_by_fruit[msg_client_id][fruit] = self.amount_by_fruit[msg_client_id].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_fruit_top(self, msg):
        logging.error(f"Unexpected fruit top message received in Sum node") 

    def _process_eof(self, msg) -> None:
        logging.info(f"Process EOF")
        must_propagate = msg[message_protocol.internal.MsgField.PROPAGATE]
        msg_client_id = msg[message_protocol.internal.MsgField.CLIENT_ID]

        self.amount_by_fruit.setdefault(msg_client_id, dict())
        
        for final_fruit_item in self.amount_by_fruit[msg_client_id].values():
            self.output.send_fuits(msg_client_id, final_fruit_item)
        
        self.output.send_eof(msg_client_id, must_propagate) # habria q ver como handlear errores de mqtt en gral TODO

        del self.amount_by_fruit[msg_client_id]        

    def start(self) -> None:
        self.input.start_consuming(self.process_data_messsage)

    def graceful_shutdown(self):
        self.input.graceful_shutdown()
        self.output.graceful_shutdown()

def main():
    try:
        logging.basicConfig(level=logging.INFO)
        
        sum_filter = SumFilter()
        signal.signal(signal.SIGTERM, sum_filter.graceful_shutdown)

        sum_filter.start()

    except Exception as e:
        logging.exception(e)

    finally:
        sum_filter.graceful_shutdown()

    return 0

if __name__ == "__main__":
    main()

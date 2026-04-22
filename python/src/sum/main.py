import os
import logging
import signal

import sum_input
import sum_output
from common import message_protocol, fruit_item

ID = int(os.environ["ID"])
SUM_PREFIX = os.environ["SUM_PREFIX"]

INSTANCE_NAME = f'{SUM_PREFIX}_{ID}'

class SumFilter:
    def __init__(self):
        self.input = sum_input.SumInput(INSTANCE_NAME)
        self.output = sum_output.SumOutput(INSTANCE_NAME)
        self.amount_by_fruit = dict()

        signal.signal(signal.SIGTERM, self.graceful_shutdown)

    def _process_fruit_record(self, client_id: str, fruit: str, amount: int) -> None:
        logging.info(f"Process fruit record")
        self.amount_by_fruit.setdefault(client_id, dict())
        self.amount_by_fruit[client_id][fruit] = self.amount_by_fruit[client_id].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id: str, must_propagate: bool) -> None:
        logging.info(f"Process EOF")
        self.amount_by_fruit.setdefault(client_id, dict())
        
        for final_fruit_item in self.amount_by_fruit[client_id].values():
            self.output.send_fuits(client_id, final_fruit_item)
        
        self.output.send_eof(client_id, must_propagate) # habria q ver como handlear errores de mqtt en gral

        del self.amount_by_fruit[client_id]

    def process_data_messsage(self, msg) -> None:
        logging.info("Process msg")
        
        msg_type = msg[message_protocol.internal.MsgField.MSG_TYPE]
        msg_client = msg[message_protocol.internal.MsgField.CLIENT_ID]

        if msg_type == message_protocol.internal.MsgType.FRUIT_RECORD:
            self._process_fruit_record(msg_client ,*msg[message_protocol.internal.MsgField.DATA])
        elif msg_type == message_protocol.internal.MsgType.END_OF_RECODS:
            must_propagate = msg[message_protocol.internal.MsgField.PROPAGATE]
            self._process_eof(msg_client, must_propagate)
        else:
            logging.error(f"Unexpected msg type received") 
        

    def start(self) -> None:
            self.input.start_consuming(self.process_data_messsage)

    def graceful_shutdown(self):
        self.input.graceful_shutdown()
        self.output.graceful_shutdown()

def main():
    logging.basicConfig(level=logging.INFO)
    # agregar finally que cierre todo re clean
    sum_filter = SumFilter()
    sum_filter.start()
    sum_filter.graceful_shutdown()
    return 0

if __name__ == "__main__":
    main()

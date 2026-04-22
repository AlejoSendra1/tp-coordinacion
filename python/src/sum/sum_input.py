import os
import logging
import threading
from heapq import heappush,heappop

import time
from common import middleware, message_protocol
from common.msg_dto import Msg_dto
from common.utils import function_retry

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_PREFIX = os.environ["SUM_PREFIX"]

SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"   # exchange for control btwn sum's
RETRY_BASE_TIME = 2                             # used for exponential backoff must be bigger than 1
PREFETCH_AMOUNT = 10


class SumInput:
    def __init__(self, instance_name):
        self.instance_name = instance_name

        self.lock = threading.Lock()
        self.msg_heap = []

        read_eof_exchange_thread = threading.Thread(target=self.read_eof_exchange, args=())
        read_sum_queue_thread = threading.Thread(target=self.read_sum_queue, args=())
        read_eof_exchange_thread.start()
        read_sum_queue_thread.start()


    def read_eof_exchange(self):
        sum_eof_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, [SUM_PREFIX]
        )
        def handle_broadcast(message, ack, nack):  
            try: 
                msg = message_protocol.internal.deserialize(message)
                if msg[message_protocol.internal.MsgField.SENDER] != self.instance_name:
                    with self.lock:
                        logging.info("guardando en heap eof de cliente")
                        heappush(self.msg_heap,Msg_dto(msg))
                ack()
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                nack()            
        sum_eof_exchange.start_consuming(handle_broadcast)

    def start_consuming(self, process_msg):
        read_retry_time = RETRY_BASE_TIME
        heap_was_empty = False

        while True: # MODIFICAR EL WHILE TRUE TODO
            logging.info("Reading from msg heap")
            with self.lock:
                if len(self.msg_heap) != 0:
                    function_retry(process_msg, [heappop(self.msg_heap).msg])
                    read_retry_time = RETRY_BASE_TIME
                    heap_was_empty = False
                else:
                    heap_was_empty = True

            if heap_was_empty: # Exponential backoff
                logging.info("Heap is empty")
                time.sleep(read_retry_time)
                read_retry_time *= 2           

    def graceful_shutdown():
        pass
    
# no puedo tener todo en un hilo porq se complica el hecho de se dificulta vaciar una cola sin el callback invocado por cada 
    def read_sum_queue(self):
        input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE, PREFETCH_AMOUNT
        )

        def callback (message, ack, nack):
            try: 
                msg = message_protocol.internal.deserialize(message)
                with self.lock:
                    logging.info("Pushing fruit register into msg heap")
                    heappush(self.msg_heap,Msg_dto(msg))
                ack()
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                nack()
        
        input_queue.start_consuming(callback)
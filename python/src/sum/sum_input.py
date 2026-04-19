import os
import logging
import threading
from heapq import heappush,heappop

import time
from common import middleware, message_protocol
from common.msg_dto import Msg_dto

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

RETRY_BASE_TIME = 2

class SumInput:
    def __init__(self, instance_name):
        self.instance_name = instance_name

        self.lock = threading.Lock()
        self.msg_heap = []

        logging.info("inicializando threads de input")
        read_eof_exchange_thread = threading.Thread(target=self.read_eof_exchange, args=())
        read_sum_queue_thread = threading.Thread(target=self.read_sum_queue, args=())
        read_eof_exchange_thread.start()
        read_sum_queue_thread.start()
        logging.info("threads de input inicializados")

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
        # consume con exponential backoff 
        retry_time = RETRY_BASE_TIME

        heap_was_empty = False
        while True: # MODIFICAR EL WHILE TRUE TODO
            logging.info("leyendo vamos a leer del heap")
            with self.lock:
                if len(self.msg_heap) != 0:
                    process_msg(heappop(self.msg_heap).msg)
                    retry_time = RETRY_BASE_TIME
                    heap_was_empty = False
                else:
                    heap_was_empty = True
            if heap_was_empty:
                logging.info("no hay nada en heap")
                time.sleep(retry_time)
                retry_time *= 2           
    
# no puedo tener todo en un hilo porq se complica el hecho de se dificulta vaciar una cola sin el callback invocado por cada 
    def read_sum_queue(self):
        input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        def callback (message, ack, nack):
            try: 
                msg = message_protocol.internal.deserialize(message)
                with self.lock:
                    logging.info("guardando en heap registro de frutas")
                    heappush(self.msg_heap,Msg_dto(msg))
                ack()
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                nack()
        
        input_queue.start_consuming(callback)
import logging
from abc import ABC, abstractmethod
from common import message_protocol

class Node(ABC):

    # Comienza a consumir de su input pasando cada mensaje de datos o de control a la funcion process_data_messsage.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse lo eleva.
    @abstractmethod
    def start(self) -> None:
        pass
    
    # Dado el mensaje "msg" recibido, determina la funcion a aplicar en base al tipo de mensaje.
    def process_data_messsage(self, msg) -> None:
        logging.info("Process msg")
        msg = message_protocol.internal.deserialize(msg)
        msg_type = msg[message_protocol.internal.MsgField.MSG_TYPE]

        if msg_type == message_protocol.internal.MsgType.FRUIT_RECORD:
            self._process_fruit_record(msg)
        elif msg_type == message_protocol.internal.MsgType.FRUIT_TOP:
            self._process_fruit_top(msg)
        elif msg_type == message_protocol.internal.MsgType.END_OF_RECODS:
            self._process_eof(msg)
        else:
            logging.error(f"Unexpected Msg Type received: {msg_type}") 
        

    # Procesa un mensaje de tipo FRUIT_RECORD 
    @abstractmethod
    def _process_fruit_record(self, msg) -> None:
        pass

    # Procesa un mensaje de tipo FRUIT_TOP
    @abstractmethod
    def _process_fruit_top(self, msg) -> None:
        pass

    # Procesa un mensaje de tipo END_OF_RECORDS
    @abstractmethod
    def _process_eof(self, msg) -> None:
        pass

    # Libera los recursos y cierra las conexiones pertinentes para cerra el sistema de forma graceful
    @abstractmethod
    def graceful_shutdown(self):
        pass







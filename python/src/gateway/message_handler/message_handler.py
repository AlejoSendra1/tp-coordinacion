from common import message_protocol
import uuid
import logging

from common.message_protocol.internal import MsgField

class MessageHandler:

    def __init__(self):
        self.client_uuid = uuid.uuid4().hex
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize_fruit_register_message(self.client_uuid ,[fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize_eof_message(self.client_uuid)

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)

        if self.client_uuid != fields[MsgField.CLIENT_ID]:
            return None
        logging.info(f"resultado final: {fields[MsgField.DATA]}")    
        return fields[MsgField.DATA]

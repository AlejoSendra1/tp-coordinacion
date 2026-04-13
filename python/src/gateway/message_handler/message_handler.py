from common import message_protocol
import uuid

class MessageHandler:

    def __init__(self):
        self.client_uuid = uuid.uuid4()
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(
            {
             'msg_type': message_protocol.internal.MsgType.END_OF_RECODS,   
             'client_id': self.client_uuid, 
             'data':[fruit, amount]
            }
        )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(
            {
                'msg_type': message_protocol.internal.MsgType.END_OF_RECODS,
                'client_id': self.client_uuid,
                'data': []
            }
        )

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if self.client_uuid != fields['client_id']:
            return None
        return fields['data']

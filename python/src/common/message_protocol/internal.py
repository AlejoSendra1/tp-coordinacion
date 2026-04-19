from datetime import datetime, timezone
import json

class MsgType:
    FRUIT_RECORD = 1
    FRUIT_TOP = 2
    ACK = 3
    END_OF_RECODS = 4

class MsgField:
    MSG_TYPE = 'msg_type'
    CLIENT_ID = 'client_id'
    DATA = 'data'
    TIMESTAMP = 'timestamp'
    SENDER = 'sender'
    PROPAGATE = 'propagate'

def serialize_fruit_register_message(client_id , fruit_register, sender):
    return serialize(
        {
            MsgField.MSG_TYPE: MsgType.FRUIT_RECORD,   
            MsgField.CLIENT_ID: client_id, 
            MsgField.DATA: fruit_register,
            MsgField.TIMESTAMP: datetime.now(timezone.utc).isoformat(),
            MsgField.SENDER: sender
        }
    )

def serialize_fruit_top(client_id,fruit_top, sender):
    return serialize(
        {
            MsgField.MSG_TYPE: MsgType.FRUIT_TOP,
            MsgField.CLIENT_ID: client_id,
            MsgField.DATA: fruit_top,
            MsgField.TIMESTAMP: datetime.now(timezone.utc).isoformat(),
            MsgField.SENDER: sender
        }
    )

def serialize_eof_message(client_id, sender, propagate: bool): # agregar un default por si falta el sender para evitar harcodear el "getaway" en el handler
    return serialize(
        {
            MsgField.MSG_TYPE: MsgType.END_OF_RECODS,
            MsgField.CLIENT_ID: client_id,
            MsgField.DATA: [],
            MsgField.TIMESTAMP: datetime.now(timezone.utc).isoformat(),
            MsgField.SENDER: sender,
            MsgField.PROPAGATE: propagate,
        }
    )


# Serializacion y Deserializacion de datos ------------
def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))

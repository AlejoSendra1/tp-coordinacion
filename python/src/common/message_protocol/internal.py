from datetime import datetime, timezone
import json

UNDEFINED_SENDER = 'Undefined'

class MsgType:
    FRUIT_RECORD = 1
    FRUIT_TOP = 2
    ACK = 3
    END_OF_RECODS = 4

class MsgField:
    MSG_TYPE = "1"
    CLIENT_ID = "2"
    DATA = "3"
    TIMESTAMP = "4"
    SENDER = "5"
    PROPAGATE = "6"

def serialize_fruit_register_message(client_id: str , fruit_register, sender: str = UNDEFINED_SENDER) -> bytes:
    return serialize(
        {
            MsgField.MSG_TYPE: MsgType.FRUIT_RECORD,   
            MsgField.CLIENT_ID: client_id, 
            MsgField.DATA: fruit_register,
            MsgField.TIMESTAMP: datetime.now(timezone.utc).isoformat(),
            MsgField.SENDER: sender
        }
    )

def serialize_fruit_top(client_id: str, fruit_top: list, sender: str = UNDEFINED_SENDER) -> bytes:
    return serialize(
        {
            MsgField.MSG_TYPE: MsgType.FRUIT_TOP,
            MsgField.CLIENT_ID: client_id,
            MsgField.DATA: fruit_top,
            MsgField.TIMESTAMP: datetime.now(timezone.utc).isoformat(),
            MsgField.SENDER: sender
        }
    )

def serialize_eof_message(client_id: str, sender: str = UNDEFINED_SENDER, propagate: bool = True) -> bytes: 
    return serialize(
        {
            MsgField.MSG_TYPE: MsgType.END_OF_RECODS,
            MsgField.CLIENT_ID: client_id,
            MsgField.TIMESTAMP: datetime.now(timezone.utc).isoformat(),
            MsgField.SENDER: sender,
            MsgField.PROPAGATE: propagate,
        }
    )

# Serializacion y Deserializacion de datos ------------
def serialize(message) -> bytes:
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))

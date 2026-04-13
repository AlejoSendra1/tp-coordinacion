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


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))

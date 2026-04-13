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

# La funcion espera el registro de una fruta con la facha [ fruit_name , amount ]
def serialize_fruit_register_message(client_id , fruit_register):
    return serialize(
        {
            'msg_type': MsgType.FRUIT_RECORD,   
            'client_id': client_id, 
            'data': fruit_register
        }
    )

def serialize_fruit_top(client_id,fruit_top):
    return serialize(
        {
            'msg_type': MsgType.FRUIT_TOP,
            'client_id': client_id,
            'data': fruit_top
        }
    )

def serialize_eof_message(client_id):
    return serialize(
        {
            'msg_type': MsgType.END_OF_RECODS,
            'client_id': client_id,
            'data': []
        }
    )


# Serializacion y Deserializacion de datos ------------
def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))

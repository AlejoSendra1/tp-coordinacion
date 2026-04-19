from common import message_protocol

class Msg_dto:
    def __init__(self, msg):
        self.msg = msg

    def __lt__(self,other):
        return self.msg[message_protocol.internal.MsgField.TIMESTAMP] < other.msg[message_protocol.internal.MsgField.TIMESTAMP]
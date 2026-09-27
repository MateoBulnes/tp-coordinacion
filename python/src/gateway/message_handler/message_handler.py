from common import message_protocol


class MessageHandler:
    _last_client_id = 0

    def __init__(self):
        MessageHandler._last_client_id += 1
        self.client_id = MessageHandler._last_client_id

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(
            message_protocol.internal.DATA, self.client_id, [[fruit, amount]]
        )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(
            message_protocol.internal.EOF, self.client_id
        )

    def deserialize_result_message(self, message):
        _, client_id, fruit_top = message_protocol.internal.deserialize(message)
        if client_id != self.client_id:
            return None
        return fruit_top

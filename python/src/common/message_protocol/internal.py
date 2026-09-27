import json

# Tipos de mensaje
DATA = 1
EOF = 2
TOP = 3


def serialize(msg_type, client_id, payload=None):
    return json.dumps([msg_type, client_id, payload]).encode("utf-8")


def deserialize(message):
    [msg_type, client_id, payload] = json.loads(message.decode("utf-8"))
    return msg_type, client_id, payload

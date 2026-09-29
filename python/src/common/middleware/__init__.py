from .middleware import (
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
)
from .middleware_rabbitmq import (
    MessageMiddlewareQueueRabbitMQ,
    MessageMiddlewareExchangeRabbitMQ,
)

ERRORS = (
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
)

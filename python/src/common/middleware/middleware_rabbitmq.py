import pika

from .middleware import (
    MessageMiddleware,
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

DEFAULT_AMQP_PORT = 5672
DEFAULT_EXCHANGE_TYPE = "direct"

# Un mensaje sin ackear por consumidor
CONSUMER_PREFETCH_COUNT = 1

# Cubren los errores que pueden venir del transporte.
_TRANSPORT_ERRORS = (
    pika.exceptions.AMQPError,
    pika.exceptions.ChannelError,
    pika.exceptions.ReentrancyError,
    OSError,
)


def _raise_domain_error(error):
    """Traduce una excepción del transporte a la excepción de dominio."""

    if isinstance(error, (pika.exceptions.AMQPConnectionError, OSError)):
        raise MessageMiddlewareDisconnectedError(str(error)) from error
    raise MessageMiddlewareMessageError(str(error)) from error


class _RabbitMQMiddleware(MessageMiddleware):
    """Contiene la lógica común a los dos modelos que son el ciclo de vida de
    la conexión y el canal. Cada instancia es dueña de una conexión y un canal
    propios.

    Las subclases setean self._queue_name con la cola de la que consumen.
    """

    def __init__(self, host):
        # Arrancan en None para que close() funcione aunque falle la conexión.
        self._connection = None
        self._channel = None

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host, port=DEFAULT_AMQP_PORT))
            self._channel = self._connection.channel()
        except _TRANSPORT_ERRORS as error:
            _raise_domain_error(error)

    def start_consuming(self, on_message_callback):
        # pika entrega los mensajes con su propia firma de 4 args, mientras que
        # la interfaz espera 3. dispatch funciona como un adaptador entre las 2
        def _dispatch(channel, method, props, body):
            on_message_callback(
                body,
                lambda: channel.basic_ack(method.delivery_tag),
                lambda: channel.basic_nack(method.delivery_tag, requeue=True))

        try:
            self._channel.basic_qos(prefetch_count=CONSUMER_PREFETCH_COUNT)
            self._channel.basic_consume(queue=self._queue_name,
                                        on_message_callback=_dispatch,
                                        auto_ack=False)
            # Bloquea hasta que stop_consuming() cancele el consumo.
            self._channel.start_consuming()
        except _TRANSPORT_ERRORS as error:
            _raise_domain_error(error)

    def stop_consuming(self):
        try:
            self._channel.stop_consuming()
        except _TRANSPORT_ERRORS as error:
            _raise_domain_error(error)

    def close(self):
        try:
            try:
                if self._channel is not None and self._channel.is_open:
                    self._channel.close()
            finally:
                # Garantizamos que la conexión se cierre aunque falle el cierre del canal
                if self._connection is not None and self._connection.is_open:
                    self._connection.close()
        except _TRANSPORT_ERRORS as error:
            raise MessageMiddlewareCloseError(str(error)) from error


class MessageMiddlewareQueueRabbitMQ(_RabbitMQMiddleware, MessageMiddlewareQueue):
    """Modelo Productor-Consumidor sobre una work queue, todos los consumidores comparten
    la misma cola, así que cada mensaje lo procesa uno solo de ellos."""

    def __init__(self, host, queue_name):
        super().__init__(host)
        self._queue_name = queue_name

        try:
            # auto_delete en False, si no, la cola se borraría con sus
            # mensajes cuando se va el último consumidor.
            self._channel.queue_declare(
                queue=queue_name,
                durable=False,
                exclusive=False,
                auto_delete=False)
        except _TRANSPORT_ERRORS as error:
            self.close()
            _raise_domain_error(error)

    def send(self, message):
        # El exchange por defecto rutea a la cola que se llama igual que la routing key.
        try:
            self._channel.basic_publish(exchange="",
                                        routing_key=self._queue_name,
                                        body=message)
        except _TRANSPORT_ERRORS as error:
            _raise_domain_error(error)


class MessageMiddlewareExchangeRabbitMQ(_RabbitMQMiddleware,
                                        MessageMiddlewareExchange):
    """Modelo Publisher-Subscriber sobre un exchange, cada suscriptor recibe su propia
    copia de los mensajes de sus routing keys."""

    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self._exchange_name = exchange_name
        self._routing_keys = list(routing_keys)
        # La cola del suscriptor se crea al empezar a consumir, así un objeto
        # que solo publica no deja una cola que nadie lee.
        self._queue_name = None

        try:
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=DEFAULT_EXCHANGE_TYPE,
                durable=False)
        except _TRANSPORT_ERRORS as error:
            self.close()
            _raise_domain_error(error)

    def _declare_subscriber_queue(self):
        """Declara la cola privada de este suscriptor y la bindea a sus routing
        keys. Si ya está declarada no vuelve a declararla, para
        que un segundo start_consuming no cree una cola nueva.
        """
        if self._queue_name is not None:
            return

        try:
            result = self._channel.queue_declare(queue="",
                                                 exclusive=True,
                                                 durable=False)
            self._queue_name = result.method.queue

            for routing_key in self._routing_keys:
                self._channel.queue_bind(queue=self._queue_name,
                                         exchange=self._exchange_name,
                                         routing_key=routing_key)
        except _TRANSPORT_ERRORS as error:
            _raise_domain_error(error)

    def start_consuming(self, on_message_callback):
        self._declare_subscriber_queue()
        super().start_consuming(on_message_callback)

    def send(self, message):
        try:
            for routing_key in self._routing_keys:
                self._channel.basic_publish(exchange=self._exchange_name,
                                            routing_key=routing_key,
                                            body=message)
        except _TRANSPORT_ERRORS as error:
            _raise_domain_error(error)

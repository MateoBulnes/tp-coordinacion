import os
import logging
from collections import defaultdict

from common import middleware, message_protocol, fruit_accumulator

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.accumulators = defaultdict(fruit_accumulator.FruitAccumulator)

    def _process_data(self, client_id, records):
        self.accumulators[client_id].add(records)

    def _process_eof(self, client_id):
        accumulator = self.accumulators.pop(
            client_id, fruit_accumulator.FruitAccumulator()
        )
        fruit_top = accumulator.top(TOP_SIZE)
        logging.info(f"Sending partial top of client {client_id}")
        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.TOP, client_id, fruit_top
            )
        )

    def process_message(self, message, ack, nack):
        msg_type, client_id, payload = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.DATA:
            self._process_data(client_id, payload)
        elif msg_type == message_protocol.internal.EOF:
            self._process_eof(client_id)
        else:
            logging.error(f"Unknown message type {msg_type}")
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()

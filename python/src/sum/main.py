import os
import logging
from collections import defaultdict

from common import middleware, message_protocol, fruit_accumulator

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
            
        self.accumulators = defaultdict(fruit_accumulator.FruitAccumulator)

    def _process_data(self, client_id, records):
        self.accumulators[client_id].add(records)

    def _process_eof(self, client_id):
        accumulator = self.accumulators.pop(
            client_id, fruit_accumulator.FruitAccumulator()
        )
        records = accumulator.items()
        logging.info(f"Flushing {len(records)} fruits of client {client_id}")

        for data_output_exchange in self.data_output_exchanges:
            if records:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        message_protocol.internal.DATA, client_id, records
                    )
                )
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.EOF, client_id
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
        self.input_queue.start_consuming(self.process_message)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

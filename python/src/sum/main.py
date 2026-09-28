import os
import zlib
import logging
from collections import defaultdict

from common import middleware, message_protocol, fruit_accumulator

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

CONTROL_EXCHANGE_SUFFIX = "control"


def _aggregation_for(fruit):
    """Decide que replica de Aggregation tiene a cargo una fruta."""
    return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT


class SumFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        control_exchange = f"{SUM_PREFIX}_{CONTROL_EXCHANGE_SUFFIX}"
        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            control_exchange,
            [f"{SUM_PREFIX}_{ID}"],
            shared_with=self.input_queue,
        )
        self.control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            control_exchange,
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT) if i != ID],
            shared_with=self.input_queue,
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                AGGREGATION_PREFIX,
                [f"{AGGREGATION_PREFIX}_{i}"],
                shared_with=self.input_queue,
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.accumulators = defaultdict(fruit_accumulator.FruitAccumulator)

    def _process_data(self, client_id, records):
        self.accumulators[client_id].add(records)

    def _broadcast_end_of_ingestion(self, client_id):
        logging.info(f"Broadcasting end of ingestion of client {client_id}")
        self.control_output.send(
            message_protocol.internal.serialize(
                message_protocol.internal.EOF, client_id
            )
        )

    def _shard(self, records):
        """Agrupa las frutas por la replica de Aggregation que las tiene a
        cargo."""
        shards = [[] for _ in range(AGGREGATION_AMOUNT)]
        for record in records:
            [fruit, _] = record
            shards[_aggregation_for(fruit)].append(record)
        return shards

    def _finish(self, client_id):
        accumulator = self.accumulators.pop(
            client_id, fruit_accumulator.FruitAccumulator()
        )
        shards = self._shard(accumulator.items())
        logging.info(
            f"Flushing fruits of client {client_id} "
            f"in shards {[len(shard) for shard in shards]}"
        )

        for shard, data_output_exchange in zip(shards, self.data_output_exchanges):
            if shard:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        message_protocol.internal.DATA, client_id, shard
                    )
                )
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.EOF, client_id
                )
            )

    def process_data_message(self, message, ack, nack):
        msg_type, client_id, payload = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.DATA:
            self._process_data(client_id, payload)
        elif msg_type == message_protocol.internal.EOF:
            self._broadcast_end_of_ingestion(client_id)
            self._finish(client_id)
        else:
            logging.error(f"Unknown message type {msg_type}")
        ack()

    def process_control_message(self, message, ack, nack):
        msg_type, client_id, _ = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.EOF:
            self._finish(client_id)
        else:
            logging.error(f"Unknown control message type {msg_type}")
        ack()

    def start(self):
        self.control_input.consume(self.process_control_message)
        self.input_queue.start_consuming(self.process_data_message)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

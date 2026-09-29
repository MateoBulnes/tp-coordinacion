import os
import signal
import logging
from collections import defaultdict

from common import middleware, message_protocol, fruit_accumulator

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.accumulators = defaultdict(fruit_accumulator.FruitAccumulator)
        self.partial_tops_received = defaultdict(int)

    def _process_partial_top(self, client_id, records):
        self.accumulators[client_id].add(records)
        self.partial_tops_received[client_id] += 1
        logging.info(
            f"Received partial top "
            f"{self.partial_tops_received[client_id]}/{AGGREGATION_AMOUNT} "
            f"of client {client_id}"
        )

        if self.partial_tops_received[client_id] < AGGREGATION_AMOUNT:
            return

        fruit_top = self.accumulators.pop(client_id).top(TOP_SIZE)
        del self.partial_tops_received[client_id]
        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.TOP, client_id, fruit_top
            )
        )

    def process_message(self, message, ack, nack):
        msg_type, client_id, payload = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.TOP:
            self._process_partial_top(client_id, payload)
        else:
            logging.error(f"Unknown message type {msg_type}")
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_message)

    def stop(self):
        self.input_queue.request_stop()

    def close(self):
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)

    try:
        join_filter = JoinFilter()
        signal.signal(signal.SIGTERM, lambda signum, frame: join_filter.stop())
        try:
            join_filter.start()
        finally:
            join_filter.close()
    except middleware.ERRORS as error:
        logging.error(error)
        return 1

    logging.info("Shutting down")
    return 0


if __name__ == "__main__":
    main()

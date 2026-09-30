import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.count = {} # va a llegar de Aggregation_amount antes de enviar todo. client - count
        self.dict = {} # Identico al de sum pero sumo tops

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        (client_id, fruit_top) = message_protocol.internal.deserialize(message)
        if client_id not in self.count:
            self.count[client_id] = 1
            self.dict[client_id] = {}
            for fruit, amount in fruit_top:
                # Mismo codigo que usado en sum
                self.dict[client_id][fruit] = self.dict[client_id].get(
                    fruit, fruit_item.FruitItem(fruit, 0)
                ) + fruit_item.FruitItem(fruit, int(amount))
        else:
            self.count[client_id] += 1
            for fruit, amount in fruit_top:
                # Mismo codigo que usado en sum
                self.dict[client_id][fruit] = self.dict[client_id].get(
                    fruit, fruit_item.FruitItem(fruit, 0)
                ) + fruit_item.FruitItem(fruit, int(amount))

        # Si llegaron todos los tops de un client de los aggregations, ahi envio el top definitivo
        if self.count[client_id] == AGGREGATION_AMOUNT:
            sorted_list = sorted(self.dict[client_id].values()) # ordeno
            # Recupero el top igual que hace un aggregation
            fruit_chunk = sorted_list[-TOP_SIZE:]
            fruit_chunk.reverse()
            fruit_to_send = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    fruit_chunk,
                )
            )
            self.output_queue.send(message_protocol.internal.serialize((client_id, fruit_to_send)))
            del self.dict[client_id] # Ya lo envie, no lo necesito mas
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()

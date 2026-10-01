import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
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
        self.fruit_top = {} # va a ser cliente - fruit_item
        self.dict_eof = {} # cliente - cantidad de eofs recibidos. Debe llegar a SUM_AMOUNT

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        # similar a sum, pero aca es cliente - fruit_item para mantener el orden
        if client_id not in self.fruit_top:
            self.fruit_top[client_id] = [] # Creo la lista de este client
        else:
            for i in range(len(self.fruit_top[client_id])):
                if self.fruit_top[client_id][i].fruit == fruit:
                    self.fruit_top[client_id][i] = self.fruit_top[client_id][i] + fruit_item.FruitItem(
                        fruit, amount
                    ) # Con esto se actualiza la fruta
                    return
        bisect.insort(self.fruit_top[client_id], fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")

        # Si recibi SUM_AMOUNT de este cliente mando a join. Sino solo sumo 1
        if client_id not in self.dict_eof:
            self.dict_eof[client_id] = 0
        self.dict_eof[client_id] += 1

        if self.dict_eof[client_id] == SUM_AMOUNT:
            # La lista solo la armo al momento de mandar todo a join
            fruits_client = self.fruit_top.get(client_id, [])
            sorted_list = sorted(fruits_client)
            fruit_chunk = list(sorted_list[-TOP_SIZE:])
            fruit_chunk.reverse()
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    fruit_chunk,
                )
            )

            self.output_queue.send(message_protocol.internal.serialize((client_id, fruit_top)))
            del self.fruit_top[client_id] # Solo al cliente terminado se lo elimina

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def handle_sigterm(self, signum, frame):
        # Aca no necesito threadsafe porque es un hilo solo
        self.input_exchange.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_exchange.start_consuming(self.process_messsage)

        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()

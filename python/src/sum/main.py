import os
import logging
import threading
import zlib

from common import middleware, message_protocol, fruit_item
from threading import Thread, Lock

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
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

        #Para enviar al resto de sums
        self.control_exchange = []
        for i in range(SUM_AMOUNT):
            if  i == ID:
                continue
            control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{i}"]
            )
            self.control_exchange.append(control_exchange)
        # Para escuchar
        self.control_message = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{ID}"]
        )

        self.lock = threading.Lock()
        self.amount_by_fruit = {} # Va a ser de clientes - frutas y el value cantidad

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")

        with self.lock:
            # Agrego al diccionario el cliente si no esta. Despues dentro del cliente iran sus frutas
            if client_id not in self.amount_by_fruit:
                self.amount_by_fruit[client_id] = {}
            # Ahora la clave es id + fruta para distinguir entre clientes
            self.amount_by_fruit[client_id][fruit] = self.amount_by_fruit[client_id].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")

        with self.lock:
            # El if para evitar el caso de 1° mensaje de este client en este sum sea eof y rompa
            if client_id in self.amount_by_fruit:
                for final_fruit_item in self.amount_by_fruit[client_id].values():
                    # Envio el mensaje a solo 1 aggregator y me aseguro que sea entre 0 y aggregation_amount - 1
                    # misma fruta va al mismo aggregator, asi no falla combinar tops mas adelante
                    aggregator_to_send = zlib.crc32(final_fruit_item.fruit.encode()) % AGGREGATION_AMOUNT
                    self.data_output_exchanges[aggregator_to_send].send(
                        message_protocol.internal.serialize(
                            [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                        )
                    )
                # Ya se envio todo a algun aggregator
                del self.amount_by_fruit[client_id]

        logging.info(f"Broadcasting EOF message")
        # Broadcast (excepto a mi mismo) del EOF a cada sum
        for node in self.control_exchange:
            node.send(message_protocol.internal.serialize([client_id]))
        # Mensaje a cada aggregator indicando que ya tiene todo lo de este cliente de este sum
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))
       
    def _process_control_work(self, client_id):
        with self.lock:
            if client_id not in self.amount_by_fruit:
                # Mensaje a cada aggregator indicando que ya tiene todo lo de este cliente
                for data_output_exchange in self.data_output_exchanges:
                    data_output_exchange.send(message_protocol.internal.serialize([client_id]))
                return # Indica que yo hice el broadcast de este cliente
                # ya envie todo a aggregatory elimine los datos de este cliente
            
            # Llego el oef de otro sum, mando todo de ese cliente al aggregaor
            for final_fruit_item in self.amount_by_fruit[client_id].values():
                # Envio el mensaje a solo 1 aggregator y me aseguro que sea entre 0 y aggregation_amount - 1
                # misma fruta va al mismo aggregator, asi no falla combinar tops mas adelante
                aggregator_to_send = zlib.crc32(final_fruit_item.fruit.encode()) % AGGREGATION_AMOUNT
                self.data_output_exchanges[aggregator_to_send].send(
                    message_protocol.internal.serialize(
                        [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )
            
            del self.amount_by_fruit[client_id] # Borro data de este cliente en este sum
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message_protocol.internal.serialize([client_id])) # Para sumar 1 en aggregator


    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3: # 2 -> 3 por agregar ID
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 1:
            self._process_control_work(*fields)
        ack()

    def start(self):
        Thread(target=self.control_message.start_consuming, args=(self.process_control_message,)).start()
        Thread(target=self.input_queue.start_consuming, args=(self.process_data_message,)).start()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

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

        self.count_by_client = {} # client - count de mensajes recibidos (para cordinacion)
        self.count_cordination = {} # client - count pero guardo solo el n° de mensaje de un eof
        # Solo guardo los clientes que en este sum soy cordinador (me llego el eof)

        self.cordinators = {} # cliente - sum cordinador (detalles en el informe)

        self.send_lock = threading.Lock() # Por los problemas de threadsafe al enviar mensajes

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        # Voy contando los mensajes por cliente para coordinar (mas detalle en el informe)
        if not client_id in self.count_by_client:
            self.count_by_client[client_id] = 0
        self.count_by_client[client_id] += 1
        with self.lock:
            # Agrego al diccionario el cliente si no esta. Despues dentro del cliente iran sus frutas
            if client_id not in self.amount_by_fruit:
                self.amount_by_fruit[client_id] = {}
            # Ahora la clave es id + fruta para distinguir entre clientes
            self.amount_by_fruit[client_id][fruit] = self.amount_by_fruit[client_id].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
        
        if self.count_by_client[client_id] == 0: # Estaba en -1, faltaban mansajes por procesar
            with self.send_lock:
                self.control_exchange[self.cordinators[client_id]].send(message_protocol.internal.serialize([client_id, 1]))
                self.count_by_client[client_id] = -1 # para volver al mismo estado

    def _process_eof(self, client_id, count):
        logging.info(f"Broadcasting data messages")

        with self.lock:
            # El if para evitar el caso de 1° mensaje de este client en este sum sea eof y rompa
            if client_id in self.amount_by_fruit:
                for final_fruit_item in self.amount_by_fruit[client_id].values():
                    # Envio el mensaje a solo 1 aggregator y me aseguro que sea entre 0 y aggregation_amount - 1
                    # misma fruta va al mismo aggregator, asi no falla combinar tops mas adelante
                    aggregator_to_send = zlib.crc32(final_fruit_item.fruit.encode()) % AGGREGATION_AMOUNT
                    with self.send_lock:
                        self.data_output_exchanges[aggregator_to_send].send(
                            message_protocol.internal.serialize(
                                [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                            )
                        )
                # Ya se envio todo a algun aggregator
                del self.amount_by_fruit[client_id]

        logging.info(f"Broadcasting EOF message")
        self.count_cordination[client_id] = count
        if not client_id in self.count_by_client:
            count_cordinator = 0 # EOF fue el unico mensaje que me llego de este clietne
            self.count_by_client[client_id] = 0
        else:
            count_cordinator = self.count_by_client[client_id] # agrego a este valor lo que recibi

        if count_cordinator == count:
            # Broadcast (excepto a mi mismo) del EOF a cada sum
            with self.send_lock:
                for node in self.control_exchange:
                    node.send(message_protocol.internal.serialize([client_id]))
            # Ya todos avisados de mandar al aggregation
            del self.count_cordination[client_id]
            del self.count_by_client[client_id]

            # Mensaje a cada aggregator indicando que ya tiene todo lo de este cliente de este sum
            with self.send_lock:
                for data_output_exchange in self.data_output_exchanges:
                    data_output_exchange.send(message_protocol.internal.serialize([client_id]))
        else:
            with self.send_lock:
                for node in self.control_exchange:
                    # El 0 no importa, es para cambiar la cantidad de parametros
                    # Aca pido que me diga cada sum cuantos mensajes del cliente llego para coordinar
                    node.send(message_protocol.internal.serialize([client_id, ID, 0]))
       
    def _process_control_work(self, client_id):
        with self.lock:
            if client_id not in self.amount_by_fruit:
                # Mensaje a cada aggregator indicando que ya tiene todo lo de este cliente
                with self.send_lock:
                    for data_output_exchange in self.data_output_exchanges:
                        data_output_exchange.send(message_protocol.internal.serialize([client_id]))
                    return # Indica que yo hice el broadcast de este cliente
                    # ya envie todo a aggregatory elimine los datos de este cliente
            
            # Llego el oef de otro sum, mando todo de ese cliente al aggregaor
            for final_fruit_item in self.amount_by_fruit[client_id].values():
                # Envio el mensaje a solo 1 aggregator y me aseguro que sea entre 0 y aggregation_amount - 1
                # misma fruta va al mismo aggregator, asi no falla combinar tops mas adelante
                aggregator_to_send = zlib.crc32(final_fruit_item.fruit.encode()) % AGGREGATION_AMOUNT
                with self.send_lock:
                    self.data_output_exchanges[aggregator_to_send].send(
                        message_protocol.internal.serialize(
                            [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                        )
                    )
            
            del self.amount_by_fruit[client_id] # Borro data de este cliente en este sum
            with self.send_lock:
                for data_output_exchange in self.data_output_exchanges:
                    data_output_exchange.send(message_protocol.internal.serialize([client_id])) # Para sumar 1 en aggregator


    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3: # 2 -> 3 por agregar ID
            self._process_data(*fields)
        else: # Pasa a ser 2 por mandar el count de mensajes
            self._process_eof(*fields)
        ack()

    def _process_send_information(self, client_id, sum_id, count):
        # El count recibido es 0 para tener un parametro mas, aca no me interesa ese valor
        if ID < sum_id:
            # Es asi porque no me guardo a mi mismo en la lista de sums para enviar por exchange un mensaje
            sum_cordinator_to_send = sum_id - 1
        else: # Necesariamente mayor, nunca va a ser igal
            sum_cordinator_to_send = sum_id
        # Envio la cant de mensajes de ese cliente recibidos por mi
        if not client_id in self.count_by_client:
            with self.send_lock:
                # Esto implica que este sum no trabajo sobre el cliente
                self.control_exchange[sum_cordinator_to_send].send(message_protocol.internal.serialize([client_id, 0]))

        else:
            with self.send_lock:
                self.control_exchange[sum_cordinator_to_send].send(message_protocol.internal.serialize([client_id, self.count_by_client[client_id]]))
        self.count_by_client[client_id] = -1 # Para distinguir de los que no enviaron
        self.cordinators[client_id] = sum_cordinator_to_send # Por si me llega otro mensaje de ese cliente para avisarle

    def _process_recive_feedback(self, client_id, count):
        self.count_by_client[client_id] += count # Actualizo con lo que llega
        if self.count_cordination[client_id] == self.count_by_client[client_id]:
            # Broadcast (excepto a mi mismo) del EOF a cada sum
            with self.send_lock:
                for node in self.control_exchange:
                    node.send(message_protocol.internal.serialize([client_id]))

            # Ya todos avisados de mandar al aggregation
            del self.count_cordination[client_id]
            del self.count_by_client[client_id]

            with self.send_lock:
                # Mensaje a cada aggregator indicando que ya tiene todo lo de este cliente de este sum
                for data_output_exchange in self.data_output_exchanges:
                    data_output_exchange.send(message_protocol.internal.serialize([client_id]))


    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 1:
            self._process_control_work(*fields)
        elif len(fields) == 2: # 2 -> client id y count del sum que manda
            self._process_recive_feedback(*fields)
        else: # 3 -> client id, sum id y count del sum que manda
            self._process_send_information(*fields)
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

import pika
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError


# Por defecto, RabbitMQ envía cada mensaje al siguiente consumidor, en secuencia. 
# En promedio, cada consumidor recibe la misma cantidad de mensajes (Round Robin)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            
            self.channel = channel
            self.connection = connection
            self.queue_name = queue_name
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareDisconnectedError(error)

    def start_consuming(self, on_message_callback):
        try:
            self.channel.basic_qos(prefetch_count=1) # Hasta no terminar la tarea, Rabbit no envia otra al worker

            def callback(ch, method, properties, body):
                on_message_callback(
                    message=body,
                    ack=lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                    nack=lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
                )
            
            self.channel.basic_consume(queue=self.queue_name,
                        on_message_callback=callback) # Saco el ACK automatico

            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError(error)
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareMessageError(error)

    def stop_consuming(self):
        try:
            if self.channel.is_open:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareDisconnectedError(error)
    
    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                        routing_key=self.queue_name,
                        body=message,
                        properties=pika.BasicProperties( # Hago que los mensajes sean persistentes
                            delivery_mode = pika.DeliveryMode.Persistent
                        ))
        except pika.exceptions.AMQPConnectionError as error:
            # Este error para probelmas de conexion
            raise MessageMiddlewareDisconnectedError(error)
        except pika.exceptions.AMQPError as error:
            # El MessageError en cualquier otro caso
            raise MessageMiddlewareMessageError(error)

    def close(self):
        try:
            if self.connection.is_open: # Para hacer close solo si la conexion esta abierta
                self.connection.close()
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareCloseError(error)

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            channel = connection.channel()
            channel.exchange_declare(exchange=exchange_name, exchange_type='direct') # Creo el exchange
            # direct manda mensajes a las colas con binding key = routing key
            
            self.channel = channel
            self.connection = connection
            self.exchange_name = exchange_name
            self.routing_keys = routing_keys
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareDisconnectedError(error)

    def start_consuming(self, on_message_callback):
        try:
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue # Rabbit me da el nombre de la queue

            # Hago un binding por cada rputing key
            for key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, 
                            queue=queue_name, 
                            routing_key=key)

            def callback(ch, method, properties, body):
                on_message_callback(
                    message=body,
                    ack=lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                    nack=lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
                )

            self.channel.basic_consume(
                queue=queue_name, on_message_callback=callback) # El ACK automatico del tutorial rompia el test

            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError(error)
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareMessageError(error)
    
    def stop_consuming(self):
        try:
            if self.channel.is_open:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareDisconnectedError(error)

    def send(self, message):
        try:
            for key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, 
                            routing_key=key, 
                            body=message)
        except pika.exceptions.AMQPConnectionError as error:
            # Este error para probelmas de conexion
            raise MessageMiddlewareDisconnectedError(error)
        except pika.exceptions.AMQPError as error:
            # El MessageError en cualquier otro caso
            raise MessageMiddlewareMessageError(error)
 
    def close(self):
        try:
            if self.connection.is_open: # Para hacer close solo si la conexion esta abierta
                self.connection.close()
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareCloseError(error)

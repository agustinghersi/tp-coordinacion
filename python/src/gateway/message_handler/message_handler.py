from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4()) # ID para saber que cliente envio el mensaje
        self.count = 0 # Contador de mensajes para un cliente
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.count += 1 # Todavia no necesito mandarlo
        return message_protocol.internal.serialize([self.client_id, fruit, amount]) # Paso ID al protocolo interno

    def serialize_eof_message(self, message):
        # El mensaje de eof no lo voy a contar para coordinar los sum
        return message_protocol.internal.serialize([self.client_id, self.count])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        (client_id, fruit_top) = fields
        if client_id == self.client_id:
            return fruit_top # En este caso, este top corresponde a este cliente
        return None # Si no es ese cliente, se ignora para esperar su top correspondiente

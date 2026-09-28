from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4()) # ID para saber que cliente envio el mensaje
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_id, fruit, amount]) # Paso ID al protocolo interno

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        (client_id, fruit_top) = fields
        if client_id == self.client_id:
            return fruit_top # En este caso, este top corresponde a este cliente
        return None # Si no es ese cliente, se ignora para esperar su top correspondiente

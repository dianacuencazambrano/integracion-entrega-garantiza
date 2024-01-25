import os
import json
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from bson import ObjectId

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Configuración del consumidor Kafka
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BROKER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_KEY'),
    'sasl.password': os.getenv('KAFKA_SECRET'),
    'group.id': 'integracionKafkaPython',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(conf)

# Configuración de la conexión a MongoDB
mongo_client = MongoClient(os.getenv('DATABASE_URL'))
db = mongo_client.get_database()

def on_assign(consumer, partitions):
    for partition in partitions:
        partition.offset = 0

consumer.subscribe(['orders2'], on_assign=on_assign)

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        try:
            # Parsear el mensaje JSON
            message = json.loads(msg.value().decode('utf-8'))
            id = message.get('id')
            value = message.get('value')

            print({
                'id': id,
                'value': value,
            })

            # Actualizar el estado de recepción en MongoDB
            db.messages.update_one(
                {'_id': ObjectId(id)},
                {'$set': {'receiveStatus': 'recibido'}}
            )
        except Exception as e:
            print('Error al procesar el mensaje:', e)
            # Actualizar el estado de recepción en caso de error
            db.messages.update_one(
                {'_id': ObjectId(id)},
                {'$set': {'receiveStatus': 'error'}}
            )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    mongo_client.close()

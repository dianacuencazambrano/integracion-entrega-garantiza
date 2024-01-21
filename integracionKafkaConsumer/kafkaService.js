const { Kafka } = require('kafkajs');
const dotenv = require('dotenv')

dotenv.config()


const kafka = new Kafka({
    clientId: 'integracionKafka',
    brokers: [process.env.KAFKA_BROKER],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: process.env.KAFKA_KEY,
      password: process.env.KAFKA_SECRET
    },
  });

//Crea y devuelve un consumidor de Kafka
function kafkaConsumer() {
  const consumer = kafka.consumer({ groupId: 'integracionKafkaNodeJs' });
  return consumer;
}

module.exports = {
  kafkaConsumer
};
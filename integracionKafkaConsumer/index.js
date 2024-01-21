const express = require('express')
const cors = require('cors')
const bodyParser = require('body-parser')

const { kafkaConsumer } = require("./kafkaService.js")

const mongoose = require('./database.js')
const Message = require('./messageModel.js')

const dotenv = require('dotenv')
dotenv.config()

const app = express()
const port = process.env.PORT

app.use(cors())
app.use(bodyParser.json())

/**Se inicializa el consumidor */
const consumer = kafkaConsumer()

/**Función para consumo de mensajes mensaje */
async function consume() {
  try {
    await consumer.connect()
    await consumer.subscribe({ topic: 'orders', fromBeginning: true })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const msg = JSON.parse(message.value.toString());
          const { id, value } = msg
          console.log({ id, value })

          /**Se actualoza el estatus de recibo */
          await Message.findByIdAndUpdate(id, { receiveStatus: 'recibido' })
        } catch (error) {
          console.error('Error al procesar el mensaje:', error)
          /**Se actualoza el estatus de recibo en caso de error */
          await Message.findByIdAndUpdate(id, { receiveStatus: 'error' })
        }
      },
    })
  } catch (error) {
    console.error('Error Conexión kafka: ', error)
  }
}

/**Se inicia el proyecto */
const startServer = async () => {
  try {
    consume().catch(console.error)
    app.listen(port, () => {
      console.log(`Servidor escuchando en el puerto ${port}`)
    })
  } catch (error) {
    console.error('Error:', error)
  }
}

startServer()
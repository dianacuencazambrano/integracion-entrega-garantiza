const express = require('express')
const cors = require('cors')

const bodyParser = require('body-parser')

const { kafkaProducer } = require("./kafkaService.js")

const mongoose = require('./database.js')
const Message = require('./messageModel.js')

const dotenv = require('dotenv')
dotenv.config()

const app = express()
const port = process.env.PORT

/**Se establece intervalo de envio a 60seg */
const DELIVERY_INTERVAL = 60000;

app.use(cors())
app.use(bodyParser.json())

/**Se inicializa el productor */
const producer = kafkaProducer()

/**Endpoint post de un mensaje */
app.post('/mensaje', async (req, res) => {
  const { frase } = req.body
  const topic = 'orders'
  /**Se guarda en la BD */
  const id = await saveMessage({ value: frase, topic });
  try {
    await producer.connect()
    try {
      console.log(JSON.stringify({ id: id, value: frase }))
      await producer.send({
        topic: 'orders',
        messages: [{ value: JSON.stringify({ id: id, value: frase }) }],
      });

      await Message.findByIdAndUpdate(id, { sendStatus: 'enviado' });

      console.log('Mensaje enviado: ' + frase)
      res.status(200).json({ success: true, message: 'Mensaje enviado: ' + frase })
    
    } catch (error) {
      /**Actualiza en caso de error */
      await Message.findByIdAndUpdate(id, { sendStatus: 'error' });
      console.error(error)
      res.status(500).json({ success: false, error: 'Error al enviar el mensaje' })
    
    } finally{
      await producer.disconnect()
    }
  } catch (error) {
    /**Actualiza en caso de error */
    await Message.findByIdAndUpdate(id, { sendStatus: 'error' });
    console.error(error)
    res.status(500).json({ success: false, error: 'Error en la conexión con kafka' })
  
  }
})

app.post('/mensaje2', async (req, res) => {
  const { frase } = req.body
  const topic = 'orders2'
  /**Se guarda en la BD */
  const id = await saveMessage({ value: frase, topic })
  try {
    await producer.connect()
    try {
      console.log(JSON.stringify({ id: id, value: frase }))
      await producer.send({
        topic: topic,
        messages: [{ value: JSON.stringify({ id: id, value: frase }) }],
      });

      await Message.findByIdAndUpdate(id, { sendStatus: 'enviado' });

      console.log('Mensaje enviado: ' + frase)
      res.status(200).json({ success: true, message: 'Mensaje enviado: ' + frase })
    
    } catch (error) {
      /**Actualiza en caso de error */
      await Message.findByIdAndUpdate(id, { sendStatus: 'error' });
      console.error(error)
      res.status(500).json({ success: false, error: 'Error al enviar el mensaje' })
    
    } finally{
      await producer.disconnect()
    }
  } catch (error) {
    /**Actualiza en caso de error */
    await Message.findByIdAndUpdate(id, { sendStatus: 'error' });
    console.error(error)
    res.status(500).json({ success: false, error: 'Error en la conexión con kafka' })
  
  }
})


/**Metodo para guardar en la BD */
async function saveMessage({ value, topic }) {
  try {
    const newMessage = new Message({ value, topic });
    await newMessage.save()
    console.log('Se guardo: ', newMessage)
    return newMessage._id
  } catch (error) {
    console.error('Error al guardar en la DB:', error)
  }
}

/**Método para el envío de mensajes con status error o pendiente */
async function sendPendingMessages() {
  const pendingMessages = await Message.find({
    $or: [
      { sendStatus: 'pendiente' },
      { sendStatus: 'error' },
    ],
  })

  console.log('Total: ' + pendingMessages.length)
  let i = 0;
  for (const message of pendingMessages) {
    i++;
    console.log('Entra: ' + i)
    try {
      await producer.connect()
      await producer.send({
        topic: message.topic,
        messages: [{ value: JSON.stringify({ id: message._id, value: message.value }) }],
      })

      /**Se actualiza estado registro de envio */
      await Message.findByIdAndUpdate(message._id, {
        $inc: { deliveryAttempts: 1 },
        sendStatus: 'entregado',
        updatedAt: new Date(),
        lastDeliveryAttempt: new Date(),
      })
    } catch (error) {
      /**Se aumenta cantidad de intentos */
      await Message.findByIdAndUpdate(message._id, {
        $inc: { deliveryAttempts: 1 },
        status: 'error',
        updatedAt: new Date(),
        lastDeliveryAttempt: new Date(),
      });
      console.error(`Error al enviar mensaje con ID ${message._id}:`, error)
    } finally {
      await producer.disconnect()
    }
  }
}

/**Envio constante mensajes con error o pendientes */
setInterval(() => {
  console.log('Enviando Registros Pendientes y con Error')
  sendPendingMessages().catch(console.error)
}, DELIVERY_INTERVAL)


/**Se inicia el proyecto */
const startServer = async () => {
  try {
    //await producer.connect()
    app.listen(port, () => {
      console.log(`Servidor escuchando en el puerto ${port}`)
    })
  } catch (error) {
    console.error('Error:', error)
  }
}

startServer()
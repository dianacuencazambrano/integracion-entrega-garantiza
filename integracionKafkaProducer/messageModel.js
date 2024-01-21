const mongoose = require("mongoose");

const messageSchema = new mongoose.Schema(
  {
    value: { type: String, required: true },
    sendStatus: { type: String, default: 'pendiente' }, //pendiente, error, enviado
    receiveStatus: { type: String, default: 'pendiente' }, //pendiente, error, recibido
    deliveryAttempts: { type: Number, default: 0 }, //numero de intentos de entrega
    lastDeliveryAttempt: { type: Date }, //fecha ultimo intento de entrega
    createdAt: { type: Date, default: Date.now }, //creación
    updatedAt: { type: Date }, //última actualización
  }
);

const Message = mongoose.model("Message", messageSchema, 'messages');

module.exports = Message;

const dotenv = require('dotenv');
const mongoose = require('mongoose');

dotenv.config();
// URL de conexión a tu base de datos MongoDB
const url = process.env.DATABASE_URL;

mongoose.connect(url);

const db = mongoose.connection;

db.on('error', (error) => {
  console.error('Error de conexión a MongoDB:', error);
});

db.once('open', () => {
  console.log('Conexión a MongoDB establecida');
});

module.exports = mongoose;
require('dotenv').config();

module.exports = {
  PORT: parseInt(process.env.RELAY_PORT) || 8080,
  BRIDGE_TOKEN: process.env.BRIDGE_TOKEN || 'change_me',
};
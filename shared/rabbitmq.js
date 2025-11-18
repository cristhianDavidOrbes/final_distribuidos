const amqp = require('amqplib');
const { createLogger } = require('./logger');

const logger = createLogger('rabbitmq');

async function createRabbitMqChannel() {
  const url = process.env.RABBITMQ_URL || 'amqp://rabbitmq:5672';
  const connection = await amqp.connect(url);
  connection.on('error', (err) => logger.error('RabbitMQ connection error', err));
  connection.on('close', () => logger.warn('RabbitMQ connection closed'));
  const channel = await connection.createChannel();
  return { connection, channel };
}

module.exports = { createRabbitMqChannel };

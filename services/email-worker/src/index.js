require('dotenv').config();
const {
  NOTIFICATIONS_EXCHANGE,
  EMAIL_WIN_ROUTING_KEY,
  EMAIL_LOSS_ROUTING_KEY,
} = require('../../../shared/constants');
const { createRabbitMqChannel } = require('../../../shared/rabbitmq');
const { createLogger } = require('../../../shared/logger');

const logger = createLogger('email-worker');

async function bootstrap() {
  const { channel } = await createRabbitMqChannel();
  await channel.assertExchange(NOTIFICATIONS_EXCHANGE, 'topic', { durable: true });

  await consumeQueue(channel, 'email_win_queue', EMAIL_WIN_ROUTING_KEY, sendWinEmail);
  await consumeQueue(channel, 'email_loss_queue', EMAIL_LOSS_ROUTING_KEY, sendLossEmail);

  logger.info('Email worker ready');
}

async function consumeQueue(channel, queueName, routingKey, handler) {
  await channel.assertQueue(queueName, { durable: false });
  await channel.bindQueue(queueName, NOTIFICATIONS_EXCHANGE, routingKey);
  channel.consume(
    queueName,
    (msg) => {
      if (!msg) return;
      const payload = JSON.parse(msg.content.toString());
      handler(payload);
      channel.ack(msg);
    },
    { noAck: false }
  );
}

function sendWinEmail({ user_id, winnings, match_id }) {
  logger.info(`EMAIL → ${user_id}: ¡Ganaste ${winnings}!`, { match_id });
}

function sendLossEmail({ user_id, lost, match_id }) {
  logger.info(`EMAIL → ${user_id}: Lo sentimos, perdiste ${lost}`, { match_id });
}

bootstrap().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});

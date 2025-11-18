require('dotenv').config();
const {
  EVENT_TYPES,
  MATCH_EVENTS_TOPIC,
  MATCH_TIMERS_EXCHANGE,
  MATCH_SIMULATION_QUEUE,
  MATCH_SIMULATION_ROUTING_KEY,
} = require('../../../shared/constants');
const { createKafkaClient, buildEvent } = require('../../../shared/kafka');
const { createRabbitMqChannel } = require('../../../shared/rabbitmq');
const { createLogger } = require('../../../shared/logger');

const logger = createLogger('match-simulator');
const kafka = createKafkaClient('match-simulator');
const producer = kafka.producer();

async function bootstrap() {
  await producer.connect();
  const { channel } = await createRabbitMqChannel();
  await channel.assertQueue(MATCH_SIMULATION_QUEUE, { durable: true });
  await channel.assertExchange(MATCH_TIMERS_EXCHANGE, 'x-delayed-message', {
    durable: true,
    arguments: { 'x-delayed-type': 'direct' },
  });
  await channel.bindQueue(
    MATCH_SIMULATION_QUEUE,
    MATCH_TIMERS_EXCHANGE,
    MATCH_SIMULATION_ROUTING_KEY
  );

  logger.info('Waiting for matches to simulate');

  channel.consume(MATCH_SIMULATION_QUEUE, async (msg) => {
    if (!msg) return;
    try {
      const content = JSON.parse(msg.content.toString());
      await handleSimulation(content);
      channel.ack(msg);
    } catch (err) {
      logger.error('Simulation error', err);
      channel.nack(msg, false, false);
    }
  });
}

async function handleSimulation({ match_id, teams = ['A', 'B'] }) {
  const winner = pickWinner(teams);
  const event = buildEvent({
    eventType: EVENT_TYPES.MATCH_FINISHED,
    matchId: match_id,
    payload: { match_id, winner },
  });

  await producer.send({
    topic: MATCH_EVENTS_TOPIC,
    messages: [{ key: match_id, value: JSON.stringify(event) }],
  });

  logger.info('Match finished', { match_id, winner });
}

function pickWinner(teams) {
  if (!Array.isArray(teams) || teams.length === 0) {
    return 'A';
  }
  const index = Math.floor(Math.random() * teams.length);
  return teams[index];
}

bootstrap().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});

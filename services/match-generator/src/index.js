require('dotenv').config();
const { v4: uuidv4 } = require('uuid');
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

const logger = createLogger('match-generator');
const kafka = createKafkaClient('match-generator');
const producer = kafka.producer();


const TEAMS = ['A', 'B', 'C', 'D', 'E', 'F'];
const intervalMs = Number(process.env.MATCH_GENERATE_INTERVAL_MS || 10000);
const matchDurationMs = Number(process.env.MATCH_DURATION_MS || 60000);

async function bootstrap() {
  await producer.connect();
  const { channel } = await createRabbitMqChannel();
  await channel.assertExchange(MATCH_TIMERS_EXCHANGE, 'x-delayed-message', {
    durable: true,
    arguments: { 'x-delayed-type': 'direct' },
  });
  await channel.assertQueue(MATCH_SIMULATION_QUEUE, { durable: true });
  await channel.bindQueue(
    MATCH_SIMULATION_QUEUE,
    MATCH_TIMERS_EXCHANGE,
    MATCH_SIMULATION_ROUTING_KEY
  );

  const tick = async () => {
    const matchId = `m-${uuidv4()}`;
    const teams = pickTeams();
    const payload = { match_id: matchId, teams };

    const event = buildEvent({
      eventType: EVENT_TYPES.MATCH_CREATED,
      matchId,
      payload,
    });

    await producer.send({
      topic: MATCH_EVENTS_TOPIC,
      messages: [{ key: matchId, value: JSON.stringify(event) }],
    });

    const timerPayload = Buffer.from(JSON.stringify({ match_id: matchId, teams }));
    channel.publish(
      MATCH_TIMERS_EXCHANGE,
      MATCH_SIMULATION_ROUTING_KEY,
      timerPayload,
      { headers: { 'x-delay': matchDurationMs } }
    );

    logger.info('Match created', { matchId, teams });
  };

  await tick();
  setInterval(() => tick().catch((err) => logger.error('Tick error', err)), intervalMs);
  logger.info('Match generator started', { intervalMs, matchDurationMs });
}

function pickTeams() {
  const shuffled = [...TEAMS].sort(() => Math.random() - 0.5);
  return shuffled.slice(0, 2);
}

bootstrap().catch((err) => {
  logger.error('Fatal error on bootstrap', err);
  process.exit(1);
});

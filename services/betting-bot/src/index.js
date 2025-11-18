require('dotenv').config();
const { EVENT_TYPES, MATCH_EVENTS_TOPIC } = require('../../../shared/constants');
const { createKafkaClient } = require('../../../shared/kafka');
const { createLogger } = require('../../../shared/logger');

const logger = createLogger('betting-bot');
const kafka = createKafkaClient('betting-bot');
const consumer = kafka.consumer({
  groupId: 'betting-bot-group',
  allowAutoTopicCreation: true,
});

const bettingApiUrl = process.env.BETTING_API_URL || 'http://betting-api:8080/bet';
const botUsers = (process.env.BOT_USERS || 'bot-01,bot-02,bot-03,bot-04')
  .split(',')
  .map((u) => u.trim())
  .filter(Boolean);
if (!botUsers.length) {
  botUsers.push('bot-virtual');
}
const betsPerMatch = Number(process.env.BOT_BETS_PER_MATCH || 3);
const minAmount = Number(process.env.BOT_MIN_AMOUNT || 25);
const maxAmount = Number(process.env.BOT_MAX_AMOUNT || 150);
const maxDelayMs = Number(process.env.BOT_MAX_DELAY_MS || 25000);

const scheduledBets = new Map();

async function bootstrap() {
  await consumer.connect();
  await consumer.subscribe({ topic: MATCH_EVENTS_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      await handleEvent(event);
    },
  });

  logger.info('Betting bot ready to simulate users', {
    botUsers,
    betsPerMatch,
    minAmount,
    maxAmount,
  });
}

async function handleEvent(event) {
  const { event_type: type, match_id: matchId, payload = {} } = event;
  switch (type) {
    case EVENT_TYPES.MATCH_CREATED:
      scheduleBets(matchId, payload.teams || ['A', 'B']);
      break;
    case EVENT_TYPES.MATCH_FINISHED:
      clearScheduledBets(matchId);
      break;
    default:
      break;
  }
}

function scheduleBets(matchId, teams) {
  clearScheduledBets(matchId);
  const timeouts = [];
  for (let i = 0; i < betsPerMatch; i += 1) {
    const delay = Math.floor(Math.random() * maxDelayMs);
    const timeout = setTimeout(() => placeBet(matchId, teams), delay);
    timeouts.push(timeout);
  }
  scheduledBets.set(matchId, timeouts);
  logger.info('Scheduled bot bets', { matchId, bets: timeouts.length });
}

function clearScheduledBets(matchId) {
  const timers = scheduledBets.get(matchId) || [];
  timers.forEach((timer) => clearTimeout(timer));
  scheduledBets.delete(matchId);
}

async function placeBet(matchId, teams) {
  const user_id = botUsers[Math.floor(Math.random() * botUsers.length)];
  const bet_on = teams[Math.floor(Math.random() * teams.length)];
  const amount = Number((Math.random() * (maxAmount - minAmount) + minAmount).toFixed(2));

  try {
    const response = await fetch(bettingApiUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_id, match_id: matchId, bet_on, amount }),
    });

    if (!response.ok) {
      const error = await response.text();
      logger.warn('Bot bet rejected', { matchId, user_id, status: response.status, error });
      return;
    }

    logger.info('Bot bet placed', { matchId, user_id, bet_on, amount });
  } catch (err) {
    logger.error('Bot bet failed', { matchId, error: err.message });
  }
}

function shutdown() {
  logger.info('Shutting down betting bot');
  scheduledBets.forEach((timers) => timers.forEach((timer) => clearTimeout(timer)));
  consumer
    .disconnect()
    .catch((err) => logger.error('Error during bot shutdown', { error: err.message }))
    .finally(() => process.exit(0));
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

bootstrap().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});

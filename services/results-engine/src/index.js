require('dotenv').config();
const {
  MATCH_EVENTS_TOPIC,
  EVENT_TYPES,
  NOTIFICATIONS_EXCHANGE,
  DASHBOARD_ROUTING_KEY,
  USER_BALANCE_ROUTING_KEY,
  EMAIL_WIN_ROUTING_KEY,
  EMAIL_LOSS_ROUTING_KEY,
} = require('../../../shared/constants');
const { createKafkaClient } = require('../../../shared/kafka');
const { createRabbitMqChannel } = require('../../../shared/rabbitmq');
const { createLogger } = require('../../../shared/logger');

const logger = createLogger('results-engine');
const kafka = createKafkaClient('results-engine');
const consumer = kafka.consumer({
  groupId: 'results-engine-group',
  allowAutoTopicCreation: true,
});

const defaultUserBalance = Number(process.env.DEFAULT_USER_BALANCE || 1000);
const matchState = new Map();
const userState = new Map();

async function bootstrap() {
  await ensureTopicExists();
  const { channel } = await createRabbitMqChannel();
  await channel.assertExchange(NOTIFICATIONS_EXCHANGE, 'topic', { durable: true });

  await consumer.connect();
  await consumer.subscribe({ topic: MATCH_EVENTS_TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      await handleEvent(event, channel);
    },
  });

  logger.info('Results engine running and consuming Kafka events');
}

async function handleEvent(event, channel) {
  const { event_type: type, match_id: matchId, payload = {} } = event;
  switch (type) {
    case EVENT_TYPES.MATCH_CREATED:
      registerMatch(matchId, payload);
      await publishMatchUpdate(channel, matchId);
      break;
    case EVENT_TYPES.BET_PLACED:
      await handleBetPlaced(matchId, payload, channel);
      break;
    case EVENT_TYPES.MATCH_FINISHED:
      await handleMatchFinished(matchId, payload, channel);
      break;
    default:
      logger.warn('Unknown event type', { type });
  }
}

function registerMatch(matchId, payload) {
  if (matchState.has(matchId)) {
    return matchState.get(matchId);
  }
  const teams = Array.isArray(payload.teams) && payload.teams.length ? payload.teams : ['A', 'B'];
  const match = {
    match_id: matchId,
    teams,
    status: 'OPEN',
    bets: [],
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
  matchState.set(matchId, match);
  return match;
}

async function handleBetPlaced(matchId, payload, channel) {
  const record = matchState.get(matchId);
  if (!record || record.status !== 'OPEN') {
    logger.warn('Ignoring bet for closed or missing match', { matchId });
    return;
  }

  const amount = Number(payload.amount);
  if (Number.isNaN(amount) || amount <= 0) {
    logger.warn('Ignoring invalid bet amount', { matchId, amount: payload.amount });
    return;
  }
  const bet = { ...payload, amount };
  record.bets.push(bet);
  record.updated_at = new Date().toISOString();

  const user = ensureUser(payload.user_id);
  user.bets_placed += 1;
  user.pending_bets += 1;
  user.total_wagered += amount;
  user.last_updated = new Date().toISOString();

  logger.info('Bet accepted', { matchId, bet_id: payload.bet_id });
  await publishMatchUpdate(channel, matchId);
  await publishUserUpdate(channel, user);
}

async function handleMatchFinished(matchId, payload, channel) {
  const record = matchState.get(matchId) || createFallbackMatch(matchId);

  record.status = 'CLOSED';
  record.winner = payload.winner;
  record.closed_at = new Date().toISOString();
  record.updated_at = record.closed_at;

  const betsToSettle = [...record.bets];
  record.lastMetrics = buildMetrics(betsToSettle, record.teams);
  record.bets = [];
  matchState.set(matchId, record);

  await publishMatchUpdate(channel, matchId);

  for (const bet of betsToSettle) {
    const user = ensureUser(bet.user_id);
    const amount = Number(bet.amount);
    user.pending_bets = Math.max(0, user.pending_bets - 1);
    user.last_updated = new Date().toISOString();

    if (bet.bet_on === payload.winner) {
      user.balance += amount;
      user.wins += 1;
      user.total_returned += amount;
      await channel.publish(
        NOTIFICATIONS_EXCHANGE,
        EMAIL_WIN_ROUTING_KEY,
        Buffer.from(
          JSON.stringify({
            user_id: bet.user_id,
            match_id: matchId,
            bet_id: bet.bet_id,
            winnings: amount,
          })
        )
      );
    } else {
      user.balance -= amount;
      user.losses += 1;
      user.total_returned -= amount;
      await channel.publish(
        NOTIFICATIONS_EXCHANGE,
        EMAIL_LOSS_ROUTING_KEY,
        Buffer.from(
          JSON.stringify({
            user_id: bet.user_id,
            match_id: matchId,
            bet_id: bet.bet_id,
            lost: amount,
          })
        )
      );
    }

    await publishUserUpdate(channel, user);
  }

  logger.info('Match settled', { matchId, winner: payload.winner });
}

function createFallbackMatch(matchId) {
  const fallback = {
    match_id: matchId,
    teams: ['A', 'B'],
    status: 'CLOSED',
    bets: [],
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
  matchState.set(matchId, fallback);
  return fallback;
}

async function publishMatchUpdate(channel, matchId) {
  const record = matchState.get(matchId);
  if (!record) return;
  const metrics = getMatchMetrics(record);

  const message = {
    type: 'MATCH',
    payload: {
      match_id: matchId,
      status: record.status,
      teams: record.teams,
      winner: record.winner,
      total_bets: metrics.totalBets,
      total_pool: metrics.totalPool,
      per_team: metrics.perTeam,
      created_at: record.created_at,
      last_update: record.updated_at,
      closed_at: record.closed_at,
    },
  };

  await channel.publish(
    NOTIFICATIONS_EXCHANGE,
    DASHBOARD_ROUTING_KEY,
    Buffer.from(JSON.stringify(message))
  );
}

async function publishUserUpdate(channel, user) {
  const message = {
    type: 'USER',
    payload: { ...user },
  };

  await channel.publish(
    NOTIFICATIONS_EXCHANGE,
    USER_BALANCE_ROUTING_KEY,
    Buffer.from(JSON.stringify(message))
  );
}

function ensureUser(userId) {
  if (!userState.has(userId)) {
    userState.set(userId, {
      user_id: userId,
      starting_balance: defaultUserBalance,
      balance: defaultUserBalance,
      bets_placed: 0,
      wins: 0,
      losses: 0,
      total_wagered: 0,
      total_returned: 0,
      pending_bets: 0,
      last_updated: new Date().toISOString(),
    });
  }
  return userState.get(userId);
}

function getMatchMetrics(record) {
  if (record.status === 'CLOSED' && record.lastMetrics) {
    return record.lastMetrics;
  }
  return buildMetrics(record.bets, record.teams);
}

function buildMetrics(bets = [], teams = []) {
  const perTeam = {};
  teams.forEach((team) => {
    perTeam[team] = { amount: 0, count: 0 };
  });

  let totalPool = 0;
  bets.forEach((bet) => {
    const amount = Number(bet.amount) || 0;
    totalPool += amount;
    if (!perTeam[bet.bet_on]) {
      perTeam[bet.bet_on] = { amount: 0, count: 0 };
    }
    perTeam[bet.bet_on].amount += amount;
    perTeam[bet.bet_on].count += 1;
  });

  return {
    totalPool,
    totalBets: bets.length,
    perTeam,
  };
}

async function ensureTopicExists() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: MATCH_EVENTS_TOPIC, numPartitions: 0, replicationFactor: 1 }],
    });
  } catch (err) {
    if (err?.type !== 'TOPIC_ALREADY_EXISTS') {
      throw err;
    }
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

bootstrap().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});

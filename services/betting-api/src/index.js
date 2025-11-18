require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const { EVENT_TYPES, MATCH_EVENTS_TOPIC } = require('../../../shared/constants');
const { createKafkaClient, buildEvent } = require('../../../shared/kafka');
const { createLogger } = require('../../../shared/logger');

const logger = createLogger('betting-api');
const app = express();
const port = Number(process.env.PORT || 8080);

const kafka = createKafkaClient('betting-api');
const producer = kafka.producer();

app.use(express.json());
app.use(cors());

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.post('/bet', async (req, res) => {
  const { user_id, match_id, bet_on, amount } = req.body || {};
  if (!user_id || !match_id || !bet_on || amount === undefined) {
    return res.status(400).json({ error: 'user_id, match_id, bet_on, amount are required' });
  }

  const parsedAmount = Number(amount);
  if (Number.isNaN(parsedAmount) || parsedAmount <= 0) {
    return res.status(400).json({ error: 'amount must be a positive number' });
  }

  const betId = uuidv4();

  const event = buildEvent({
    eventType: EVENT_TYPES.BET_PLACED,
    matchId: match_id,
    payload: { bet_id: betId, user_id, bet_on, amount: parsedAmount },
  });

  await producer.send({
    topic: MATCH_EVENTS_TOPIC,
    messages: [{ key: match_id, value: JSON.stringify(event) }],
  });

  logger.info('Bet placed', { betId, match_id });
  return res.status(202).json({ bet_id: betId, status: 'accepted' });
});

async function bootstrap() {
  await producer.connect();
  app.listen(port, () => logger.info(`Betting API listening on ${port}`));
}

bootstrap().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});

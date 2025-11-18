require('dotenv').config();
const path = require('path');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { createRabbitMqChannel } = require('../../../shared/rabbitmq');
const { NOTIFICATIONS_EXCHANGE } = require('../../../shared/constants');
const { createLogger } = require('../../../shared/logger');

const logger = createLogger('dashboard-backend');
const app = express();
const port = Number(process.env.PORT || 3000);
const bettingApiUrl = process.env.BETTING_API_URL || 'http://betting-api:8080/bet';

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const matchState = new Map();
const userState = new Map();

app.use(express.json());

app.get('/api/state', (_req, res) => {
  res.json({
    matches: Array.from(matchState.values()),
    users: Array.from(userState.values()),
  });
});

app.post('/api/bet', async (req, res) => {
  try {
    const response = await fetch(bettingApiUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    const text = await response.text();
    let data;
    try {
      data = text ? JSON.parse(text) : {};
    } catch (parseErr) {
      data = { raw: text };
    }
    return res.status(response.status).json(data);
  } catch (err) {
    logger.error('Error proxying bet', { error: err.message });
    return res.status(500).json({ error: 'Betting service unavailable' });
  }
});

app.use(express.static(path.join(__dirname, '..', 'public')));

wss.on('connection', (socket) => {
  logger.info('WebSocket client connected');
  socket.send(JSON.stringify({ type: 'INIT', payload: buildStatePayload() }));
  socket.send(JSON.stringify({ type: 'SYSTEM', message: 'Connected a feed en vivo' }));
});

async function bootstrap() {
  const { channel } = await createRabbitMqChannel();
  await channel.assertExchange(NOTIFICATIONS_EXCHANGE, 'topic', { durable: true });
  const queue = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(queue.queue, NOTIFICATIONS_EXCHANGE, 'notify.dashboard.*');

  channel.consume(
    queue.queue,
    (msg) => {
      if (!msg) return;
      try {
        const content = JSON.parse(msg.content.toString());
        processNotification(content);
      } catch (err) {
        logger.error('Invalid dashboard payload', { error: err.message });
      }
    },
    { noAck: true }
  );

  server.listen(port, () => logger.info(`Dashboard running on ${port}`));
}

function processNotification(message) {
  if (!message || !message.type) {
    return;
  }

  if (message.type === 'MATCH' && message.payload?.match_id) {
    matchState.set(message.payload.match_id, message.payload);
  }
  if (message.type === 'USER' && message.payload?.user_id) {
    userState.set(message.payload.user_id, message.payload);
  }

  broadcast(JSON.stringify(message));
}

function broadcast(message) {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

function buildStatePayload() {
  return {
    matches: Array.from(matchState.values()),
    users: Array.from(userState.values()),
  };
}

bootstrap().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});

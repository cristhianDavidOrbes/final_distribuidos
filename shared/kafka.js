const { Kafka, logLevel } = require('kafkajs');

function createKafkaClient(serviceName = 'service') {
  const brokers = (process.env.KAFKA_BROKERS || 'kafka:9092')
    .split(',')
    .map((b) => b.trim())
    .filter(Boolean);

  return new Kafka({
    clientId: serviceName,
    brokers,
    logLevel: logLevel.INFO,
  });
}

function buildEvent({ eventType, matchId, payload = {} }) {
  return {
    event_id: `${eventType}-${matchId}-${Date.now()}`,
    event_type: eventType,
    match_id: matchId,
    payload,
    occurred_at: new Date().toISOString(),
  };
}

module.exports = {
  createKafkaClient,
  buildEvent,
};

FROM rabbitmq:3.12-management

RUN set -eux; \
    apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*; \
    curl -L -o /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-3.12.0.ez \
      https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v3.12.0/rabbitmq_delayed_message_exchange-3.12.0.ez; \
    rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange

FROM node:20-alpine
WORKDIR /app

COPY shared /app/shared
RUN npm install --production --prefix shared

COPY services/betting-bot /app/services/betting-bot
RUN npm install --production --prefix services/betting-bot

WORKDIR /app/services/betting-bot
CMD ["npm", "start"]

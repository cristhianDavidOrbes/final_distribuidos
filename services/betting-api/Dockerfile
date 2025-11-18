FROM node:20-alpine
WORKDIR /app

COPY shared /app/shared
RUN npm install --production --prefix shared

COPY services/betting-api /app/services/betting-api
RUN npm install --production --prefix services/betting-api

WORKDIR /app/services/betting-api
CMD ["npm", "start"]

FROM node:20-alpine
WORKDIR /app

COPY shared /app/shared
RUN npm install --production --prefix shared

COPY services/match-simulator /app/services/match-simulator
RUN npm install --production --prefix services/match-simulator

WORKDIR /app/services/match-simulator
CMD ["npm", "start"]

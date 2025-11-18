FROM node:20-alpine
WORKDIR /app

COPY shared /app/shared
RUN npm install --production --prefix shared

COPY services/results-engine /app/services/results-engine
RUN npm install --production --prefix services/results-engine

WORKDIR /app/services/results-engine
CMD ["npm", "start"]

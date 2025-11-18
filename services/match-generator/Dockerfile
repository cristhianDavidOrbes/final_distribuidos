FROM node:20-alpine
WORKDIR /app

COPY shared /app/shared
RUN npm install --production --prefix shared

COPY services/match-generator /app/services/match-generator
RUN npm install --production --prefix services/match-generator

WORKDIR /app/services/match-generator
CMD ["npm", "start"]

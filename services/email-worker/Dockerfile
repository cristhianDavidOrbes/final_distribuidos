FROM node:20-alpine
WORKDIR /app

COPY shared /app/shared
RUN npm install --production --prefix shared

COPY services/email-worker /app/services/email-worker
RUN npm install --production --prefix services/email-worker

WORKDIR /app/services/email-worker
CMD ["npm", "start"]

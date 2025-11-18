FROM node:20-alpine
WORKDIR /app

COPY shared /app/shared
RUN npm install --production --prefix shared

COPY services/dashboard-backend /app/services/dashboard-backend
RUN npm install --production --prefix services/dashboard-backend

WORKDIR /app/services/dashboard-backend
CMD ["npm", "start"]

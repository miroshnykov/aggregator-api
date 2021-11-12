FROM node:14.17-alpine3.14

RUN apk add --no-cache redis curl

WORKDIR /home/app

COPY . .

RUN npm install && npm run build && npm prune --production && npm install --production

EXPOSE 80

ENTRYPOINT redis-server --daemonize yes && \
        npm run start

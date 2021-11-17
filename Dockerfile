FROM node:14.17-alpine3.14

RUN apk add --no-cache redis

WORKDIR /home/app

COPY . .

# Install modules, build and remove unnecessary modules after
RUN npm install \
    && npm run build \
    && npm prune --production \
    && npm install --production \
    && rm -rf src \
    && rm -f .npmrc \
    && rm -rf /usr/local/lib/node_modules/npm/ /usr/local/bin/npm

EXPOSE 80

ENTRYPOINT redis-server --daemonize yes && \
        node -r dotenv/config ./dist/server.js dotenv_config_path=/run/secrets/environment

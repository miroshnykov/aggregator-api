FROM node:16.13.1-alpine3.14 AS builder

WORKDIR /app

COPY . .

# Install modules, build and remove unnecessary modules after
RUN npm install \
    && npm run build \
    && npm prune --production \
    && npm install --production \
    && rm -rf src \
    && rm -f .npmrc

FROM node:16.13.1-alpine3.14

RUN rm -rf /usr/local/lib/node_modules/npm/ /usr/local/bin/npm

WORKDIR /app

COPY --from=builder /app .

EXPOSE 1089 2089

ENTRYPOINT ["node", "-r", "dotenv/config", "./dist/server.js", "dotenv_config_path=/run/secrets/environment"]

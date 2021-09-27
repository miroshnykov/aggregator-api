FROM ubuntu:20.04
ARG node_version=14.17.6

#COPY tests/run_docker_tests.sh /usr/local/bin/run_docker_tests.sh
RUN rm -rf /var/lib/apt/lists/*

RUN apt-get update
RUN apt-get install \
    build-essential \
    apt-transport-https \
    lsb-release \
    ca-certificates \
    curl \
    wget \
    python -y \
    redis-server

RUN apt-get install --yes git

RUN git config --global http.sslVerify "false"

RUN curl -fsSL https://deb.nodesource.com/setup_14.x | bash -
RUN apt-get install --yes nodejs


RUN mkdir /tmp/co-recipe
RUN chmod 755 /tmp

WORKDIR /home/app

COPY . .
RUN npm install

RUN cp /var/run/secrets/environment /home/app/.env || echo "Coldn't copy env from /run/secrets";
RUN npm run build

EXPOSE 9002


# Required to push into different branchs.
ARG branch
ENV BRANCH=${branch}

ENTRYPOINT redis-server --daemonize yes && if [ "$BRANCH" = "stage1" ] ; then \
        npm run stage1 ; \
    elif [ "$BRANCH" = "stage2" ] ; then \
        npm run stage2 ; \
    elif [ "$BRANCH" = "dev" ] ; then \
        npm run dev ; \
    else \
        npm run prod ; \
    fi
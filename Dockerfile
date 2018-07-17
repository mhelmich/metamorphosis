#
# Copyright 2018 Marco Helmich
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# the builder container
# I can be as wasteful as I like in here
# none of that will carry over into the real container
# move to leaner alpine image for building as well
# that way I'm building and running the on the same distro
FROM golang:1.10.3-alpine3.7 as builder
# set builder workdir inside of GOPATH
WORKDIR /go/src/github.com/mhelmich/metamorphosis
# install build dependencies
RUN apk -vvv --no-cache update \
    && apk -vvv --no-cache upgrade \
    && apk -vvv --no-cache add git \
    && apk -vvv --no-cache add build-base \
    && apk -vvv --no-cache add curl \
    && rm -rf /var/cache/apk/*
# get and build the sources
RUN git clone https://github.com/mhelmich/metamorphosis.git . \
    && curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh \
    && dep ensure -v \
    && echo 'Building binary...' \
    && go build -a

# the runtime container
# now it's getting interesting!!!
# the file size actually matters and I only try to take with me what I need
FROM alpine:3.7
RUN apk -vvv --no-cache update \
    && apk -vvv --no-cache upgrade \
    && apk -vvv --no-cache add ca-certificates \
    && rm -rf /var/cache/apk/*

ENV WRK_DIR=/metamorphosis/ \
    METAMORPHOSIS_USER=franzkafka
WORKDIR ${WRK_DIR}

# copy over the binary and the necessary config file
COPY --from=builder /go/src/github.com/mhelmich/metamorphosis/metamorphosis .
COPY default_config.yaml ./

# set up nsswitch.conf for Go's "netgo" implementation
# - https://github.com/golang/go/blob/go1.9.1/src/net/conf.go#L194-L275
# - docker run --rm debian:stretch grep '^hosts:' /etc/nsswitch.conf
RUN [ ! -e /etc/nsswitch.conf ] && echo 'hosts: files dns' > /etc/nsswitch.conf \
# add metamorphosis user and group
    && adduser -S -u 7447 ${METAMORPHOSIS_USER} \
    && addgroup -S -g 7447 ${METAMORPHOSIS_USER} \
    && addgroup ${METAMORPHOSIS_USER} ${METAMORPHOSIS_USER} \
# set up workdir
    && chown -R ${METAMORPHOSIS_USER}:${METAMORPHOSIS_USER} ${WRK_DIR} \
    && chmod -R 700 ${WRK_DIR}
USER ${METAMORPHOSIS_USER}

# expose all ports out of the default config file
EXPOSE 8080 8081 16000 16001
# set our entry point on alpines shell
ENTRYPOINT ["/bin/sh", "-c", "/metamorphosis/metamorphosis -config /metamorphosis/default_config.yaml"]

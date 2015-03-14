FROM ollin/vertx
MAINTAINER Andreas Gerstmayr <andreas@andreasgerstmayr.at>

COPY . /src
RUN vertx install io.vertx~lang-jython~2.1.1

EXPOSE  6000
WORKDIR /src
CMD ["vertx", "runmod", "at.andreasgerstmayr~simple-kv-store~1.0", "-conf", "conf.json"]

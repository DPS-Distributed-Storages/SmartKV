FROM gradle:7.6.1-jdk11-alpine AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle -p node assemble --no-daemon

FROM eclipse-temurin:11-alpine

RUN mkdir /app
RUN mkdir /conf

COPY --from=build /home/gradle/src/node/build/libs/*-all.jar /app/dml.jar
COPY ./conf/config.json ./conf/config.json

ENV JAVA_OPTS=""
ENV JGROUPS_CONFIG="default-configs/default-jgroups-tcp.xml"

EXPOSE 9000
EXPOSE 9001

ENTRYPOINT exec java $JAVA_OPTS -Djava.net.preferIPv4Stack=true -Dvertx.jgroups.config=$JGROUPS_CONFIG -jar /app/dml.jar

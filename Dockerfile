FROM gradle:8.5.0-jdk21 AS build

COPY --chown=gradle:gradle . /home/gradle/src

WORKDIR /home/gradle/src

RUN gradle kiwi-server:clean kiwi-server:assembleDist kiwi-server:installDist --no-daemon

FROM eclipse-temurin:21-alpine

EXPOSE 6379

WORKDIR /app

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD nc -z localhost 6379 || exit 1

COPY --from=build /home/gradle/src/kiwi-server/build/install/kiwi-server/ /app/kiwi-server/

CMD ["/app/kiwi-server/bin/kiwi-server"]

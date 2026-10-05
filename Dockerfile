# syntax=docker/dockerfile:1
# One build stage for both services; pick the runtime with --target (Compose does).
FROM maven:3.9.11-eclipse-temurin-25 AS build
WORKDIR /src
COPY pom.xml ./
COPY common/pom.xml common/
COPY payment-service/pom.xml payment-service/
COPY ledger-service/pom.xml ledger-service/
COPY e2e-tests/pom.xml e2e-tests/
COPY common/src common/src
COPY payment-service/src payment-service/src
COPY ledger-service/src ledger-service/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -pl payment-service,ledger-service -am package -DskipTests

FROM eclipse-temurin:25-jre AS payment-service
COPY --from=build /src/payment-service/target/payment-service-*-exec.jar /app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app.jar"]

FROM eclipse-temurin:25-jre AS ledger-service
COPY --from=build /src/ledger-service/target/ledger-service-*-exec.jar /app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "/app.jar"]

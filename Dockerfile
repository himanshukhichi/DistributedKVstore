FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/target/distkv-0.1.0-SNAPSHOT.jar /app/distkv.jar
EXPOSE 50051 9100
ENTRYPOINT ["java", "-jar", "/app/distkv.jar"]

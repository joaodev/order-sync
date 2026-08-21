# ---- Stage 1: build ----
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace

COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .
RUN ./mvnw dependency:go-offline -B

COPY src src
RUN ./mvnw clean package -DskipTests -B

# ---- Stage 2: runtime ----
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN addgroup --system spring && adduser --system --ingroup spring spring

COPY --from=build /workspace/target/order-sync-*.jar app.jar
RUN chown -R spring:spring /app

USER spring:spring

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
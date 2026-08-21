# Order Sync

A backend system that keeps a modern data store synchronized with a legacy ERP (TOTVS/Progress-style) database, with full versioning and audit trail — built as a hands-on study project covering the technology stack used in enterprise integration roles.

## The Problem

Legacy ERPs often can't be modified or exposed through an API. Sales orders are written directly into an Oracle database by the ERP itself. This project simulates that scenario: it captures changes from a "legacy" `LEGACY_ORDERS` table without touching the ERP, processes them through an event-driven pipeline, and maintains a fully versioned, auditable copy for downstream consumers (dashboards, order-tracking apps, reporting).

## Why This Project

This is a portfolio project built to reinforce, hands-on, the responsibilities of a backend/integration role working with:

- Java microservices (Java 21, Spring Boot 4.x)
- Oracle database integration and advanced SQL
- Change Data Capture (CDC) from a legacy system into Kafka
- ETL pipelines with Spring Batch
- File system monitoring for batch/legacy file ingestion
- Data versioning strategy: snapshots, deltas, and audit trails
- Containerized, orchestrated deployment (Docker, Kubernetes)
- DevOps pipeline and observability

## Architecture Overview

```
LEGACY_ORDERS (Oracle)
      │
      │  redo log
      ▼
  Debezium ──────► Kafka topic: orders.cdc
                          │
                          ▼
                  Spring Kafka Consumer
                  (idempotency via Redis)
                          │
                          ▼
                  Spring Batch ETL job ◄───── File System Watcher
                  (validate, normalize)        (CSV batch files from
                          │                      branches without CDC)
                          ▼
              ┌───────────────────────┐
              │   Order (current)     │
              │   OrderSnapshot       │
              │   OrderDelta          │
              │   AuditTrail          │
              └───────────────────────┘
                          │
                          ▼
                REST API (Spring Web)
                secured via Keycloak (OAuth2/JWT)
                          │
                          ▼
              n8n workflows (reconciliation,
              alerts, scheduled jobs)
```

Metrics are exposed via Spring Boot Actuator + Micrometer, scraped by Prometheus, and visualized in Grafana. The whole stack runs locally via Docker Compose, and later on Kubernetes (Minikube/Kind) for the orchestration phase.

## Running Locally

There are two ways to run the application: from your IDE/Maven (useful while developing), or fully containerized (closer to how it would run in a real deployment).

### Option A: Application on host, infrastructure in Docker

1. Copy `.env.example` to `.env` (defaults work fine for local development).
2. Start the infrastructure:
   ```
   docker compose up -d
   ```
3. Wait for the Oracle container to report `healthy`:
   ```
   docker compose ps
   ```
4. Run the application from IntelliJ (`OrderSyncApplication`), or via Maven:
   ```
   ./mvnw spring-boot:run
   ```

The `.env` file is loaded automatically by the application at startup (via `spring-dotenv`) — no manual environment variable setup needed in your IDE or shell.

On startup, Flyway automatically applies the schema migrations against the Oracle instance running in Docker.

### Option B: Everything in Docker, including the application

The application itself can also run as a container, using the `docker` Spring profile (`application-docker.yaml`), which points at the other services by their Docker Compose service names (`oracle-xe`, `kafka`, `redis`, `keycloak`) instead of `localhost`.

1. Copy `.env.example` to `.env` and fill in the real values (the `app` container reads these as environment variables, not via `spring-dotenv`, since `spring-dotenv` is a development-only dependency not included in the container image).
2. Build and start everything, including the application:
   ```
   docker compose up -d --build
   ```
   Or, to (re)build and start only the application service after infrastructure is already running:
   ```
   docker compose build app
   docker compose up -d app
   ```
3. Check it started correctly:
   ```
   docker compose logs app
   ```
   Look for `Started OrderSyncApplication`, a Flyway line confirming migrations were applied against `oracle-xe`, and no connection errors for Kafka, Redis, or Keycloak.
4. Verify it's reachable from the host:
   ```
   curl http://localhost:8080/actuator/health
   ```

The container build uses a multi-stage `Dockerfile`: the app is compiled inside a `jdk` image, then copied into a much smaller `jre`-only runtime image, running as a non-root user.

**File imports while containerized:** the `./import` folder is bind-mounted into the container, so dropping a CSV into `./import/incoming` on your host still triggers the file watcher exactly as described in [Testing the File Import](#testing-the-file-import-spring-batch--file-system-watcher).

**Getting a token from inside the container** (useful for debugging network issues without leaving the Docker network):

```bash
docker compose exec app sh -c '
TOKEN=$(curl -s -X POST http://keycloak:8080/realms/order-sync/protocol/openid-connect/token \
  -d "grant_type=password" \
  -d "client_id=order-sync-api" \
  -d "client_secret=$KEYCLOAK_CLIENT_SECRET" \
  -d "username=testuser" \
  -d "password=testpassword" | sed -n "s/.*\"access_token\":\"\([^\"]*\)\".*/\1/p")

curl -i http://localhost:8080/api/orders -H "Authorization: Bearer $TOKEN"
'
```

Note the container uses `keycloak:8080` (the internal Docker network address and port), not `localhost:8081` (the host-mapped port used everywhere else in this README).

## Setting Up CDC (Debezium + Oracle)

By default, Oracle doesn't generate redo log data in a format Debezium can read — this requires enabling `ARCHIVELOG` mode and supplemental logging, plus a dedicated low-privilege user for the connector. This is a one-time setup per environment (already applied to the `oracle-xe` container in this project, but documented here for reproducibility).

### 1. Enable ARCHIVELOG mode and supplemental logging

Connect to the Oracle container as `sysdba`:

```bash
docker exec -it order-sync-oracle sqlplus / as sysdba
```

Then run:

```sql
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

Verify:

```sql
ARCHIVE LOG LIST;
-- Database log mode should be "Archive Mode"

SELECT supplemental_log_data_min, supplemental_log_data_all FROM v$database;
-- Both columns should show YES (or IMPLICIT / YES)
```

### 2. Create a dedicated Debezium user

Still connected as `sysdba`:

```sql
CREATE USER c##dbzuser IDENTIFIED BY dbz_password
   DEFAULT TABLESPACE USERS
   QUOTA UNLIMITED ON USERS
   CONTAINER=ALL;

GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;

GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;

GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;

GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER=ALL;
```

> **Why `c##` prefix?** Oracle's multitenant architecture (CDB + PDB) requires common users — ones that need to exist across all pluggable databases — to be prefixed with `c##`. This is an Oracle naming convention, not a project choice.

### 3. Register the Debezium connector

With `kafka-connect` running (`docker compose up -d kafka-connect`), register the connector using the config in `debezium-oracle-connector.json`:

```bash
curl -i -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-oracle-connector.json
```

Check its status:

```bash
curl http://localhost:8083/connectors/order-sync-oracle-connector/status
```

Both `connector.state` and the task's `state` should show `RUNNING`.

### 4. Verify CDC is working

Insert a row directly into the legacy table:

```bash
docker exec -it order-sync-oracle sqlplus order_sync/order_sync_pw@localhost:1521/XEPDB1
```

```sql
INSERT INTO legacy_orders (customer_name, product_code, quantity, unit_price, status)
VALUES ('CDC Test Customer', 'SKU-CDC', 2, 33.50, 'PENDING');
COMMIT;
```

In another terminal, consume the CDC topic:

```bash
docker exec -it order-sync-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ordersync.ORDER_SYNC.LEGACY_ORDERS \
  --from-beginning
```

A JSON event representing the insert should appear, without any manual API call.

## Testing the File Import (Spring Batch + File System Watcher)

Branches without CDC access send order data as CSV files instead. The application watches a folder for new `.csv` files and imports them automatically through the same `OrderVersioningService` used by the REST API and the CDC pipeline — no manual trigger needed.

### How it works

1. A `FileSystemWatcherService` monitors `./import/incoming` using Java's native `WatchService`.
2. When a `.csv` file appears, it waits until the file finishes being written (checks that its size has stopped changing), then launches a Spring Batch job.
3. The job reads the file in chunks of 5 rows, validates each row, and creates or updates orders exactly like the REST API and CDC consumer do.
4. On success, the file is moved to `./import/processed`. On failure, it's moved to `./import/error` for inspection.

### Directory structure

These folders are created automatically on startup if they don't exist:

```
import/
  incoming/   <- drop new CSV files here
  processed/  <- successfully imported files land here
  error/      <- failed files land here
```

### CSV format

Header row required, columns in this exact order:

```
legacyOrderId,customerName,productCode,quantity,unitPrice,status
```

Example (`sample-orders.csv`, included in the project root):

```csv
legacyOrderId,customerName,productCode,quantity,unitPrice,status
5001,Batch Test Corp,SKU-BATCH,3,45.00,PENDING
5002,File Import Ltd,SKU-FILE,7,12.50,CONFIRMED
5003,Warehouse Direct,SKU-WHS,1,299.99,PENDING
```

### Try it

1. Make sure the application is running (`OrderSyncApplication`).
2. Copy `sample-orders.csv` into `./import/incoming/`:
   ```bash
   cp sample-orders.csv import/incoming/
   ```
3. Watch the application logs — within a second or two you should see the batch job start, followed by `Hibernate: insert into orders...` for each new row.
4. Confirm the file moved to `./import/processed/`.
5. Confirm the orders were imported:
   ```bash
   curl http://localhost:8080/api/orders
   ```
   Orders `5001`, `5002`, and `5003` should appear, each with `currentVersion: 1` and a corresponding snapshot and audit trail entry (`source: FILE_WATCHER`).
6. Drop the same file again (or a modified copy with the same `legacyOrderId`s) to see the update path: existing orders get a new snapshot, delta, and `currentVersion` increments.

### Troubleshooting

- **Nothing happens after dropping the file** — check the file actually has a `.csv` extension; other file types are ignored. Also confirm the app log shows `File system watcher started, monitoring: .../import/incoming` at startup.
- **File lands in `error/`** — check the application logs for the exception; the most common cause is a malformed row (wrong column count, non-numeric quantity/price, or empty required fields).

## Setting Up Keycloak

All REST endpoints require a valid JWT. This is a one-time setup per environment (already applied to the `keycloak` container in this project, but documented here for reproducibility).

### 1. Create the realm

1. Open `http://localhost:8081` and log in (`admin` / `admin`, or whatever you set in `.env`).
2. Click the realm dropdown (top left, shows `master`) → **Create Realm**.
3. Name: `order-sync`. Click **Create**.

### 2. Create the client

1. Inside the `order-sync` realm, go to **Clients** → **Create client**.
2. Client ID: `order-sync-api`. Click **Next**.
3. **Capability config**: turn **Client authentication** to `On` (this makes it a confidential client, with a secret). Under **Authentication flow**, check **Standard flow**, **Direct access grants**, and **Service accounts roles**. Click **Next**.
4. **Login settings**: leave everything blank (these fields are only relevant for browser-redirect login flows, which this project doesn't use). Click **Save**.

### 3. Get the client secret

1. On the client's page, go to the **Credentials** tab.
2. Copy the **Client secret** value.

### 4. Create a test user

1. Go to **Users** → **Add user**. Username: `testuser`. Click **Create**.
2. On the user's **Credentials** tab, click **Set password**. Set a password (e.g. `testpassword`), turn **Temporary** off, and save.

### 5. Configure the application

Add the following to your `.env` (see `.env.example` for the full list):

```
KEYCLOAK_CLIENT_SECRET=<the client secret you copied>
KEYCLOAK_TEST_USERNAME=testuser
KEYCLOAK_TEST_PASSWORD=testpassword
```

The application reads `.env` automatically at startup (via `spring-dotenv`) — no manual environment variable setup needed in your IDE or shell.

## Testing the API

The base URL for local development is `http://localhost:8080`. All endpoints below (except `/actuator/health`) require a valid JWT — see [Setting Up Keycloak](#setting-up-keycloak) first.

### Getting an access token

```bash
TOKEN=$(curl -s -X POST http://localhost:8081/realms/order-sync/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=order-sync-api" \
  -d "client_secret=$KEYCLOAK_CLIENT_SECRET" \
  -d "username=testuser" \
  -d "password=testpassword" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
```

This uses the `password` grant with the test user — the simplest way to get a token by hand. Machine-to-machine callers (like the automated tests) instead use the `client_credentials` grant, which doesn't require a username/password.

Include the token in every request:

```bash
curl http://localhost:8080/api/orders -H "Authorization: Bearer $TOKEN"
```

A request without a valid token, or with an expired one, returns `401 Unauthorized`.

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/orders` | List all synced orders |
| `POST` | `/api/orders` | Create a new order |
| `PUT` | `/api/orders/{legacyOrderId}` | Update an existing order |
| `GET` | `/actuator/health` | Application health check (Spring Boot Actuator) |

### Create an order

```
POST /api/orders
Content-Type: application/json

{
  "legacyOrderId": 3001,
  "customerName": "Acme Corp",
  "productCode": "SKU-001",
  "quantity": 5,
  "unitPrice": 49.90,
  "status": "PENDING"
}
```

Example with `curl`:

```bash
curl -i -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
        "legacyOrderId": 3001,
        "customerName": "Acme Corp",
        "productCode": "SKU-001",
        "quantity": 5,
        "unitPrice": 49.90,
        "status": "PENDING"
      }'
```

Returns `201 Created` with the created order (currentVersion: 1) and a `Location` header pointing to the new resource. A snapshot (version 1) and an audit trail entry (`CREATED`) are generated automatically.

### Update an order

```
PUT /api/orders/{legacyOrderId}
Content-Type: application/json

{
  "customerName": "Acme Corp",
  "productCode": "SKU-001",
  "quantity": 5,
  "unitPrice": 49.90,
  "status": "CONFIRMED"
}
```

Example with `curl`:

```bash
curl -i -X PUT http://localhost:8080/api/orders/3001 \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
        "customerName": "Acme Corp",
        "productCode": "SKU-001",
        "quantity": 5,
        "unitPrice": 49.90,
        "status": "CONFIRMED"
      }'
```

Returns `200 OK` with the updated order. If any field actually changed, `currentVersion` is incremented, and a new snapshot, a delta (describing what changed), and an audit trail entry (`UPDATED`) are generated. If nothing changed, the order is returned as-is with no new version.

### List all orders

```bash
curl http://localhost:8080/api/orders -H "Authorization: Bearer $TOKEN"
```

Returns `200 OK` with a JSON array of all orders currently synced.

### Error responses

| Scenario | Status | Notes |
|---|---|---|
| Missing or invalid JWT | `401 Unauthorized` | |
| Creating an order with a `legacyOrderId` that already exists | `409 Conflict` | |
| Updating a `legacyOrderId` that doesn't exist | `404 Not Found` | |
| Invalid payload (missing/blank fields, negative quantity or price) | `400 Bad Request` | Response body includes a `fields` map with the specific validation errors |

## Deploying to Kubernetes

The application can also run inside a local Kubernetes cluster (Minikube), while the infrastructure (Oracle, Kafka, Redis, Keycloak) keeps running via Docker Compose, outside the cluster. This mirrors a common real-world pattern: a new service moving to Kubernetes while integrating with systems that stay where they are.

### Prerequisites

Install `kubectl` and Minikube:

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/

curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
```

Start the cluster:

```bash
minikube start --driver=docker
kubectl get nodes   # should show "minikube" with status Ready
```

### How networking works here

The app's pod runs inside the Minikube cluster, but Oracle, Kafka, Redis, and Keycloak run outside it, via the existing `docker-compose.yml`. Minikube (with the `docker` driver) resolves the special hostname `host.minikube.internal` back to your host machine, so the `k8s` Spring profile (`application-k8s.yaml`) uses that instead of `localhost` or Docker Compose service names:

```yaml
spring:
  datasource:
    url: jdbc:oracle:thin:@host.minikube.internal:1521/XEPDB1
  kafka:
    bootstrap-servers: host.minikube.internal:9094
  data:
    redis:
      host: host.minikube.internal
  security:
    oauth2:
      resourceserver:
        jwt:
          jwk-set-uri: http://host.minikube.internal:8081/realms/order-sync/protocol/openid-connect/certs
```

**Kafka needs a third listener for this.** The broker advertises different addresses depending on who's connecting: `kafka:19092` inside the Docker Compose network, `localhost:9092` for the host machine, and — new for this phase — `host.minikube.internal:9094` for anything running inside Minikube. All three are configured on the `kafka` service in `docker-compose.yml`:

```yaml
KAFKA_LISTENERS: PLAINTEXT://:19092,PLAINTEXT_HOST://:9092,PLAINTEXT_K8S://:9094,CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:19092,PLAINTEXT_HOST://localhost:9092,PLAINTEXT_K8S://host.minikube.internal:9094
```

Without this, the pod can make the *initial* connection to Kafka but then gets redirected to `localhost:9092` for the actual group coordinator — which, from inside the pod, means the pod itself, not the broker. That failure mode looks like `Connection to node ... (localhost/127.0.0.1:9092) could not be established` in the logs, repeated forever.

### Build the image inside Minikube

Minikube runs its own isolated Docker daemon — the image built for `docker-compose.yml` isn't visible to it. Point your terminal at Minikube's Docker before building:

```bash
eval $(minikube docker-env)
docker build -t order-sync-app:latest .
docker images | grep order-sync-app   # confirm it exists
```

This `eval` only affects the current terminal session. Undo it with `eval $(minikube docker-env -u)` when you want to go back to your normal Docker (for example, to run `docker compose` commands against the host infrastructure).

### Kubernetes manifests

The `k8s/` folder contains:

- `configmap.yaml` — non-sensitive configuration (active profile, DB username).
- `secret.yaml` — sensitive values (DB password, Keycloak client secret). **Not committed to git** — copy `secret.example.yaml` and fill in real values.
- `deployment.yaml` — the app's Deployment, with `imagePullPolicy: Never` (the image is only ever built locally into Minikube, never pulled from a registry), and readiness/liveness probes against Spring Boot Actuator's `/actuator/health/readiness` and `/actuator/health/liveness` endpoints.
- `service.yaml` — a `NodePort` Service exposing the app on port `30080`.

Apply them:

```bash
kubectl apply -f k8s/configmap.yaml -f k8s/secret.yaml -f k8s/deployment.yaml -f k8s/service.yaml
kubectl get pods -w
```

Wait for the pod to reach `1/1 Running` with `0` restarts.

### Testing the deployment

```bash
MINIKUBE_IP=$(minikube ip)
curl http://$MINIKUBE_IP:30080/actuator/health
```

With a token (see [Getting an access token](#getting-an-access-token)):

```bash
curl http://$MINIKUBE_IP:30080/api/orders -H "Authorization: Bearer $TOKEN"
```

### Troubleshooting

- **Pod crashes with `AccessDeniedException` on `/app/import`** — the container's non-root user doesn't own `/app`. Fix: in the `Dockerfile`, run `chown -R spring:spring /app` after copying the jar, before switching to the `spring` user.
- **Consumer stuck reconnecting to `localhost:9092` forever** — missing third Kafka listener. See the networking section above; add `PLAINTEXT_K8S` and recreate the `kafka` container.
- **A newly added container port shows as mapped (`docker port`) but connections are refused on the host** — the `docker-proxy` process handling that port binding can get stuck, especially after adding a port to an already-running container. `docker compose stop && docker compose rm -f && docker compose up -d` on the affected service usually fixes it; if not, `sudo systemctl restart docker` clears it (this restarts all containers, including Minikube, which you'll need to `minikube start` again afterwards).
- **Readiness/liveness probes fail with `401`** — Spring Security is blocking Kubernetes' own health checks. The `SecurityConfig` needs to permit `/actuator/health/**` (not just the exact `/actuator/health` path), since the probes hit `/actuator/health/readiness` and `/actuator/health/liveness` specifically.
- **After `minikube start`, the built image seems to have disappeared** — if Minikube's underlying container was fully removed (not just restarted), rebuild it with `eval $(minikube docker-env) && docker build -t order-sync-app:latest .`, then `kubectl rollout restart deployment/order-sync`.

## Running Tests

- `./mvnw test` — fast unit/context tests only, no Docker required.
- `./mvnw verify` — runs the full suite, including integration tests (`*IT`) that spin up ephemeral Oracle containers via Testcontainers. Requires Docker to be running.

## License

This is a personal study project. No license restrictions — feel free to use it as a reference.
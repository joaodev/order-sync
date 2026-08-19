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

## Testing the API

The base URL for local development is `http://localhost:8080`.

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
curl http://localhost:8080/api/orders
```

Returns `200 OK` with a JSON array of all orders currently synced.

### Error responses

| Scenario | Status | Notes |
|---|---|---|
| Creating an order with a `legacyOrderId` that already exists | `409 Conflict` | |
| Updating a `legacyOrderId` that doesn't exist | `404 Not Found` | |
| Invalid payload (missing/blank fields, negative quantity or price) | `400 Bad Request` | Response body includes a `fields` map with the specific validation errors |

## Running Tests

- `./mvnw test` — fast unit/context tests only, no Docker required.
- `./mvnw verify` — runs the full suite, including integration tests (`*IT`) that spin up ephemeral Oracle containers via Testcontainers. Requires Docker to be running.

## License

This is a personal study project. No license restrictions — feel free to use it as a reference.
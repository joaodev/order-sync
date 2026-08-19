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

On startup, Flyway automatically applies the schema migrations against the Oracle instance running in Docker.

## Testing the API

The base URL for local development is `http://localhost:8080`.

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
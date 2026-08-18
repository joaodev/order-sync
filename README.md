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

## Tech Stack

| Concern | Technology |
|---|---|
| Language / Runtime | Java 21 |
| Framework | Spring Boot 4.1 (Spring Framework 7) |
| Build tool | Maven |
| Database | Oracle Database XE |
| Data access | Spring Data JPA + jOOQ |
| DB versioning | Flyway |
| Messaging | Apache Kafka (KRaft mode) |
| CDC | Debezium (stand-in for Oracle GoldenGate) |
| Batch processing | Spring Batch |
| Caching / idempotency | Redis |
| Auth | Keycloak (OAuth2 / JWT) |
| Workflow orchestration | n8n |
| Integration testing | Testcontainers |
| Containerization | Docker, Docker Compose |
| Orchestration | Kubernetes (Minikube/Kind) |
| Observability | Prometheus, Grafana, Actuator |
| CI/CD | GitHub Actions (stand-in for Azure DevOps) |

> **Note on substitutions:** Oracle GoldenGate and Azure DevOps/ArgoCD are proprietary/enterprise tools without an accessible free tier for a personal study project. Debezium and GitHub Actions are used here as functional equivalents — the underlying concepts (CDC → event stream, CI/CD pipeline) transfer directly.

## License

This is a personal study project. No license restrictions — feel free to use it as a reference.
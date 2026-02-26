# University Recommendation System

> *"Designed for production readiness; minimal subset implemented in local environment."*

A **Batch-based Collaborative Filtering Recommendation System** built for an online learning platform.  
It ingests large-scale student interaction data (EdNet / KT4 format), processes it with Apache Spark to derive student feature vectors, stores everything in HDFS, and serves personalised recommendations through a FastAPI REST API backed by PostgreSQL.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Container Orchestration](#container-orchestration)
- [Scheduler](#scheduler)
- [Data Source Layer](#data-source-layer)
- [Data Ingestion Layer](#data-ingestion-layer)
- [Data Storage Layer](#data-storage-layer)
- [Batch Processing Layer](#batch-processing-layer)
- [Serving Layer](#serving-layer)
- [Non-Functional Requirements](#non-functional-requirements)
- [Folder Structure](#folder-structure)
- [Technology Stack](#technology-stack)

---

## Architecture Overview

The system is structured across **5 functional layers**, all connected through **Docker Compose** on a single shared network:

```
Data Source Layer
      │
      ▼
Data Ingestion Layer       ← Python / PySpark  (spark:3.x)
      │
      ▼
Data Storage Layer         ← HDFS              (bde2020/hadoop)
      │
      ▼
Batch Processing Layer     ← Spark / PySpark + Spark SQL  (spark:3.x)
      │
      ▼
Serving Layer              ← Python / FastAPI + PostgreSQL
```

All layers are orchestrated by a **cron-based scheduler** that runs two bash scripts:
- **Script A** — One-time initial historical load
- **Script B** — Daily incremental pipeline at `02:00`

---

## Container Orchestration

Managed via **Docker Compose** with the following guarantees across all services:

| Concern | Implementation |
|---|---|
| Networking | One shared Docker network |
| Service discovery | By service name (DNS-based) |
| Reliability | Healthchecks + restart policies on all containers |
| Versioning | Versioned Docker images per service |

---

## Scheduler

| Script | Type | Description |
|---|---|---|
| **Bash Script A** | One-time | Initial Load — bootstraps the system with full historical data |
| **Bash Script B** | Cron-based (Daily @ 02:00) | Daily Pipeline — processes and loads the previous day's incremental data |

---

## Data Source Layer

### Sources

| Source | Type |
|---|---|
| Worldwide Mobile Users | Live user interactions |
| Worldwide Web Users | Live user interactions |
| Online Learning Platform | Session and activity events |
| **EdNet Dataset** | Batch data export (CSV) |
| **KT4 Interaction Logs** | >100M time-stamped interactions, educational platform behavior |

### Non-Functional Requirements

| Category | Details |
|---|---|
| **Governance** | License compliance, Dataset documentation & citation |
| **Security** | Checksum verification on ingested files, Read-only raw files |
| **Privacy** | Use anonymized user IDs only — no PII at source |

---

## Data Ingestion Layer

**Microservice:** `microservices/ingestion/`  
**Runtime:** Python / PySpark  
**Base Image:** `spark:3.x`  
**Docker Compose:** 4 containers (part of Storage Microservice group)

### Pipeline Steps (in order)

#### 1. File Intake
- Reads incoming **CSV files** from the data source
- Entry point for all raw data into the system

#### 2. Structural Validation
Validates each file before it is accepted:
- Required columns check
- Row count / completeness check
- Type casting validation
- Basic normalization

#### 3. Format Conversion
- Converts accepted CSV files → **Parquet format**
- Enables columnar storage and efficient Spark reads downstream

#### 4. Compaction *(by `event_date`)*
- Merges many small Parquet files into larger, optimally-sized files
- **Target file size:** ~128–512 MB per file
- Reduces HDFS metadata overhead and improves Spark read parallelism

#### 5. Raw Storage Write
- Writes final Parquet files into the **HDFS Raw Zone**
- Two modes:
  - **Initial historical dump** — full backfill on first run
  - **Daily incremental files** — appended daily by the scheduler

### Non-Functional Requirements

| Category | Details |
|---|---|
| **Governance** | Schema contract enforcement, Basic data quality checks |
| **Security** | Non-root container, Input validation on all fields |
| **Privacy** | Keep only required columns — drop all unrequired PII fields |
| **Reliability** | Durable append-only writes, Schema + data validation gates, Failure isolation per step |
| **Scalability** | Partition by `event_date`, Parquet columnar format |
| **Maintainability** | Modular ingestion pipeline (one responsibility per pipeline step), Versioned Docker image |

---

## Data Storage Layer

**Microservice:** `microservices/storage/`  
**Base Image:** `bde2020/hadoop` (HDFS)  
**Docker Compose:** 4 containers

### HDFS Cluster Infrastructure

| Component | Count | Role |
|---|---|---|
| **NameNode** | 1 | Stores HDFS metadata (file tree, block locations) |
| **DataNode** | 3 | Stores actual data blocks (Parquet files) |
| **Replication Factor** | 3 | Each block replicated across 3 DataNodes (block-level) |

HDFS acts as the **Data Lake** — all data stored as Parquet files.

### Logical Data Zones

#### 🔒 Raw Zone — `/data/raw` *(immutable)*

| Path | Contents |
|---|---|
| `/data/raw/kt4/partitions_by_event_date/` | KT4 student interaction events, partitioned by `event_date` |
| `/data/raw/content/questions/` | Raw question/exercise content |
| `/data/raw/content/lectures/` | Raw lecture content |

> **Immutability rule:** Raw zone data is never modified after write. All transformations happen in Curated zone only.

#### ✏️ Curated Zone — `/data/curated` *(derived only)*

| Path | Contents |
|---|---|
| `/data/curated/Aggregated_student_features/` | Per-student aggregated feature profiles |
| `/data/curated/User_vectors/` | Final user embedding vectors used for similarity computation |
| `/data/curated/recommendations_batch/` | Precomputed batch recommendation outputs |

### Non-Functional Requirements

| Category | Details |
|---|---|
| **Governance** | Raw vs. Curated zone separation enforced, Immutable raw zone (no updates ever) |
| **Security** | RBAC permissions on HDFS paths, Encryption at rest |
| **Privacy** | No PII stored in HDFS, Retention policy enforced |
| **Reliability** | Replication factor = 3 (no SPOF), Automated failover, Failure isolation |
| **Scalability** | Horizontally scalable cluster (add DataNodes), Parallel reads via Spark |
| **Maintainability** | Clear zone separation, Structured folder hierarchy |

---

## Batch Processing Layer

**Microservice:** `microservices/processing/`  
**Runtime:** Python / PySpark + Spark SQL  
**Base Image:** `spark:3.x`

### Pipeline Steps (in order)

#### 1. Data Intake
- Reads raw KT4 interaction events from HDFS (last N days)
- Joins interaction data with **questions** and **lectures** content from Raw Zone
- Output: enriched interaction DataFrame in Spark

#### 2. Feature Aggregation
Computes per-student aggregate profiles:

| Feature | Description |
|---|---|
| **Learning Engagement Level** | How actively the student interacts with content |
| **Skill Strength Profile** | Per-skill mastery scores derived from question responses |
| **Learning Stage** | Current stage of progression in the curriculum |

#### 3. Feature Engineering
Transforms raw aggregates into ML-ready vectors:

| Step | Description |
|---|---|
| **Normalization** | Scales all features to a consistent range |
| **Recency weighting** | Gives more weight to recent interactions over older ones |
| **Build user vectors** | Produces final dense feature vector per student |

Output: Curated zone → `/data/curated/User_vectors/`

#### 4. Similarity Computation
Computes student-to-student similarity for collaborative filtering:

| Step | Description |
|---|---|
| **Cosine similarity** | Measures angle between user feature vectors |
| **Top-K collaborators** | For each user, identifies the K most similar peers |

Output: Curated zone → `/data/curated/recommendations_batch/`

### Non-Functional Requirements

| Category | Details |
|---|---|
| **Governance** | Feature lineage tracking, Reproducible batch logic |
| **Security** | Env-based secrets (no hardcoded credentials), Internal network isolation |
| **Privacy** | Only derived features stored in curated zone — no raw PII |
| **Reliability** | Cron retries + idempotent jobs, Task-level fault recovery, Failure isolation |
| **Scalability** | Distributed Spark processing, Windowed processing (last N days, configurable) |
| **Maintainability** | Modular processing pipeline, Configurable processing window |

---

## Serving Layer

**Docker Compose:** 2 containers

The serving layer is split into two distinct sub-services:

---

### Recommendation Loader

**Path:** `microservices/serving/recommendation_loader/`  
**Runtime:** Python

Responsibility: **HDFS → PostgreSQL ETL**

Reads the precomputed recommendation batches from the HDFS Curated Zone and loads them into PostgreSQL so the API can serve them with low latency.

---

### Serving Store (PostgreSQL)

**Path:** `serving_store/postgres/`  
**Base Image:** `postgres:14`

Stores the **last N days** of precomputed recommendations.

**Table: `recommendations`**

| Column | Type | Description |
|---|---|---|
| `user_id` | UUID / INT | The student receiving the recommendation |
| `recommended_user_id` | UUID / INT | The recommended peer student |
| `similarity_score` | FLOAT | Cosine similarity score between the two users |
| `generation_date` | DATE | The batch date this recommendation was generated |

---

### Recommendation API

**Path:** `microservices/serving/recommendation_api/`  
**Runtime:** Python / FastAPI  
**Base Image:** `python:3.11-slim`

| Endpoint | Method | Description |
|---|---|---|
| `/recommendations/me` | `GET` | Returns the current user's top recommended peers |

- Reads directly from the `recommendations` table in PostgreSQL
- Returns a **JSON** response with peer IDs and similarity scores
- Stateless — no computation at request time (all precomputed)

### Non-Functional Requirements

| Category | Details |
|---|---|
| **Governance** | API versioning enforced, `generation_date` stored on every record |
| **Security** | JWT authentication required on all routes, HTTPS only |
| **Privacy** | Response returns IDs + score only — no personal data exposed |
| **Reliability** | Read-only recommendation table (no write risk), Graceful degradation, Failure isolation |
| **Scalability** | Precomputed recommendations (no real-time compute), Indexed queries on `user_id` |
| **Maintainability** | Versioned endpoint (`/v1/recommendations/me`), Loader and API are fully separated services |

---

## Non-Functional Requirements

Summary across all layers:

| Layer | Key NFRs |
|---|---|
| **Data Source** | License compliance, anonymized IDs, checksum verification |
| **Ingestion** | Schema contracts, input validation, non-root containers, idempotent writes |
| **Storage** | RBAC, encryption at rest, no PII, replication=3, no SPOF |
| **Processing** | Feature lineage, env secrets, idempotent Spark jobs, windowed processing |
| **Serving** | JWT auth, HTTPS, precomputed results, indexed queries, versioned API |

---

## Folder Structure

```
ui_university_project/
│
├── microservices/
│   │
│   ├── ingestion/                          # Python/PySpark ingestion service
│   │   ├── pipelines/
│   │   │   ├── file_intake/               # Reads raw CSV files
│   │   │   ├── structural_validation/     # Column checks, type casting, normalization
│   │   │   ├── format_conversion/         # CSV → Parquet
│   │   │   ├── compaction/                # Merge small files (~128-512MB target)
│   │   │   └── raw_storage_write/         # Write to HDFS raw zone
│   │   ├── src/                           # Core service source code
│   │   ├── config/                        # Service configuration
│   │   ├── docker/                        # Dockerfile
│   │   └── tests/                         # Unit & integration tests
│   │
│   ├── processing/                         # Spark batch processing service
│   │   ├── pipelines/
│   │   │   ├── data_intake/               # Read raw interactions (last N days) + join content
│   │   │   ├── feature_engineering/       # Normalization, recency weighting, user vectors
│   │   │   ├── feature_aggregation/       # Engagement, skill strength, learning stage
│   │   │   └── similarity_computation/    # Cosine similarity, Top-K collaborators
│   │   ├── spark_jobs/                    # Spark job entry-point scripts
│   │   ├── src/                           # Supporting source code
│   │   ├── config/                        # Spark & env config
│   │   ├── docker/                        # Dockerfile
│   │   └── tests/                         # Transformation unit tests
│   │
│   ├── storage/                            # HDFS cluster + logical zone definitions
│   │   ├── hdfs/
│   │   │   ├── namenode/                  # HDFS NameNode config (metadata)
│   │   │   └── datanodes/                 # HDFS DataNode config (block storage, x3)
│   │   ├── zones/
│   │   │   ├── raw/
│   │   │   │   ├── kt4/
│   │   │   │   │   └── partitions_by_event_date/   # KT4 events, date-partitioned
│   │   │   │   └── content/
│   │   │   │       ├── lectures/          # Raw lecture content
│   │   │   │       └── questions/         # Raw question content
│   │   │   └── curated/
│   │   │       ├── aggregated_student_features/     # Per-student feature profiles
│   │   │       ├── user_vectors/                    # Final user embedding vectors
│   │   │       └── recommendations_batch/           # Precomputed recommendation output
│   │   ├── src/
│   │   ├── config/
│   │   ├── docker/
│   │   └── tests/
│   │
│   └── serving/
│       ├── recommendation_loader/          # HDFS → PostgreSQL ETL (Python)
│       │   ├── hdfs_to_postgres/          # ETL logic
│       │   ├── pipelines/                 # Load pipeline steps
│       │   ├── src/
│       │   ├── config/
│       │   ├── docker/
│       │   └── tests/
│       │
│       └── recommendation_api/            # REST API (Python / FastAPI)
│           ├── src/
│           │   ├── routes/                # HTTP route handlers (GET /recommendations/me)
│           │   ├── schemas/               # Request/response validation schemas
│           │   └── services/              # Business logic — queries PostgreSQL
│           ├── config/
│           ├── docker/
│           └── tests/
│
├── orchestration/
│   ├── docker_compose/                    # Docker Compose YAML for all services
│   ├── healthchecks/                      # Per-service health check definitions
│   ├── network/                           # Docker network configuration
│   └── scheduler/
│       ├── cron/                          # Cron tab definitions (Daily @ 02:00)
│       └── scripts/
│           ├── initial_load/              # Bash Script A: one-time historical load
│           └── daily_pipeline/            # Bash Script B: daily incremental run
│
├── serving_store/
│   └── postgres/
│       ├── migrations/                    # SQL schema migrations (versioned)
│       └── seeds/                         # SQL seed data (initial/test data)
│
└── shared/
    ├── schema_contracts/                  # Shared data schemas enforced across services
    └── utils/                             # Common utilities (logging, HDFS client, etc.)
```

---

## Technology Stack

| Layer | Technology | Base Image / Version |
|---|---|---|
| Ingestion | Python / PySpark | `spark:3.x` |
| Storage (HDFS) | Apache HDFS | `bde2020/hadoop` |
| Batch Processing | Apache Spark / PySpark + Spark SQL | `spark:3.x` |
| Serving Database | PostgreSQL | `postgres:14` |
| Recommendation API | Python / FastAPI | `python:3.11-slim` |
| Containerisation | Docker + Docker Compose | — |
| Scheduler | Bash + Cron | — |
| Data Format | Parquet (HDFS), JSON (API), SQL (PostgreSQL) | — |

---

## Data Flow Summary

```
1. SOURCE
   KT4 events (>100M interactions) + Content (lectures, questions)

2. INGESTION  [spark:3.x]
   file_intake
     → structural_validation  (required cols, row check, type casting, normalization)
     → format_conversion       (CSV → Parquet)
     → compaction              (merge to ~128-512MB files, by event_date)
     → raw_storage_write       (initial dump OR daily incremental)
   → HDFS Raw Zone

3. PROCESSING  [spark:3.x]
   data_intake                 (read last N days, join questions & lectures)
     → feature_aggregation     (engagement level, skill strength, learning stage)
     → feature_engineering     (normalization, recency weighting, user vectors)
     → similarity_computation  (cosine similarity, Top-K collaborators)
   → HDFS Curated Zone

4. SERVING LOAD  [Python]
   recommendation_loader:
   HDFS curated/recommendations_batch → PostgreSQL recommendations table

5. SERVING API  [FastAPI / python:3.11-slim]
   GET /recommendations/me
   → reads from PostgreSQL → returns JSON (user_id, recommended_user_id, score)

6. ORCHESTRATION
   Cron @ 02:00 → triggers steps 2–4 daily
   Docker Compose → all services on one network
```

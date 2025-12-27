# Dagster DAG Factory

**Declarative Data Engineering for the Modern Data Stack.**

`dagster-dag-factory` is a powerful framework designed to generate Dagster assets, jobs, and sensors dynamically from YAML-driven pipeline configurations. It bridges the gap between high-level analyst intent and robust, high-performance execution.

---

## 🚀 Key Features

### 1. Unified Streaming Pattern
High-performance "Self-Feeding" streaming engine for massive data transfers.
- **SQL Server to S3**: Parallelized extraction and upload with 10% progress milestones.
- **SFTP to S3**: Efficient file-by-file or multi-file streaming.
- **Universal Predicates**: Niagara-style Jinja predicates (e.g., `file_size > 1024`) for precise filtering.

### 2. Smart Discovery (Sensors)
Data-driven discovery mechanisms to automate ingestion.
- **SQL Sensor**: Incremental polling via High Water Mark (HWM) tracking (SQL Server, Snowflake, Postgres).
- **SFTP & S3 Sensors**: Cursor-based file discovery with regex and predicate support.

### 3. Rich Template Ecosystem
Leverage powerful custom macros in your YAML definitions:
- **`fn.date`**: Advanced date formatting and math.
- **`fn.json`**: Helpers for parsing/serializing JSON payloads.
- **`fn.file`**: Dynamic path and filename generation.
- **`fn.secrets`**: Secure retrieval of credentials and environmental variables.

### 4. Build-Phase Integrity
- **Strict Validation**: Pydantic-powered schema enforcement catches errors before the DAG even builds.
- **Contextual Logging**: Structured build-phase logs with clear action summaries.
- **CLI Linting**: Standalone `lint` tool to validate pipeline repos locally.

---

## 📂 Project Structure

A standard pipeline repository follow this layout:

```text
my-pipelines/
├── connections/     # Resource definitions (S3, Snowflake, etc.)
├── vars/           # Environment-specific variables (common, dev, prod)
├── defs/           # YAML Pipeline definitions
├── pyproject.toml  # Standard Dagster discovery configuration
└── src/            # (Optional) Custom Python logic
```

---

## 🛠️ Getting Started

1.  **Initialize your Repository**: Follow the layout above.
2.  **Define a Connection**:
    ```yaml
    # connections/dev.yaml
    resources:
      s3_conn:
        type: S3Resource
        config:
           bucket_name: "my-data-lake"
    ```
3.  **Create a Pipeline**:
    ```yaml
    # defs/ingest_orders.yaml
    assets:
      - name: orders_ingestion
        source:
          type: SQLSERVER
          connection: sqlserver_conn
          configs:
            sql: "SELECT * FROM orders WHERE updated_at > '{{ metadata.last_cursor }}'"
        target:
          type: S3
          connection: s3_conn
          configs:
            prefix: "raw/orders/"
    ```

---

## ✅ Verification
Use the included `example-pipelines` repository as a baseline for testing and verification. It contains proven configurations for all major operators and features.

---

## ⚖️ License
Internal Use Only.

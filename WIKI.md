# 🚀 Dagster Factory: Analyst Wiki

Welcome to the **Dagster Factory**! This guide is designed for Data Analysts and Engineers to help you build, deploy, and scale data pipelines using simple YAML configuration files. No deep Python knowledge is required to get started.

---

## 🏗️ 1. Anatomy of a Pipeline
Every pipeline in this system is defined in a single YAML file located in `src/pipelines/defs/`. A pipeline consists of three main components:

### 📦 Assets
An asset is a "data object" (like a table in Snowflake or a file in S3). In the factory, assets define **what** data is being moved and **how** it is configured.

```yaml
assets:
  - name: regional_sales_raw
    source:
      type: SFTP
      connection: sftp_prod
      configs:
        path: /outgoing/sales/
        pattern: ".*\\.csv"
    target:
      type: S3
      connection: s3_prod
      configs:
        bucket_name: my-bucket
        key: "raw/sales/{{ source.item.file_name }}"
```

### 🏃 Jobs
A job defines a **runnable unit** of work. It selects which assets should be materialized together.

```yaml
jobs:
  - name: ingestion_job
    selection: ["regional_sales_raw"]
```

### 📡 Sensors
Sensors are the "watchers." They poll external systems (like S3 or SFTP) and automatically trigger a **Job** when new data is found.

```yaml
sensors:
  - name: sftp_watcher
    type: SFTP
    job: ingestion_job
    connection: sftp_prod
    configs:
      path: /outgoing/sales/
```

---

## 🪄 2. The Magic of Jinja Templating
Jinaj is the engine that makes our YAMLs dynamic. It allows you to inject variables at runtime.

### 🌍 Global Variables
Available everywhere in your YAML:
- `{{ env.MY_ENV_VAR }}`: Access any system environment variable.
- `{{ vars.my_config }}`: Access global variables defined in `vars.yaml`.
- `{{ run_tags["factory/source_item"] }}`: Access metadata passed from a sensor to a job.

### 🎯 Contextual Variables (Inside Assets)
When an asset is triggered by a sensor, it has access to the `source.item` object:

| Variable | Description | Example |
| :--- | :--- | :--- |
| `{{ source.item.file_name }}` | The name of the file found. | `sales_2023_01.csv` |
| `{{ source.item.key }}` | The full path/key of the object. | `raw/sales/sales.csv` |
| `{{ source.item.size }}` | The size of the file in bytes. | `1024` |
| `{{ source.item.modified_date }}` | Human-readable timestamp. | `2023-12-25T10:00:00` |

---

## ⚒️ 3. Configuration Reference

### 🧊 S3 Configuration (`S3Config`)
Used for all S3 operations (Source or Target).

| Field | Description | Default |
| :--- | :--- | :--- |
| `bucket_name` | Destination bucket name. | (Required) |
| `key` | Specific S3 path including filename. | `None` |
| `prefix` | Folder path for multiple files. | `""` |
| `object_type` | Data format: `CSV`, `JSON`, `PARQUET`. | `CSV` |
| `mode` | `COPY` (standard) or `MULTI_FILE` (streaming). | `COPY` |
| `check_is_modifying`| If `true`, skips files active in the last 60s. | `false` |
| `predicate` | Python expression for advanced filtering. | `None` |

### 📂 SFTP Configuration (`SFTPConfig`)
Used for scanning and pulling data from SFTP.

| Field | Description | Default |
| :--- | :--- | :--- |
| `path` | Remote directory or file path. | (Required) |
| `pattern` | Regex to match filenames. | `.*` |
| `recursive` | Search subfolders? | `false` |
| `max_workers` | Parallel threads for fast transfers (1-20). | `5` |

---

## 🏎️ 4. Operator Gallery (The Data Movers)

### `SFTP` → `S3`
Transfers files from SFTP to S3. Supports parallel discovery and streaming uploads.
*   **Use Case**: Ingesting daily logs or vendor files.
*   **Context**: Provides `source.item` (FileInfo) to Jinja templates.

### `S3` → `S3`
Copies files between S3 locations.
*   **Use Case**: Moving data from `landing/` to `processed/`.
*   **Context**: Provides `source.item` (S3Info) to Jinja templates.

---

## 🧞 5. Advanced Templating & Logic

### 🗓️ Partitions
If your asset is partitioned (e.g., daily), you can use:
- `{{ partition_key }}`: The current partition (e.g., `2023-10-01`).
- `{{ partition_start }}` / `{{ partition_end }}`: ISO timestamps for the window.

### 🔍 Predicates
Predicates allow you to skip files based on logic. For example, to only ingest files larger than 1MB:
```yaml
configs:
  predicate: "source.item.file_size > 1048576"
```

---

## 🎓 6. Tutorial: Your First Pipeline

### Step 1: Create the YAML
Create `src/pipelines/defs/my_first_pipeline.yaml`:

```yaml
assets:
  - name: my_hello_world
    source:
      type: S3
      connection: s3_prod
      configs:
        bucket_name: "my-poc-bucket"
        key: "inbound/hello.txt"
    target:
      type: S3
      connection: s3_prod
      configs:
        bucket_name: "my-poc-bucket"
        key: "outbound/hello_{{ execution_date }}.txt"

jobs:
  - name: dev_test_job
    selection: ["my_hello_world"]
```

### Step 2: Verify in Dagster
1. Save the file.
2. Open the Dagster UI.
3. Your new asset and job will appear automatically!
4. Click **Launchpad** and hit **Launch Run**.

---

## 💡 Best Practices
1. **Always use connections**: Never hardcode credentials. Use the `connection:` field.
2. **Dynamic Paths**: Always use `{{ source.item.object_name }}` in the target key to maintain clean structures.
3. **Dry Run**: Use the "Preview" feature in sensors to see what they would find before enabling them.
4. **Compression**: Use the `compress_options` field in S3 configs to automatically `GZIP` your data on upload.

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
Available everywhere in your YAML (Assets, Jobs, Sensors):

| Variable | Source | Description |
| :--- | :--- | :--- |
| `{{ env.VAR_NAME }}` | Environment | Access system environment variables (e.g. `env.AWS_REGION`). |
| `{{ vars.my_val }}` | `vars.yaml` | Access global project-level variables. |
| `{{ run_tags["key"] }}` | Runtime | Access metadata from the current run (e.g. `factory/source_item`). |
| `{{ partition_key }}` | Partitions | The current partition ID (e.g. `2023-12-25`). |
| `{{ partition_start }}` | Partitions | ISO start of the partition window. |
| `{{ partition_end }}` | Partitions | ISO end of the partition window. |

---

## 🏎️ 4. Operator Gallery
Operators are the "engines" that move data. The factory automatically picks the right one based on your `source.type` and `target.type`.

### **1. SFTP → S3** (`SftpToS3Operator`)
Moves files from a remote server to AWS S3.
- **Features**: Parallel streaming, regex matching, recursive scan, newline-safe splitting.
- **Context**: `source.item` is a `FileInfo` object.

### **2. S3 → S3** (`S3ToS3Operator`)
Copies/Moves objects between S3 buckets or folders.
- **Features**: Cross-bucket copy, metadata preservation, parallel execution.
- **Context**: `source.item` is an `S3Info` object.

### **3. SqlServer → S3** (`SqlServerToS3Operator`)
Exports data from SQL Server to S3 (CSV/Parquet).
- **Features**: Query-based extraction, auto-chunking, compression.

### **4. S3 → Snowflake** (`S3ToSnowflakeOperator`)
Loads files from S3 into Snowflake tables.
- **Features**: High-performance `COPY INTO`, schema evolution, deduping.

### **5. SqlServer → Snowflake** (`SqlServerSnowflakeOperator`)
Direct database-to-database transfer via S3 staging.

---

## ⚙️ 5. Global Configuration Structure
To keep pipelines clean, we separate **Logic** (defs) from **Connection Details** (connections).

### 📁 **Connections (`connections/`)**
The factory looks for connections in an environment-aware way:
1. `common.yaml`: Base settings for all environments.
2. `prod.yaml` / `dev.yaml`: Environment-specific overrides.

**Example `common.yaml`:**
```yaml
resources:
  s3_prod:
    type: S3Resource
    config:
      region_name: "us-east-1"
```

### 📁 **Global Variables (`vars.yaml`)**
Use this for non-sensitive, shared constants that you want to reference in templates via `{{ vars.xxx }}`.

```yaml
# vars.yaml
landing_bucket: "my-company-raw-data"
default_retention_days: 30
```

### 🎯 Contextual Variables (Inside Assets)
It is important to note that **metadata models depend on the source type**. An S3 source provides `S3Info`, while an SFTP source provides `FileInfo`. 

#### **SFTP Metadata (`FileInfo`)**
| Variable | Description | Example |
| :--- | :--- | :--- |
| `{{ source.item.file_name }}` | The simple filename. | `sales.csv` |
| `{{ source.item.full_file_path }}` | The absolute path on the server. | `/tmp/inbox/sales.csv` |
| `{{ source.item.path }}` | The relative directory path. | `inbox` |
| `{{ source.item.file_size }}` | Size in bytes. | `1048576` |
| `{{ source.item.modified_ts }}` | Unix timestamp of modification. | `1703462400.0` |
| `{{ source.item.name }}` | Filename without extension. | `sales` |
| `{{ source.item.ext }}` | File extension with dot. | `.csv` |

#### **S3 Metadata (`S3Info`)**
| Variable | Description | Example |
| :--- | :--- | :--- |
| `{{ source.item.key }}` | Full S3 key. | `raw/sales/2023/sales.csv` |
| `{{ source.item.bucket_name }}` | The S3 bucket name. | `my-poc-bucket` |
| `{{ source.item.size }}` | Object size in bytes. | `2048` |
| `{{ source.item.object_name }}` | Just the filename part. | `sales.csv` |
| `{{ source.item.path }}` | The directory portion of the key. | `raw/sales/2023` |
| `{{ source.item.object_path }}` | The path relative to the scan prefix. | `2023/sales.csv` |
| `{{ source.item.modified_ts }}` | Unix timestamp of modification. | `1703462400.0` |

---

## ⚡ 3. Helper Functions (`fn`)
We provide a rich set of functions (accessible via `fn` or directly by name) to help with date and cron logic.

### **Date Helpers (`fn.date`)**
- `fn.date.format(value, fmt)`: Formats a date. Use `fmt: "X"` for Unix timestamp, `fmt: "YYYY-MM-DD"` for standard date.
- `fn.date.to_date_nodash(value)`: Convert any date to `YYYYMMDD`.

### **Cron Helpers (`fn.cron`)**
- `fn.cron.diff(start, end, interval)`: Count periods (`days`, `week`, `month`, `year`) between two dates.
- `fn.cron.next(schedule, base_date, count=1)`: Get the next scheduled occurrence(s).
- `fn.cron.prev(schedule, base_date, count=1)`: Get the previous scheduled occurrence(s).
- `fn.cron.range(start, end, schedule)`: Get a list of all scheduled occurrences in a window.

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
Predicates allow you to skip files based on custom logic. These are **Python expressions** that have access to `source.item` and all `fn` helpers.

**Examples:**
- **Size Check**: `"source.item.file_size > 1024"` (Only find files > 1KB)
- **Extension Check**: `"source.item.ext == '.csv'"`
- **Date Comparison**: `"source.item.modified_ts > fn.date.format(execution_date, 'X')"` (Only files newer than execution start)

---

## ⏱️ 6. Schedules & Automation
Pipelines can be automated to run at specific intervals using **Cron** expressions.

### **Implicit Scheduling (Recommended)**
You can add a `cron` field directly to an **Asset** or **Job**. The system will automatically create a schedule and its corresponding job for you.

```yaml
assets:
  - name: daily_sales_raw
    cron: "0 9 * * *" # Runs daily at 9:00 AM
    ...
```

### **Explicit Scheduling**
For more control, you can define a `schedules:` block.

```yaml
schedules:
  - name: heavy_lifting_schedule
    job: ingestion_job
    cron: "0 0 * * 0" # Runs every Sunday at midnight
```

> [!NOTE]  
> If you are using **Time Partitions**, the schedule will automatically align with your partition frequency. 

---

## 🏷️ 7. Tags & Concurrency
Use tags to organize your assets and limits to manage system resources.

### **Asset Tags**
Tags allow you to add arbitrary labels to your assets, which can be filtered in the Dagster UI.

```yaml
assets:
  - name: sftp_transfer
    tags:
      owner: sales_team
      priority: critical
```

### **Concurrency (Pools)**
If you are running many parallel transfers but want to limit the load on a specific resource (e.g., an SFTP server), use a **Concurrency Key**.

```yaml
assets:
  - name: high_volume_transfer
    concurrency_key: "sftp_limit" # Limits how many of these can run at once
```

---

## 🔐 9. Connection Reference (Resources)
Connections are defined globally (usually in the `connections/` directory) and shared across multiple pipelines. They store sensitive credentials and server details.

### **AWS / S3 Connection (`S3Resource`)**
Used for all AWS-related tasks. Supports multiple authentication methods.

| Field | Description | Default |
| :--- | :--- | :--- |
| `bucket_name` | Default bucket to use. | `None` |
| `access_key` | AWS Access Key ID. | `None` |
| `secret_key` | AWS Secret Access Key. | `None` |
| `region_name` | AWS Region (e.g., `us-east-1`). | `us-east-1` |
| `assume_role_arn` | ARN of an IAM role to assume. | `None` |
| `profile_name` | Use a specific AWS CLI profile. | `None` |

---

### **SFTP Connection (`SFTPResource`)**
Used to connect to remote servers.

| Field | Description | Default |
| :--- | :--- | :--- |
| `host` | Hostname or IP of the server. | (Required) |
| `username` | Connection username. | (Required) |
| `password` | Password (if not using keys). | `None` |
| `port` | SSH port. | `22` |
| `private_key` | Base64 encoded private key. | `None` |
| `key_type` | `RSA`, `ECDSA`, or `ED25519`. | `RSA` |

---

### **SQL Server Connection (`SQLServerResource`)**
Used for extracting data from SQL Server databases.

| Field | Description | Default |
| :--- | :--- | :--- |
| `host` | IP or Hostname of the database. | (Required) |
| `database` | Name of the database. | (Required) |
| `user` | SQL Authentication username. | `None` |
| `password` | SQL Authentication password. | `None` |
| `port` | Database port. | `1433` |
| `driver` | ODBC Driver Version. | `ODBC Driver 18...`|

---

### **Snowflake Connection (`SnowflakeResource`)**
Used for loading data into Snowflake.

| Field | Description | Default |
| :--- | :--- | :--- |
| `account` | Snowflake Account Identifier. | (Required) |
| `user` | Snowflake username. | (Required) |
| `password` | Snowflake password. | `None` |
| `warehouse` | Default virtual warehouse. | `None` |
| `database` | Default database. | `None` |
| `schema` | Default schema. | `None` |
| `role` | Default security role. | `None` |

---

## 🎓 10. Tutorial: Your First Pipeline

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

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

---

## 🪄 2. Jinja Essentials
Jinja is the engine that makes our YAMLs dynamic. It allows you to inject variables and logic at runtime.

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

### 🏗️ Template Fields (Whitelist)
To ensure safety and performance, **not all fields support Jinja templates**. Only designated "Template Fields" are evaluated. For others, curly braces `{{ }}` are treated as literal text.

#### **Whitelisted Fields:**
- **S3 Operations**: `bucket_name`, `key`, `prefix`, `pattern`, `predicate`.
- **SFTP Operations**: `path`, `pattern`, `file_name`, `predicate`.
- **Database/SQL**: `query`, `table_name`, `schema_name`, `sql_pre`, `sql_post`.
- **Format Options**: `delimiter`, `quotechar`, `escapechar` (inside `csv_options`).

> [!IMPORTANT]
> If a field is NOT in this list, Jinja expressions will be ignored. This protects parts of your config (like complex Regex or SQL) from being accidentally "broken" by the template engine.

### 🎯 Contextual Variables (`source.item`)
When an operator processes a file, it provides metadata about that specific item. **The available properties depend on the source type.**

#### **If Source is SFTP (`FileInfo`)**
| Property | Description | Example Usage |
| :--- | :--- | :--- |
| `{{ source.item.file_name }}` | Filename (`sales.csv`) | `key: "raw/{{ source.item.file_name }}"` |
| `{{ source.item.path }}` | Relative directory | `key: "{{ source.item.path }}/archive"` |
| `{{ source.item.file_size }}` | Size in bytes | `predicate: "source.item.file_size > 0"` |

#### **If Source is S3 (`S3Info`)**
| Property | Description | Example Usage |
| :--- | :--- | :--- |
| `{{ source.item.object_name }}`| Filename (`sales.csv`) | `key: "processed/{{ source.item.object_name }}"`|
| `{{ source.item.key }}` | Full S3 key | `key: "backup/{{ source.item.key }}"` |
| `{{ source.item.size }}` | Object size in bytes | `predicate: "source.item.size > 1024"` |

> [!CAUTION]
> **Strict terminology**: Do NOT use `file_name` for S3 sources; it will not render. Use `object_name` instead.

### ⚡ Helper Functions (`fn`)
We provide a rich set of functions to help with date and cron logic:
- `fn.date.to_date_nodash(partition_key)` -> `20231225`
- `fn.cron.prev("0 0 * * *", execution_date)` -> Yesterday's midnight.

---

---

---

## 🏎️ 3. Operator Gallery
Operators are the "engines" that move data.

### **1. SFTP → S3** (`SftpToS3Operator`)
Moves files from a remote server to AWS S3.
- **Context**: `source.item` is a `FileInfo` object.

### **2. S3 → S3** (`S3ToS3Operator`)
Copies/Moves objects between S3 buckets or folders.
- **Context**: `source.item` is an `S3Info` object.

### **3. SqlServer → S3** (`SqlServerToS3Operator`)
Exports data from SQL Server to S3 (CSV/Parquet).

### **4. S3 → Snowflake** (`S3ToSnowflakeOperator`)
Loads files from S3 into Snowflake tables.

### **5. SqlServer → Snowflake** (`SqlServerSnowflakeOperator`)
Direct database-to-database transfer via S3 staging.

---

---

---

## ⚙️ 4. Global Configuration (Project-wide)
To keep pipelines clean, we separate **Logic** (defs) from **Connection Details** (connections).

### 📁 **Connections (`connections/`)**
The factory looks for connections in an environment-aware way: `common.yaml`, `prod.yaml`, `dev.yaml`, etc.

### � **Global Variables (`vars/`)**
Access these in templates via `{{ vars.xxx }}`.

### 📁 **Folder Hierarchy**
- `src/pipelines/defs/`: Your `.yaml` definitions.
- `src/pipelines/connections/`: Resource settings.
- `src/pipelines/vars/`: Global constants.
- `src/pipelines/lib/`: Custom Python macros.

---

## ⏱️ 5. Schedules & Automation
Pipelines can be automated to run at specific intervals using **Cron** expressions.

### **Implicit Scheduling (Recommended)**
Add a `cron` field directly to an **Asset** or **Job**.
```yaml
assets:
  - name: daily_sales_raw
    cron: "0 9 * * *"
```

### **Explicit Scheduling**
Define a `schedules:` block for more control.

---

## 🏷️ 6. Tags & Concurrency

### **Asset Tags**
Labels for your assets: `owner`, `priority`, etc.

### **Concurrency (Pools)**
Limit load on external resources using `concurrency_key`.

---

---

---

## 🔐 7. Connection Reference (Resources)
Connections are defined globally in `connections/`.

### **AWS / S3 Connection (`S3Resource`)**
| Field | Description | Default |
| :--- | :--- | :--- |
| `bucket_name` | Default bucket. | `None` |
| `access_key` | AWS Access Key. | `None` |
| `secret_key` | AWS Secret Key. | `None` |

### **SFTP Connection (`SFTPResource`)**
| Field | Description | Default |
| :--- | :--- | :--- |
| `host` | IP or Hostname. | (Required) |
| `username` | Connection user. | (Required) |

---

## 🌍 8. Environment & Tutorial
For detailed setup, see the **Environment Hierarchy** and **Tutorial** sections below.

### **Environment Strategy**
Files in `connections/` and `vars/` follow the pattern: `common.yaml` + `<ENV>.yaml`.

### **Tutorial: First Pipeline**
1. Create `src/pipelines/defs/test.yaml`.
2. Define an asset with S3 source/target.
3. Refresh Dagster UI and materialise!

---

## 💡 Best Practices
1. **Strict Terminology**: Use `object_name` for S3 and `file_name` for SFTP.
2. **Template Whitelist**: Only listed fields support `{{ }}`.
3. **No Credentials**: Always use Connection Resources.

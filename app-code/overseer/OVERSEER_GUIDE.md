# Overseer: Autonomic Control Plane Guide

The Overseer is a standalone autonomic monitoring and control service for the Lakehouse. It implements a **MAPE-K** (Monitor-Analyze-Plan-Execute-Knowledge) loop to ensure system health, data freshness, and resource efficiency without manual intervention.

---

## 🏗️ Architecture: Total Abstraction

The Overseer is designed as a **blank-slate engine**. Unlike traditional monitoring systems, it has **zero hardcoded policies** and **zero static data targets**. 

- **Logic** is loaded dynamically from **S3/MinIO**.
- **Targets** are discovered dynamically from **Redis**.

This allows the system to scale and adapt to new datasets or heuristics with **zero downtime** and **zero restarts**.

---

## ⚙️ Setup & Configuration

### 1. Prerequisites
- **MinIO/S3**: Used for storing policy code (Lakehouse-as-Code).
- **Redis**: Used as the ContextStore/Registry for data targets.
- **Python Dependencies**: `boto3`, `deltalake`, `loguru`, `httpx`.

### 2. Configuration (`config.yaml`)
The configuration is kept minimal. You only define the service endpoints (for health checks) and the S3 sync settings.

```yaml
overseer:
  loop_interval_seconds: 15
  custom_policies:
    s3_uri: "s3://delta-lake/overseer-policies/"
    sync_interval_seconds: 60
    local_dir: "/tmp/overseer_custom_policies/"
```

---

## 🧠 Dynamic Policies (Hot-Reload)

Policies are Python scripts that inherit from `BasePolicy`. The Overseer automatically syncs these from S3 and compiles them at runtime.

### Initializing Default Policies
Run the seeding script to push the core heuristics to the Lakehouse:
```bash
uv run python app-code/scripts/upload_default_policies.py
```

### Writing a Custom Policy
Create a `.py` file and upload it to your S3 bucket under the `overseer-policies/` prefix:

```python
from overseer.policies.base import BasePolicy
from overseer.models import SystemSnapshot, OverseerAction, ActionType

class MyCustomPolicy(BasePolicy):
    def evaluate(self, snapshot: SystemSnapshot) -> list[OverseerAction]:
        # Perform logic on snapshot.services
        return []
```

**Safety**: The engine wraps compilation in a safety barrier. If your code has syntax or runtime errors, the Overseer will log the error but **will not crash**.

---

## 📡 Dynamic Data Monitoring

The Overseer does not need to know your database URIs at startup. It discovers them via Redis.

### Registering a New Target
To start monitoring a new Delta Table (or database), simply add it to the Redis Hash `overseer:targets:delta_lake`:

```bash
# Using the helper script
uv run python app-code/scripts/seed_targets.py
```

Or via code:
```python
await redis.hset("overseer:targets:delta_lake", "my_table", "s3://bucket/path")
```

The `DeltaLakeCollector` will immediately pick up the new URI on the next cycle, and the `DataStalenessPolicy` will begin monitoring it.

---

## 🛠️ Operational Commands

- **Start Overseer**: `uv run overseer`
- **Sync Built-in Policies**: `uv run python app-code/scripts/upload_default_policies.py`
- **Register Demo Target**: `uv run python app-code/scripts/seed_targets.py`

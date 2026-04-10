
# SPCS Deployment Guide — CDC Pipeline Tracker

Deploy the CDC Pipeline Tracker as a Snowpark Container Service.

## Prerequisites

| Requirement | Details |
|------------|---------|
| **Docker** | Installed and running locally |
| **Snowflake** | ACCOUNTADMIN or SYSADMIN role |
| **Snowflake CLI** | `snow` CLI authenticated |
| **SPCS Enabled** | Snowpark Container Services enabled on your account |

## Deployment Steps

### Step 1: Build the Docker image

```bash
cd spcs

# Copy the app file into the build context
cp ../cdc_pipeline_tracker.py .
cp ../.env .   # or create a minimal .env for SPCS

# Build the image
docker build -t cdc_pipeline_tracker:latest .
```

### Step 2: Get the image repository URL

Run in Snowflake worksheet:
```sql
USE DATABASE CUSTOMER_ORDERS_DW;
CREATE IMAGE REPOSITORY IF NOT EXISTS cdc_repo;
SHOW IMAGE REPOSITORIES;
```

Copy the `repository_url` from the output. It looks like:
```
pmxgmsx-ipponusapartner.registry.snowflakecomputing.com/customer_orders_dw/public/cdc_repo
```

### Step 3: Push the image to Snowflake

```bash
# Login to Snowflake Docker registry
docker login <repository_url> -u <your_username>
# Password: use your Snowflake password or PAT

# Tag the image
docker tag cdc_pipeline_tracker:latest <repository_url>/cdc_pipeline_tracker:latest

# Push
docker push <repository_url>/cdc_pipeline_tracker:latest
```

### Step 4: Create compute pool and service

Run `deploy.sql` in Snowflake worksheet — it creates:
1. Image repository
2. Compute pool (CPU_X64_XS, auto-suspend 5 min)
3. Network rule for Claude API egress
4. The service with the container spec

```sql
-- Run deploy.sql in Snowflake worksheet
-- Or use snow CLI:
snow sql -f deploy.sql
```

### Step 5: Get the app URL

```sql
SHOW ENDPOINTS IN SERVICE cdc_pipeline_tracker;
```

The `ingress_url` is your app URL. Share it with your team.

## Authentication in SPCS

Inside SPCS, you have two auth options:

**Option A: OAuth token (recommended)**
The container inherits the service owner's Snowflake session. Modify `cdc_pipeline_tracker.py` to detect SPCS:

```python
import os

if os.path.exists("/snowflake/session/token"):
    # Running inside SPCS — use OAuth token
    with open("/snowflake/session/token") as f:
        token = f.read()
    conn = snowflake.connector.connect(
        host=os.getenv("SNOWFLAKE_HOST"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        token=token,
        authenticator="oauth",
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="CUSTOMER_ORDERS_DW",
        schema="BRONZE",
    )
else:
    # Running locally — use PAT from .env
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        authenticator="programmatic_access_token",
        token=os.getenv("SNOWFLAKE_TOKEN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="CUSTOMER_ORDERS_DW",
        schema="BRONZE",
    )
```

**Option B: PAT in .env (simpler for POC)**
Include the `.env` file in the Docker build. Less secure but works for demos.

## Updating the App

```bash
# Rebuild
docker build -t cdc_pipeline_tracker:latest .
docker tag cdc_pipeline_tracker:latest <repository_url>/cdc_pipeline_tracker:latest
docker push <repository_url>/cdc_pipeline_tracker:latest

# Restart the service in Snowflake
ALTER SERVICE cdc_pipeline_tracker SUSPEND;
ALTER SERVICE cdc_pipeline_tracker RESUME;
```

## Monitoring

```sql
-- Service status
CALL SYSTEM$GET_SERVICE_STATUS('cdc_pipeline_tracker');

-- Container logs
CALL SYSTEM$GET_SERVICE_LOGS('cdc_pipeline_tracker', '0', 'cdc-pipeline-tracker', 100);

-- Compute pool usage
DESCRIBE COMPUTE POOL CDC_POOL;
```

## Cost Estimate

| Component | Cost |
|----------|------|
| Compute Pool (CPU_X64_XS) | ~$0.06/credit, auto-suspends after 5 min idle |
| Image Storage | Minimal — single image ~500MB |
| Network Egress (Claude API) | Standard Snowflake egress rates |

Typical demo usage: **< $5/day** with auto-suspend.

## Cleanup

```sql
DROP SERVICE IF EXISTS cdc_pipeline_tracker;
DROP COMPUTE POOL IF EXISTS CDC_POOL;
DROP IMAGE REPOSITORY IF EXISTS cdc_repo;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS cdc_external_access;
DROP NETWORK RULE IF EXISTS cdc_egress_rule;
```

## Files

```
spcs/
├── DEPLOY_GUIDE.md      ← This file
├── Dockerfile           ← Container build instructions
├── requirements.txt     ← Python dependencies
├── spec.yaml            ← Container service specification
└── deploy.sql           ← Snowflake DDL for SPCS objects
```


# SPCS Deployment Log — CDC Pipeline Tracker

Complete record of every step taken to deploy the CDC Pipeline Tracker to Snowpark Container Services.

---

## Deployment Summary

| Item | Value |
|------|-------|
| **App** | CDC Pipeline Tracker (Streamlit) |
| **Platform** | Snowpark Container Services (SPCS) |
| **Public URL** | https://nad4qkq5-pmxgmsx-ipponusapartner.snowflakecomputing.app |
| **Snowflake Account** | pmxgmsx-ipponusapartner |
| **Database** | CUSTOMER_ORDERS_DW |
| **Compute Pool** | CDC_POOL (CPU_X64_XS, auto-suspend 5min) |
| **Service Name** | CDC_PIPELINE_TRACKER |
| **Image** | /CUSTOMER_ORDERS_DW/PUBLIC/CDC_REPO/cdc_pipeline_tracker:latest |
| **Auth Method** | Programmatic Access Token (PAT) via env vars |
| **Deployed** | 2026-04-10 |

---

## Step 1: Install Docker

**What:** Installed Docker Desktop on macOS.

**Why:** SPCS requires Docker images. Docker Desktop provides the build and push toolchain.

**Result:**
```
Docker version 29.3.1, build c2be9cc
```

---

## Step 2: Create Dockerfile and Build Context

**What:** Created deployment files in `spcs/` directory.

**Files created:**

| File | Purpose |
|------|---------|
| `Dockerfile` | Python 3.10-slim base, installs dependencies, runs Streamlit on port 8501 |
| `requirements.txt` | Pinned versions: streamlit 1.50.0, snowflake-connector 4.4.0, anthropic 0.87.0, pandas, python-dotenv |
| `spec.yaml` | Container service specification (CPU, memory, endpoints) |
| `deploy.sql` | All Snowflake DDL for SPCS objects |
| `DEPLOY_GUIDE.md` | Step-by-step instructions for team members |

**Why:** Separates deployment artifacts from application code. Anyone can rebuild and deploy using these files.

---

## Step 3: Build Docker Image

**What:** Built the image for `linux/amd64` platform.

**Why:** SPCS only supports `amd64` architecture. Mac M-series chips build `arm64` by default, so `--platform linux/amd64` is required.

**Commands:**
```bash
cd spcs
cp ../cdc_pipeline_tracker.py .
cp ../.env .
docker build --platform linux/amd64 -t cdc_pipeline_tracker:latest .
```

**Result:** Image built successfully, ~589MB.

**Lesson learned:** First build without `--platform linux/amd64` was rejected by SPCS with error: "SPCS only supports image for amd64 architecture." Always specify platform explicitly.

---

## Step 4: Create Image Repository in Snowflake

**What:** Created a Snowflake image repository to store Docker images.

**SQL (as SYSADMIN):**
```sql
USE DATABASE CUSTOMER_ORDERS_DW;
USE SCHEMA PUBLIC;
CREATE IMAGE REPOSITORY IF NOT EXISTS CDC_REPO;
```

**Result:**
```
Repository: CDC_REPO
URL: pmxgmsx-ipponusapartner.registry.snowflakecomputing.com/customer_orders_dw/public/cdc_repo
```

---

## Step 5: Create Compute Pool

**What:** Created a dedicated compute pool for the container service.

**SQL (as SYSADMIN):**
```sql
CREATE COMPUTE POOL IF NOT EXISTS CDC_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 300;
```

**Why:**
- `CPU_X64_XS` — smallest instance, sufficient for a Streamlit app
- `AUTO_SUSPEND_SECS = 300` — suspends after 5 min idle to save costs
- `AUTO_RESUME = TRUE` — wakes up when someone accesses the URL

**Estimated cost:** ~$0.06/credit, < $5/day with auto-suspend.

---

## Step 6: Create Network Rules

**What:** Created egress rules so the container can reach external services.

**SQL (as SYSADMIN):**
```sql
CREATE OR REPLACE NETWORK RULE CUSTOMER_ORDERS_DW.PUBLIC.CDC_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.anthropic.com:443');
```

**SQL (as ACCOUNTADMIN):**
```sql
CREATE OR REPLACE NETWORK RULE CUSTOMER_ORDERS_DW.PUBLIC.CDC_SF_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('pmxgmsx-ipponusapartner.snowflakecomputing.com:443');
```

**Why:**
- `CDC_EGRESS_RULE` — allows Claude API calls from the container
- `CDC_SF_EGRESS_RULE` — allows the container to connect back to Snowflake (required for PAT auth)

---

## Step 7: Create External Access Integration

**What:** Created an integration that bundles the network rules and grants them to the service.

**SQL (as ACCOUNTADMIN):**
```sql
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION CDC_EXTERNAL_ACCESS
    ALLOWED_NETWORK_RULES = (
        CUSTOMER_ORDERS_DW.PUBLIC.CDC_EGRESS_RULE,
        CUSTOMER_ORDERS_DW.PUBLIC.CDC_SF_EGRESS_RULE
    )
    ENABLED = TRUE;

GRANT USAGE ON INTEGRATION CDC_EXTERNAL_ACCESS TO ROLE SYSADMIN;
```

**Why:** SPCS containers are network-isolated by default. Without external access integration, the container cannot make any outbound network calls.

---

## Step 8: Push Docker Image to Snowflake

**What:** Pushed the built image to the Snowflake image repository.

**Commands (in terminal):**
```bash
# Login (use PAT as password, not Snowflake password)
docker login pmxgmsx-ipponusapartner.registry.snowflakecomputing.com -u RBATISH

# Tag
docker tag cdc_pipeline_tracker:latest pmxgmsx-ipponusapartner.registry.snowflakecomputing.com/customer_orders_dw/public/cdc_repo/cdc_pipeline_tracker:latest

# Push
docker push pmxgmsx-ipponusapartner.registry.snowflakecomputing.com/customer_orders_dw/public/cdc_repo/cdc_pipeline_tracker:latest
```

**Lesson learned:** Docker login requires the **PAT token** as password, not the Snowflake account password. Using the account password returns "AUTHENTICATION_FAIL".

**Result:** 9 layers pushed, digest `sha256:2d12fbeba7f99acb2863e76d374dbee259881be3a62d3eb4a267b4f8506bb366`

---

## Step 9: Create the SPCS Service

**What:** Created the container service with environment variables and endpoint configuration.

**SQL (as SYSADMIN):**
```sql
CREATE SERVICE CDC_PIPELINE_TRACKER
    IN COMPUTE POOL CDC_POOL
    FROM SPECIFICATION $$
    spec:
      containers:
        - name: cdc-tracker
          image: /CUSTOMER_ORDERS_DW/PUBLIC/CDC_REPO/cdc_pipeline_tracker:latest
          env:
            SNOWFLAKE_ACCOUNT: pmxgmsx-ipponusapartner
            SNOWFLAKE_HOST: pmxgmsx-ipponusapartner.snowflakecomputing.com
            SNOWFLAKE_USER: RBATISH
            SNOWFLAKE_TOKEN: <PAT_TOKEN>
            SNOWFLAKE_WAREHOUSE: BI_WAREHOUSE
            SNOWFLAKE_ROLE: SYSADMIN
            SNOWFLAKE_DATABASE: CUSTOMER_ORDERS_DW
            SNOWFLAKE_SCHEMA: BRONZE
          resources:
            requests:
              cpu: 0.5
              memory: 512M
            limits:
              cpu: 1
              memory: 1G
      endpoints:
        - name: streamlit
          port: 8501
          public: true
    $$
    EXTERNAL_ACCESS_INTEGRATIONS = (CDC_EXTERNAL_ACCESS)
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;
```

**Why PAT instead of OAuth:**
- First tried SPCS OAuth token (`/snowflake/session/token`) — failed with "Client is unauthorized to use Snowpark Container Services OAuth token"
- OAuth requires additional ACCOUNTADMIN grants that were not available
- PAT auth via env vars works immediately and is acceptable for POC/demo

---

## Step 10: Whitelist SPCS Container IP Range

**What:** Added the SPCS container subnet to the Snowflake network policy.

**SQL:**
```sql
ALTER NETWORK POLICY <policy_name> SET ALLOWED_IP_LIST = ('71.62.97.159', '153.45.64.0/24');
```

**Why:** SPCS containers get dynamic IPs from Snowflake's cloud infrastructure. Individual IPs change on restart:
- First attempt: blocked by `153.45.64.130`
- Second attempt: blocked by `153.45.64.129`
- Solution: whitelist entire `/24` subnet (`153.45.64.0` — `153.45.64.255`)

**Lesson learned:** Always whitelist a CIDR range for SPCS, not individual IPs. Container IPs are not stable across restarts.

---

## Issues Encountered & Resolutions

| # | Issue | Root Cause | Resolution |
|---|-------|-----------|-----------|
| 1 | `SPCS only supports image for amd64` | Mac M-series builds arm64 by default | Rebuild with `--platform linux/amd64` |
| 2 | `AUTHENTICATION_FAIL` on docker login | Used Snowflake password instead of PAT | Use PAT token as docker login password |
| 3 | `ConnectionTimeout` from container | Container couldn't reach Snowflake host | Added `CDC_SF_EGRESS_RULE` for `pmxgmsx-ipponusapartner.snowflakecomputing.com:443` |
| 4 | `Client is unauthorized to use SPCS OAuth token` | OAuth requires additional ACCOUNTADMIN grants | Switched to PAT auth via environment variables |
| 5 | `IP 153.45.64.130 is not allowed` | Network policy blocking SPCS container IP | Added container IP to network policy |
| 6 | `IP 153.45.64.129 is not allowed` | Container got different IP after restart | Whitelisted entire `/24` subnet |
| 7 | `Cannot perform CREATE TEMPTABLE` | No current schema set on connection | Added `schema='BRONZE'` to connection params |
| 8 | `Cannot perform CREATE NETWORK RULE` | No current schema in session | Used fully qualified name `CUSTOMER_ORDERS_DW.PUBLIC.CDC_EGRESS_RULE` |

---

## Snowflake Objects Created

| Object | Type | Schema | Created By |
|--------|------|--------|-----------|
| `CDC_REPO` | Image Repository | PUBLIC | SYSADMIN |
| `CDC_POOL` | Compute Pool | — | SYSADMIN |
| `CDC_EGRESS_RULE` | Network Rule | PUBLIC | SYSADMIN |
| `CDC_SF_EGRESS_RULE` | Network Rule | PUBLIC | ACCOUNTADMIN |
| `CDC_EXTERNAL_ACCESS` | External Access Integration | — | ACCOUNTADMIN |
| `CDC_PIPELINE_TRACKER` | Service | PUBLIC | SYSADMIN |

---

## Management Commands

```sql
-- Check service status
CALL SYSTEM$GET_SERVICE_STATUS('CDC_PIPELINE_TRACKER');

-- View container logs
CALL SYSTEM$GET_SERVICE_LOGS('CDC_PIPELINE_TRACKER', '0', 'cdc-tracker', 100);

-- Get public URL
SHOW ENDPOINTS IN SERVICE CDC_PIPELINE_TRACKER;

-- Restart after image update
ALTER SERVICE CDC_PIPELINE_TRACKER SUSPEND;
ALTER SERVICE CDC_PIPELINE_TRACKER RESUME;

-- Suspend to save costs
ALTER SERVICE CDC_PIPELINE_TRACKER SUSPEND;

-- Resume when needed
ALTER SERVICE CDC_PIPELINE_TRACKER RESUME;
```

---

## Update Workflow

When the app code changes:

```bash
# 1. Copy updated files into build context
cd spcs
cp ../cdc_pipeline_tracker.py .
cp ../.env .

# 2. Rebuild for amd64
docker build --platform linux/amd64 -t cdc_pipeline_tracker:latest .

# 3. Tag and push
docker tag cdc_pipeline_tracker:latest pmxgmsx-ipponusapartner.registry.snowflakecomputing.com/customer_orders_dw/public/cdc_repo/cdc_pipeline_tracker:latest
docker push pmxgmsx-ipponusapartner.registry.snowflakecomputing.com/customer_orders_dw/public/cdc_repo/cdc_pipeline_tracker:latest

# 4. Restart service (in Snowflake)
ALTER SERVICE CDC_PIPELINE_TRACKER SUSPEND;
ALTER SERVICE CDC_PIPELINE_TRACKER RESUME;
```

---

## Cleanup (when no longer needed)

```sql
DROP SERVICE IF EXISTS CDC_PIPELINE_TRACKER;
DROP COMPUTE POOL IF EXISTS CDC_POOL;
DROP IMAGE REPOSITORY IF EXISTS CDC_REPO;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS CDC_EXTERNAL_ACCESS;
DROP NETWORK RULE IF EXISTS CUSTOMER_ORDERS_DW.PUBLIC.CDC_EGRESS_RULE;
DROP NETWORK RULE IF EXISTS CUSTOMER_ORDERS_DW.PUBLIC.CDC_SF_EGRESS_RULE;
```

---

## Files

```
spcs/
├── DEPLOYMENT_LOG.md      ← This file
├── DEPLOY_GUIDE.md        ← Team deployment instructions
├── Dockerfile             ← Container build (Python 3.10, amd64)
├── requirements.txt       ← Pinned Python dependencies
├── spec.yaml              ← Container service specification
├── deploy.sql             ← All Snowflake DDL
├── cdc_pipeline_tracker.py ← App copy (build context)
└── .env                   ← Credentials (build context, gitignored)
```

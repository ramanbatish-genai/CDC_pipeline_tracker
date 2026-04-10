# Snowpark Container Services (SPCS) — What We Built & Why It Matters

A production-ready Streamlit app deployed and running entirely inside Snowflake.

---

## What We Did

Deployed a **CDC Pipeline Tracker** as a containerized application running natively inside Snowflake — no external servers, no cloud VMs, no separate hosting. The app is accessible via a public URL that Snowflake provisions and manages.

**Live URL:** https://nad4qkq5-pmxgmsx-ipponusapartner.snowflakecomputing.app

---

## Why This Is Significant

| Traditional Deployment | SPCS Deployment |
|----------------------|-----------------|
| App runs on AWS/Azure/GCP VM | App runs **inside Snowflake** |
| Data leaves Snowflake to reach the app | Data **never leaves** the platform |
| Separate infra to provision, patch, monitor | Snowflake manages compute, scaling, networking |
| Multiple credentials across systems | Single Snowflake auth for everything |
| You pay for VM + data transfer + Snowflake | You pay only **Snowflake credits** |
| App goes down → you fix it | Snowflake auto-restarts, auto-suspends, auto-resumes |

**Bottom line:** SPCS collapses the entire deployment stack into Snowflake. No Kubernetes, no Terraform, no Docker Compose in production. You build a container, push it, and Snowflake runs it.

---

## How It Works

```
Developer's Machine                          Snowflake
─────────────────                          ──────────

┌──────────────────┐     docker push     ┌───────────────────────────────┐
│                  │ ──────────────────→  │  IMAGE REPOSITORY             │
│  Docker Build    │                     │  (CDC_REPO)                   │
│  (Dockerfile)    │                     │  Stores your container image  │
│                  │                     └──────────┬────────────────────┘
└──────────────────┘                                │
                                                    │ CREATE SERVICE
                                                    ▼
                                         ┌───────────────────────────────┐
                                         │  COMPUTE POOL                 │
                                         │  (CDC_POOL)                   │
                                         │                               │
                                         │  ┌─────────────────────────┐  │
                                         │  │  YOUR CONTAINER         │  │
                                         │  │                         │  │
                                         │  │  Streamlit App          │  │
                                         │  │  Python + Snowflake SDK │  │
                                         │  │  Port 8501              │  │
                                         │  └────────┬────────────────┘  │
                                         │           │                   │
                                         │     ┌─────┴─────┐            │
                                         │     │ SNOWFLAKE  │            │
                                         │     │ TABLES     │            │
                                         │     │ (direct)   │            │
                                         │     └───────────┘            │
                                         └───────────────────────────────┘
                                                    │
                                                    │ Public Endpoint
                                                    ▼
                                         ┌───────────────────────────────┐
                                         │  https://xxx.snowflakecomputing│
                                         │  .app                         │
                                         │                               │
                                         │  Browser → Streamlit UI       │
                                         │  No VPN needed                │
                                         │  Snowflake manages SSL/TLS    │
                                         └───────────────────────────────┘
```

---

## What We Proved

### 1. Full-Stack App Inside Snowflake
A Streamlit app with real-time CDC tracking (Bronze → Silver → Gold), DQ checks, SCD Type 2, and before/after diffs — all running inside Snowflake compute.

### 2. External API Calls from Inside Snowflake
The container calls the **Claude API** (Anthropic) for NL→SQL and AI features. This required:
- Network egress rules (whitelisting `api.anthropic.com`)
- External access integration
- Proof that SPCS can reach external services while keeping data inside Snowflake

### 3. Auto-Suspend / Auto-Resume
The compute pool suspends after 5 minutes of idle — costs drop to zero. When someone hits the URL, it resumes automatically. This makes SPCS economical for internal tools and demos.

### 4. Public URL Without Infrastructure
Snowflake provisions a public HTTPS endpoint automatically. No load balancer, no DNS configuration, no SSL certificate management. Share the URL and it works.

---

## The 5 SPCS Components

| Component | What It Is | Analogy |
|-----------|-----------|---------|
| **Image Repository** | Docker registry inside Snowflake | Like Docker Hub, but private to your account |
| **Compute Pool** | CPU/GPU resources for containers | Like a Kubernetes node pool |
| **Network Rules** | Firewall rules for container egress | Like security groups |
| **External Access Integration** | Bundles network rules into a grantable object | Like an IAM policy |
| **Service** | Running container with endpoints | Like a Kubernetes deployment + service |

---

## What This Enables

### Immediate Use Cases
| Use Case | What SPCS Does | Client Value |
|----------|---------------|-------------|
| **Internal Dashboards** | Run Streamlit/Dash apps inside Snowflake | No separate hosting, data never leaves |
| **ML Model Serving** | Deploy prediction APIs as containers | Real-time scoring on Snowflake data |
| **Custom ETL** | Run Python/Java/Go pipelines as services | Beyond what Tasks/Procedures can do |
| **AI Agents** | Host LLM-powered apps with Snowflake data access | Claude/GPT calling Snowflake directly |
| **Client-Facing Apps** | Public endpoints for external users | Replace entire app hosting stack |

### Strategic Value
| Differentiator | Why It Matters |
|---------------|---------------|
| **Collapses infrastructure** | No need for AWS/Azure app hosting alongside Snowflake |
| **Data governance** | Data stays in Snowflake for every operation — critical for FSI/healthcare |
| **Cost efficiency** | Auto-suspend means you pay only when apps are used |
| **Speed to demo** | Build → push → URL in under an hour (once setup is done) |

---

## Deployment in Numbers

| Metric | Value |
|--------|-------|
| Time from zero to live URL | ~2 hours (including troubleshooting) |
| Docker image size | 589 MB |
| Compute pool instance | CPU_X64_XS (smallest) |
| Auto-suspend | 5 minutes idle |
| Estimated daily cost (demo usage) | < $5 |
| Lines of application code | ~500 (single Python file) |
| Snowflake objects created | 6 (repo, pool, 2 rules, integration, service) |
| Issues resolved during deployment | 8 (documented in DEPLOYMENT_LOG.md) |

---

## Key Lessons for SPCS Deployments

| Lesson | Detail |
|--------|--------|
| **Always build for `linux/amd64`** | Mac M-series builds arm64 by default. Use `docker build --platform linux/amd64` |
| **Use PAT for docker login** | Snowflake password doesn't work for registry auth. PAT token works. |
| **Whitelist CIDR, not individual IPs** | Container IPs change on restart. Use `/24` subnet range. |
| **PAT > OAuth for POC** | SPCS OAuth requires complex ACCOUNTADMIN grants. PAT via env vars works immediately. |
| **External access is opt-in** | Containers can't reach ANY external endpoint by default. Every hostname must be explicitly whitelisted. |
| **Snowflake egress needs its own rule** | Even connecting back to Snowflake requires a network rule + external access integration. |
| **Auto-suspend saves money** | 5-min auto-suspend means the pool costs nothing overnight/weekends. |
| **Public endpoints take 1-2 min to provision** | `SHOW ENDPOINTS` may show "provisioning" — just wait. |

---

## Replication Guide

For any project to deploy an SPCS app:

```bash
# 1. Build (always specify platform)
docker build --platform linux/amd64 -t my_app:latest .

# 2. Login to Snowflake registry (use PAT as password)
docker login <account>.registry.snowflakecomputing.com -u <username>

# 3. Tag and push
docker tag my_app:latest <repo_url>/my_app:latest
docker push <repo_url>/my_app:latest

# 4. Create service in Snowflake
CREATE SERVICE my_app
    IN COMPUTE POOL my_pool
    FROM SPECIFICATION $$ ... $$
    EXTERNAL_ACCESS_INTEGRATIONS = (my_access)
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

# 5. Get URL
SHOW ENDPOINTS IN SERVICE my_app;
```

Total commands: **5**. Time: **< 30 minutes** (after initial setup).

---

## Files in This Directory

```
spcs/
├── SPCS_OVERVIEW.md       ← This file — executive summary
├── DEPLOYMENT_LOG.md      ← Detailed step-by-step with every command and error
├── DEPLOY_GUIDE.md        ← Team instructions for replication
├── Dockerfile             ← Container definition
├── requirements.txt       ← Python dependencies
├── spec.yaml              ← Service specification
└── deploy.sql             ← All Snowflake DDL
```


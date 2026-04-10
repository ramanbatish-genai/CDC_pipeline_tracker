
# SPCS Deployment — Talking Points

Key highlights for demos and presentations.

---

## 4 Things That Matter

1. **Full-stack app running inside Snowflake** — no AWS/Azure/GCP VMs, no Kubernetes, no separate hosting. One platform.

2. **Data never leaves Snowflake** — the app queries Snowflake tables from inside the same platform. Critical for regulated industries (FSI, healthcare, insurance).

3. **External AI calls from inside Snowflake** — proved that a container running in SPCS can call the Claude API (Anthropic) for NL→SQL and AI features while keeping data in-platform.

4. **Zero infrastructure management** — auto-suspend when idle ($0 cost), auto-resume on access, public HTTPS endpoint provisioned automatically. No DevOps needed.

---

## One-Liners

- "We deployed a Streamlit app inside Snowflake — no VMs, no Kubernetes. Just push a container and get a URL."
- "Data stays in Snowflake the entire time. The app runs where the data lives."
- "Auto-suspend means it costs nothing when no one is using it. Under $5/day during active demos."
- "We call Claude API from inside Snowflake — AI features without moving data out of the platform."
- "5 commands to deploy. 30 minutes from code to live URL."

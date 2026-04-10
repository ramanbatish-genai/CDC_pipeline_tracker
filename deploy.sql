
-- ============================================================
-- SPCS Deployment for CDC Pipeline Tracker
-- Run these in Snowflake worksheet (as ACCOUNTADMIN or SYSADMIN)
-- ============================================================

-- Step 1: Create image repository
USE DATABASE CUSTOMER_ORDERS_DW;
USE SCHEMA PUBLIC;

CREATE IMAGE REPOSITORY IF NOT EXISTS cdc_repo;

-- Get the repository URL (need this for docker push)
SHOW IMAGE REPOSITORIES;
-- Copy the repository_url from output, e.g.:
-- pmxgmsx-ipponusapartner.registry.snowflakecomputing.com/customer_orders_dw/public/cdc_repo

-- Step 2: Create compute pool
CREATE COMPUTE POOL IF NOT EXISTS CDC_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 300;

-- Check pool status (wait until ACTIVE)
DESCRIBE COMPUTE POOL CDC_POOL;

-- Step 3: Create network rule for external access (Claude API)
CREATE OR REPLACE NETWORK RULE cdc_egress_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.anthropic.com:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION cdc_external_access
    ALLOWED_NETWORK_RULES = (cdc_egress_rule)
    ENABLED = TRUE;

-- Step 4: Create the service
CREATE SERVICE IF NOT EXISTS cdc_pipeline_tracker
    IN COMPUTE POOL CDC_POOL
    FROM SPECIFICATION $$
    spec:
      containers:
        - name: cdc-pipeline-tracker
          image: /CUSTOMER_ORDERS_DW/PUBLIC/cdc_repo/cdc_pipeline_tracker:latest
          env:
            SNOWFLAKE_ACCOUNT: pmxgmsx-ipponusapartner
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
    EXTERNAL_ACCESS_INTEGRATIONS = (cdc_external_access)
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- Step 5: Check service status
DESCRIBE SERVICE cdc_pipeline_tracker;
CALL SYSTEM$GET_SERVICE_STATUS('cdc_pipeline_tracker');

-- Step 6: Get the endpoint URL
SHOW ENDPOINTS IN SERVICE cdc_pipeline_tracker;
-- The 'ingress_url' is your app URL

-- ============================================================
-- Useful management commands
-- ============================================================

-- View logs
CALL SYSTEM$GET_SERVICE_LOGS('cdc_pipeline_tracker', '0', 'cdc-pipeline-tracker', 100);

-- Restart service (after image update)
ALTER SERVICE cdc_pipeline_tracker SUSPEND;
ALTER SERVICE cdc_pipeline_tracker RESUME;

-- Drop everything (cleanup)
-- DROP SERVICE cdc_pipeline_tracker;
-- DROP COMPUTE POOL CDC_POOL;
-- DROP IMAGE REPOSITORY cdc_repo;

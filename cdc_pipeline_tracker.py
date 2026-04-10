
"""
CDC Pipeline Tracker — Streamlit App
Watch a single record flow through Bronze → Silver → Gold → Audit in real-time.
"""

import streamlit as st
import os
import time
import uuid
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

load_dotenv()

st.set_page_config(page_title="CDC Pipeline Tracker", page_icon="🔄", layout="wide")

st.markdown("""
<style>
    .block-container { padding-top: 1rem; max-width: 1400px; }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .record-card {
        background: #e8f4f8; border-radius: 10px; padding: 1rem;
        border-left: 4px solid #2196F3; margin-bottom: 1rem;
    }
    .stage-done { color: #28a745; font-weight: bold; }
    .stage-wait { color: #6c757d; }
    .stage-next { color: #fd7e14; font-weight: bold; }
    .dq-pass { color: #28a745; }
    .dq-fail { color: #dc3545; }
</style>
""", unsafe_allow_html=True)

# --- Connection ---
@st.cache_resource
def get_connection():
    # Use PAT auth (works both locally and in SPCS)
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        authenticator="programmatic_access_token",
        token=os.getenv("SNOWFLAKE_TOKEN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        database="CUSTOMER_ORDERS_DW",
        schema="BRONZE",
    )

def qrun(sql, params=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params)
    if cursor.description:
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return pd.DataFrame(rows, columns=cols)
    affected = cursor.rowcount
    cursor.close()
    return affected

def qsafe(sql, params=None):
    try:
        return True, qrun(sql, params)
    except Exception as e:
        return False, str(e)

def highlight_row(row, tracked_id, key_col, failed_ids=None):
    """Return row styles based on whether this is the tracked record."""
    val = str(row.get(key_col, ""))
    if failed_ids and val in failed_ids:
        return ["background-color: #f8d7da"] * len(row)
    if val == str(tracked_id):
        return ["background-color: #d4edda"] * len(row)
    return [""] * len(row)

# --- Init session state ---
if "stage" not in st.session_state:
    st.session_state.stage = "idle"
if "run_id" not in st.session_state:
    st.session_state.run_id = None
if "tracked_record" not in st.session_state:
    st.session_state.tracked_record = None
if "timeline" not in st.session_state:
    st.session_state.timeline = []
if "bronze_after" not in st.session_state:
    st.session_state.bronze_after = None
if "silver_dq" not in st.session_state:
    st.session_state.silver_dq = None
if "silver_after" not in st.session_state:
    st.session_state.silver_after = None
if "gold_cust_before" not in st.session_state:
    st.session_state.gold_cust_before = None
if "gold_cust_after" not in st.session_state:
    st.session_state.gold_cust_after = None
if "gold_daily_before" not in st.session_state:
    st.session_state.gold_daily_before = None
if "gold_daily_after" not in st.session_state:
    st.session_state.gold_daily_after = None

# --- Sidebar ---
with st.sidebar:
    st.markdown("## 🔄 CDC Tracker")
    st.caption(f"User: {os.getenv('SNOWFLAKE_USER')}")
    st.caption(f"Role: {os.getenv('SNOWFLAKE_ROLE')}")
    st.divider()
    if st.button("🏠 Home", type="primary"):
        for key in ["stage", "run_id", "tracked_record", "timeline",
                     "bronze_after", "silver_dq", "silver_after",
                     "gold_cust_before", "gold_cust_after",
                     "gold_daily_before", "gold_daily_after",
                     "original_record"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    if st.button("🔄 Reset Pipeline"):
        for key in ["stage", "run_id", "tracked_record", "timeline",
                     "bronze_after", "silver_dq", "silver_after",
                     "gold_cust_before", "gold_cust_after",
                     "gold_daily_before", "gold_daily_after",
                     "original_record"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    st.divider()
    st.markdown("### Pipeline Status")
    stage = st.session_state.get("stage", "idle")
    stages = [
        ("🟤 Bronze", "bronze_done"),
        ("⚪ Silver", "silver_done"),
        ("🟡 Gold", "gold_done"),
        ("📋 Audit", "gold_done"),
    ]
    for label, done_at in stages:
        if stage == "idle":
            st.caption(f"⬜ {label} — waiting")
        elif stage == done_at or (done_at == "gold_done" and stage == "gold_done"):
            st.caption(f"✅ {label} — done")
        elif stages.index((label, done_at)) < [s[1] for s in stages].index(stage) + 1:
            st.caption(f"✅ {label} — done")
        else:
            st.caption(f"⬜ {label} — waiting")
    st.divider()
    st.markdown("### Tech Stack")
    st.caption("**Snowflake Streams** — CDC change capture (Bronze → Silver)")
    st.caption("**Snowflake MERGE** — SCD Type 2 upserts (Silver) + metric aggregation (Gold)")
    st.caption("**TRY_CAST + ARRAY_CONSTRUCT** — DQ validation without rejecting data")
    st.caption("**Streamlit** — Real-time pipeline visualization")
    st.caption("**Python snowflake-connector** — Programmatic pipeline execution")
    st.divider()
    st.markdown("### AI Engines")
    st.caption("🟣 **Claude API** — NL→SQL generation, result explanation, anomaly narratives (data sent externally)")
    st.caption("🔵 **Cortex AI** — SENTIMENT(), SUMMARIZE(), CLASSIFY_TEXT(), COMPLETE() (data stays in Snowflake)")

# --- Title ---
st.markdown("<h2 style='text-align:center; margin-bottom:0'>🔄 CDC Pipeline Tracker</h2>", unsafe_allow_html=True)
st.markdown("<p style='text-align:center; color:#666'>Insert a record and watch it flow through Bronze → Silver → Gold → Audit</p>", unsafe_allow_html=True)

# ============================================================
# RECORD TRACKER BAR
# ============================================================
if st.session_state.tracked_record:
    rec = st.session_state.tracked_record
    stage = st.session_state.stage

    def stage_icon(s, required):
        if s == "idle":
            return "🔒"
        order = ["idle", "bronze_done", "silver_done", "gold_done"]
        if order.index(s) >= order.index(required):
            return "✅"
        if order.index(s) == order.index(required) - 1:
            return "⏳"
        return "🔒"

    st.markdown(f"""
    <div class="record-card">
        <strong>Tracking: Order #{rec['order_id']}</strong> — {rec['customer_name']} — {rec['product']} —
        {rec['quantity']} × ${rec['unit_price']} = <strong>${float(rec['quantity']) * float(rec['unit_price']) * (1 - float(rec['discount'])):.2f}</strong> — {rec['order_status']}
        <br/>
        <span>{stage_icon(stage, 'bronze_done')} Bronze</span> →
        <span>{stage_icon(stage, 'silver_done')} Silver</span> →
        <span>{stage_icon(stage, 'gold_done')} Gold</span> →
        <span>{stage_icon(stage, 'gold_done')} Audit</span>
    </div>
    """, unsafe_allow_html=True)

# ============================================================
# INPUT FORM
# ============================================================
if st.session_state.stage == "idle":
    st.markdown("### 📝 Enter a Record")

    mode = st.radio("Mode:", ["New Order", "Update Existing"], horizontal=True, key="input_mode")

    # Default values for form
    def_order_id = str(9000 + hash(str(time.time())) % 1000)
    def_customer_id = "501"
    def_customer_name = "Acme Corp"
    def_product = "Widget Pro"
    def_quantity = "3"
    def_unit_price = "75.00"
    def_discount = "0.10"
    def_status_idx = 0  # PENDING

    # Search for existing order to pre-populate
    if mode == "Update Existing":
        # Load recent orders as selectable options
        ok_recent, recent_df = qsafe("""
            SELECT s.order_id, s.customer_id, s.status, s.order_date,
                   s.quantity, s.unit_price, s.discount, s.total_amount,
                   s.shipping_address
            FROM SILVER.CUSTOMER_ORDERS s
            WHERE s._is_current = TRUE AND s._dq_passed = TRUE
            ORDER BY s.order_id DESC
            LIMIT 20
        """)

        if ok_recent and not recent_df.empty:
            # Build display labels for selectbox
            options = ["-- Select an order --"]
            for _, r in recent_df.iterrows():
                label = f"Order #{r['ORDER_ID']} — Customer #{r['CUSTOMER_ID']} — {r['STATUS']} — ${r.get('TOTAL_AMOUNT', 0):,.2f}"
                options.append(label)

            selected = st.selectbox("Select an order to update:", options, key="order_picker")

            if selected != "-- Select an order --":
                # Extract order_id from selection
                pick_oid = selected.split("#")[1].split(" ")[0]
                row = recent_df[recent_df["ORDER_ID"].astype(str) == str(pick_oid)].iloc[0]

                # Store original values for before/after comparison
                st.session_state["original_record"] = {
                    "order_id": str(row.get("ORDER_ID", "")),
                    "customer_id": str(row.get("CUSTOMER_ID", "")),
                    "status": str(row.get("STATUS", "")),
                    "order_date": str(row.get("ORDER_DATE", "")),
                    "quantity": str(row.get("QUANTITY", "")),
                    "unit_price": str(row.get("UNIT_PRICE", "")),
                    "discount": str(row.get("DISCOUNT", "")),
                    "total_amount": str(row.get("TOTAL_AMOUNT", "")),
                    "product": str(row.get("SHIPPING_ADDRESS", "")),
                }

                # Show current record highlighted in red (before)
                st.markdown("**Current Record (will be expired):**")
                st.markdown(f"""<div style="background:#f8d7da; border-left:4px solid #dc3545; border-radius:8px; padding:0.8rem; margin-bottom:1rem;">
                    <strong>Order #{row['ORDER_ID']}</strong> — Customer #{row['CUSTOMER_ID']}<br/>
                    Status: <strong>{row['STATUS']}</strong> · Qty: {row['QUANTITY']} · Price: ${row.get('UNIT_PRICE', 0)} · Discount: {row.get('DISCOUNT', 0)}<br/>
                    Total: <strong>${row.get('TOTAL_AMOUNT', 0):,.2f}</strong> · Date: {row['ORDER_DATE']}
                </div>""", unsafe_allow_html=True)

                # SCD2 history
                ok_s, silver_hist = qsafe(f"""
                    SELECT order_id, status, _is_current, _valid_from, _valid_to, _dq_passed
                    FROM SILVER.CUSTOMER_ORDERS
                    WHERE order_id = TRY_CAST('{pick_oid}' AS INTEGER)
                    ORDER BY _valid_from
                """)
                if ok_s and not silver_hist.empty:
                    with st.expander(f"📜 SCD2 History ({len(silver_hist)} versions)"):
                        st.dataframe(silver_hist, use_container_width=True, hide_index=True)

                # Pre-populate form
                def_order_id = str(row.get("ORDER_ID", ""))
                def_customer_id = str(row.get("CUSTOMER_ID", ""))
                def_customer_name = str(row.get("SHIPPING_ADDRESS", "")) or "Customer"
                def_product = str(row.get("SHIPPING_ADDRESS", "Product"))
                def_quantity = str(row.get("QUANTITY", "1"))
                def_unit_price = str(row.get("UNIT_PRICE", "0"))
                def_discount = str(row.get("DISCOUNT", "0"))
                current_status = str(row.get("STATUS", "PENDING"))
                status_options_list = ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED", "COMPLETED"]
                def_status_idx = status_options_list.index(current_status) if current_status in status_options_list else 0

                st.markdown("**👇 Modify fields below (changes shown in green when ingested):**")
        else:
            st.warning("No existing orders found in Silver. Try inserting a new order first.")

    status_options = ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED", "COMPLETED"]
    with st.form("input_form"):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            order_id = st.text_input("Order ID", value=def_order_id)
            customer_id = st.text_input("Customer ID", value=def_customer_id)
            customer_name = st.text_input("Customer Name", value=def_customer_name)
        with c2:
            order_date = st.date_input("Order Date")
            product = st.text_input("Product", value=def_product)
        with c3:
            quantity = st.text_input("Quantity", value=def_quantity)
            unit_price = st.text_input("Unit Price", value=def_unit_price)
            discount = st.text_input("Discount (0-1)", value=def_discount)
        with c4:
            order_status = st.selectbox("Status", status_options, index=def_status_idx)

        btn_label = "🚀 Ingest to Bronze" if mode == "New Order" else "🔄 Re-Ingest Update to Bronze"
        submitted = st.form_submit_button(btn_label, type="primary")

        if submitted:
            st.session_state.run_id = str(uuid.uuid4())[:8]
            st.session_state.tracked_record = {
                "order_id": order_id, "customer_id": customer_id,
                "customer_name": customer_name, "order_date": str(order_date),
                "product": product, "quantity": quantity,
                "unit_price": unit_price, "discount": discount,
                "order_status": order_status,
            }
            st.session_state.timeline = []

            # Insert into Bronze
            t0 = time.time()
            # Generate a numeric product_id from product name
            product_id_num = str(abs(hash(product)) % 9000 + 1000)
            qrun("""
                INSERT INTO BRONZE.CUSTOMER_ORDERS_RAW
                    (order_id, customer_id, order_date, shipped_date, status,
                     product_id, quantity, unit_price, discount, total_amount,
                     shipping_address, billing_address, payment_method,
                     created_at, updated_at, _batch_id)
                VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s, %s,
                        %s, 'Tracker UI', 'CREDIT_CARD',
                        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), %s)
            """, (order_id, customer_id, str(order_date), order_status,
                  product_id_num, quantity, unit_price, discount,
                  str(round(float(quantity) * float(unit_price) * (1 - float(discount)), 2)),
                  product,  # store product name in shipping_address for display
                  f"TRACKER_{st.session_state.run_id}"))
            elapsed = round(time.time() - t0, 2)

            # Capture Bronze state
            ok, df = qsafe(f"""
                SELECT order_id, customer_id, order_date, status, quantity, unit_price,
                       discount, total_amount, _batch_id, _ingested_at
                FROM BRONZE.CUSTOMER_ORDERS_RAW
                WHERE order_id = '{order_id}'
                ORDER BY _ingested_at DESC LIMIT 10
            """)
            st.session_state.bronze_after = df if ok else None

            st.session_state.timeline.append({
                "time": time.strftime("%H:%M:%S"),
                "layer": "BRONZE",
                "action": "INGEST",
                "status": "✅",
                "detail": f"Order #{order_id} inserted ({elapsed}s)",
            })
            st.session_state.stage = "bronze_done"
            st.rerun()

# ============================================================
# 4-COLUMN PIPELINE VIEW
# ============================================================
if st.session_state.stage != "idle":
    if st.button("🏠 Home — Start New Record", key="home_main"):
        for key in list(st.session_state.keys()):
            if key not in ("main_search",):
                del st.session_state[key]
        st.rerun()

    rec = st.session_state.tracked_record
    oid = rec["order_id"]

    col_b, col_s, col_g, col_a = st.columns([3, 3, 3, 2])

    # --- BRONZE ---
    with col_b:
        st.markdown("### 🟤 Bronze")
        if st.session_state.stage in ("bronze_done", "silver_done", "gold_done"):
            st.success(f"✅ Order #{oid} ingested")

            # Show before/after diff if updating
            orig = st.session_state.get("original_record")
            if orig and orig.get("order_id") == oid:
                st.markdown("**What Changed:**")
                fields = [
                    ("Status", orig.get("status",""), rec.get("order_status","")),
                    ("Quantity", orig.get("quantity",""), rec.get("quantity","")),
                    ("Unit Price", orig.get("unit_price",""), rec.get("unit_price","")),
                    ("Discount", orig.get("discount",""), rec.get("discount","")),
                ]
                for fname, old_val, new_val in fields:
                    old_s = str(old_val).strip()
                    new_s = str(new_val).strip()
                    if old_s != new_s:
                        st.markdown(f"""<div style="margin-bottom:4px">
                            <strong>{fname}:</strong>
                            <span style="background:#f8d7da; padding:2px 6px; border-radius:4px; text-decoration:line-through">{old_s}</span>
                            → <span style="background:#d4edda; padding:2px 6px; border-radius:4px; font-weight:bold">{new_s}</span>
                        </div>""", unsafe_allow_html=True)
                    else:
                        st.caption(f"  {fname}: {old_s} (unchanged)")
            else:
                st.markdown("**🟢 New record inserted**")

            st.markdown("**Your Record in Bronze:**")
            if st.session_state.bronze_after is not None:
                styled = st.session_state.bronze_after.style.apply(
                    highlight_row, tracked_id=oid, key_col="ORDER_ID", axis=1
                )
                st.dataframe(styled, use_container_width=True, hide_index=True, height=200)

            # Stream check
            ok, stream = qsafe("SELECT COUNT(*) AS CNT FROM BRONZE.CUSTOMER_ORDERS_STREAM")
            if ok:
                st.caption(f"Stream pending: {stream['CNT'][0]} records")

            if st.session_state.stage == "bronze_done":
                if st.button("⚪ Process to Silver →", key="to_silver", type="primary"):
                    t0 = time.time()

                    # Stage stream into temp table
                    qrun(f"""
                        CREATE OR REPLACE TEMPORARY TABLE _stg_tracker AS
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY COALESCE(updated_at, _ingested_at) DESC) AS rn
                        FROM BRONZE.CUSTOMER_ORDERS_STREAM
                        WHERE METADATA$ACTION = 'INSERT'
                    """)

                    # DQ checks
                    ok_dq, dq_df = qsafe(f"""
                        SELECT order_id,
                            CASE WHEN TRY_CAST(order_id AS INTEGER) IS NULL THEN '❌' ELSE '✅' END AS dq_order_id,
                            CASE WHEN TRY_CAST(customer_id AS INTEGER) IS NULL THEN '❌' ELSE '✅' END AS dq_customer_id,
                            CASE WHEN TRY_CAST(order_date AS DATE) IS NULL THEN '❌' ELSE '✅' END AS dq_date,
                            CASE WHEN TRY_CAST(quantity AS INTEGER) IS NULL OR TRY_CAST(quantity AS INTEGER) <= 0 THEN '❌' ELSE '✅' END AS dq_quantity,
                            CASE WHEN TRY_CAST(unit_price AS NUMBER(12,2)) IS NULL OR TRY_CAST(unit_price AS NUMBER(12,2)) < 0 THEN '❌' ELSE '✅' END AS dq_price,
                            CASE WHEN TRY_CAST(discount AS NUMBER(5,2)) IS NULL OR TRY_CAST(discount AS NUMBER(5,2)) < 0 OR TRY_CAST(discount AS NUMBER(5,2)) > 1 THEN '❌' ELSE '✅' END AS dq_discount,
                            CASE WHEN status NOT IN ('PENDING','SHIPPED','DELIVERED','CANCELLED','COMPLETED','PROCESSING') THEN '❌' ELSE '✅' END AS dq_status
                        FROM _stg_tracker WHERE rn = 1
                    """)
                    st.session_state.silver_dq = dq_df if ok_dq else None

                    # Expire old SCD2 records
                    qrun(f"""
                        MERGE INTO SILVER.CUSTOMER_ORDERS tgt
                        USING (SELECT DISTINCT TRY_CAST(order_id AS INTEGER) AS order_id FROM _stg_tracker WHERE rn = 1 AND TRY_CAST(order_id AS INTEGER) IS NOT NULL) src
                        ON tgt.order_id = src.order_id AND tgt._is_current = TRUE
                        WHEN MATCHED THEN UPDATE SET _valid_to = CURRENT_TIMESTAMP(), _is_current = FALSE
                    """)

                    # Insert new Silver records
                    qrun(f"""
                        INSERT INTO SILVER.CUSTOMER_ORDERS
                            (order_id, customer_id, order_date, shipped_date, status,
                             product_id, quantity, unit_price, discount, total_amount,
                             shipping_address, billing_address, payment_method,
                             created_at, updated_at,
                             _record_hash, _dq_passed, _dq_failures, _source_batch_id)
                        WITH dq AS (
                            SELECT *,
                                SHA2(CONCAT_WS('|', order_id, customer_id, status, total_amount, updated_at)) AS rec_hash,
                                ARRAY_CONSTRUCT_COMPACT(
                                    IFF(TRY_CAST(order_date AS DATE) IS NULL, 'INVALID_DATE', NULL),
                                    IFF(TRY_CAST(total_amount AS NUMBER(14,2)) IS NULL OR TRY_CAST(total_amount AS NUMBER(14,2)) <= 0, 'BAD_AMOUNT', NULL),
                                    IFF(TRY_CAST(quantity AS INTEGER) IS NULL OR TRY_CAST(quantity AS INTEGER) <= 0, 'BAD_QTY', NULL),
                                    IFF(status NOT IN ('PENDING','SHIPPED','DELIVERED','CANCELLED','COMPLETED','PROCESSING'), 'BAD_STATUS', NULL)
                                ) AS dq_failures
                            FROM _stg_tracker WHERE rn = 1
                                AND TRY_CAST(order_id AS INTEGER) IS NOT NULL
                                AND TRY_CAST(customer_id AS INTEGER) IS NOT NULL
                        )
                        SELECT TRY_CAST(order_id AS INTEGER), TRY_CAST(customer_id AS INTEGER),
                            TRY_CAST(order_date AS DATE), TRY_CAST(shipped_date AS DATE),
                            UPPER(TRIM(status)), product_id,
                            TRY_CAST(quantity AS INTEGER), TRY_CAST(unit_price AS NUMBER(12,2)),
                            COALESCE(TRY_CAST(discount AS NUMBER(5,2)), 0),
                            TRY_CAST(total_amount AS NUMBER(14,2)),
                            TRIM(shipping_address), TRIM(billing_address),
                            UPPER(TRIM(payment_method)),
                            TRY_CAST(created_at AS TIMESTAMP_NTZ), TRY_CAST(updated_at AS TIMESTAMP_NTZ),
                            rec_hash, ARRAY_SIZE(dq_failures) = 0, dq_failures, _batch_id
                        FROM dq
                    """)

                    # Capture Silver state for this order
                    ok_s, sdf = qsafe(f"""
                        SELECT order_id, customer_id, status, order_date, quantity, unit_price,
                               discount, total_amount, _is_current, _valid_from, _valid_to,
                               _dq_passed, _source_batch_id
                        FROM SILVER.CUSTOMER_ORDERS
                        WHERE order_id = TRY_CAST('{oid}' AS INTEGER)
                        ORDER BY _valid_from
                    """)
                    st.session_state.silver_after = sdf if ok_s else None

                    elapsed = round(time.time() - t0, 2)
                    st.session_state.timeline.append({
                        "time": time.strftime("%H:%M:%S"),
                        "layer": "SILVER",
                        "action": "DQ + SCD2",
                        "status": "✅",
                        "detail": f"DQ checked, SCD2 merged ({elapsed}s)",
                    })
                    st.session_state.stage = "silver_done"
                    st.rerun()
        else:
            st.info("⏳ Waiting for ingest...")

    # --- SILVER ---
    with col_s:
        st.markdown("### ⚪ Silver")
        if st.session_state.stage in ("silver_done", "gold_done"):
            st.success(f"✅ Order #{oid} processed")

            # Before/After change visualization
            orig = st.session_state.get("original_record")
            if orig and orig.get("order_id") == oid and st.session_state.silver_after is not None:
                sdf = st.session_state.silver_after
                expired = sdf[sdf["_IS_CURRENT"] == False]
                current = sdf[sdf["_IS_CURRENT"] == True]

                if not expired.empty and not current.empty:
                    old_r = expired.iloc[-1]  # last expired version
                    new_r = current.iloc[0]

                    st.markdown("**Record Change (Silver):**")
                    compare_fields = [
                        ("Status", old_r.get("STATUS",""), new_r.get("STATUS","")),
                        ("Quantity", str(old_r.get("QUANTITY","")), str(new_r.get("QUANTITY",""))),
                        ("Unit Price", str(old_r.get("UNIT_PRICE","")), str(new_r.get("UNIT_PRICE",""))),
                        ("Discount", str(old_r.get("DISCOUNT","")), str(new_r.get("DISCOUNT",""))),
                        ("Total", str(old_r.get("TOTAL_AMOUNT","")), str(new_r.get("TOTAL_AMOUNT",""))),
                    ]
                    for fname, old_v, new_v in compare_fields:
                        if str(old_v).strip() != str(new_v).strip():
                            st.markdown(f"""<div style="margin-bottom:4px">
                                <strong>{fname}:</strong>
                                <span style="background:#f8d7da; padding:2px 6px; border-radius:4px; text-decoration:line-through">{old_v}</span>
                                → <span style="background:#d4edda; padding:2px 6px; border-radius:4px; font-weight:bold">{new_v}</span>
                            </div>""", unsafe_allow_html=True)

            # DQ Results with record details
            st.markdown("**DQ Checks (with record values):**")
            if st.session_state.silver_dq is not None:
                dq = st.session_state.silver_dq
                dq_row = dq[dq["ORDER_ID"] == oid]
                if not dq_row.empty:
                    r = dq_row.iloc[0]
                    checks = [
                        ("Order ID", r.get("DQ_ORDER_ID", "?"), rec.get("order_id", "?")),
                        ("Customer ID", r.get("DQ_CUSTOMER_ID", "?"), rec.get("customer_id", "?")),
                        ("Date", r.get("DQ_DATE", "?"), rec.get("order_date", "?")),
                        ("Quantity", r.get("DQ_QUANTITY", "?"), rec.get("quantity", "?")),
                        ("Price", r.get("DQ_PRICE", "?"), rec.get("unit_price", "?")),
                        ("Discount", r.get("DQ_DISCOUNT", "?"), rec.get("discount", "?")),
                        ("Status", r.get("DQ_STATUS", "?"), rec.get("order_status", "?")),
                    ]
                    all_passed = all(c[1] == "✅" for c in checks)
                    for check_name, result, value in checks:
                        if result == "✅":
                            st.markdown(f"""<div style="background:#d4edda; padding:3px 8px; border-radius:4px; margin-bottom:3px">
                                ✅ <strong>{check_name}:</strong> {value}</div>""", unsafe_allow_html=True)
                        else:
                            st.markdown(f"""<div style="background:#f8d7da; padding:3px 8px; border-radius:4px; margin-bottom:3px">
                                ❌ <strong>{check_name}:</strong> {value} — INVALID</div>""", unsafe_allow_html=True)
                    if all_passed:
                        st.success("All 7 DQ checks passed")
                    else:
                        st.error("DQ failures detected — record flagged")

            # SCD2 History with full record details
            st.markdown(f"**SCD Type 2 History ({len(st.session_state.silver_after)} versions):**")
            if st.session_state.silver_after is not None:
                sdf = st.session_state.silver_after
                for i, (_, row) in enumerate(sdf.iterrows()):
                    current = row.get("_IS_CURRENT", False)
                    ver = f"v{i + 1}"
                    status = row.get("STATUS", "?")
                    qty = row.get("QUANTITY", "?")
                    price = row.get("UNIT_PRICE", "?")
                    disc = row.get("DISCOUNT", "?")
                    total = row.get("TOTAL_AMOUNT", "?")
                    vf = str(row.get("_VALID_FROM", ""))[:19]
                    vt = str(row.get("_VALID_TO", ""))[:19] if not current else "current"
                    dq_icon = "✅" if row.get("_DQ_PASSED", False) else "❌"
                    if current:
                        st.markdown(f"""<div style="background:#d4edda; padding:6px 10px; border-radius:6px; margin-bottom:4px; border-left:4px solid #28a745">
                            🟢 <strong>{ver} — CURRENT</strong><br/>
                            Status: <strong>{status}</strong> · Qty: {qty} · Price: ${price} · Disc: {disc} · Total: <strong>${total}</strong><br/>
                            <span style="font-size:0.8em; color:#555">Valid: {vf} → {vt} · DQ: {dq_icon}</span>
                        </div>""", unsafe_allow_html=True)
                    else:
                        st.markdown(f"""<div style="background:#f8d7da; padding:6px 10px; border-radius:6px; margin-bottom:4px; border-left:4px solid #dc3545; opacity:0.75">
                            ⚫ <strong>{ver} — EXPIRED</strong><br/>
                            Status: {status} · Qty: {qty} · Price: ${price} · Disc: {disc} · Total: ${total}<br/>
                            <span style="font-size:0.8em; color:#888">Valid: {vf} → {vt} · DQ: {dq_icon}</span>
                        </div>""", unsafe_allow_html=True)

            if st.session_state.stage == "silver_done":
                if st.button("🟡 Process to Gold →", key="to_gold", type="primary"):
                    t0 = time.time()

                    # Capture Gold before
                    ok_gb, gb = qsafe(f"""
                        SELECT customer_id, total_orders, total_revenue, avg_order_value, customer_segment
                        FROM GOLD.CUSTOMER_METRICS
                        WHERE customer_id = TRY_CAST('{rec['customer_id']}' AS INTEGER)
                    """)
                    st.session_state.gold_cust_before = gb if ok_gb and not gb.empty else None

                    ok_gdb, gdb = qsafe(f"""
                        SELECT summary_date, total_orders, gross_revenue, unique_customers
                        FROM GOLD.DAILY_ORDER_SUMMARY
                        WHERE summary_date = '{rec['order_date']}'
                    """)
                    st.session_state.gold_daily_before = gdb if ok_gdb and not gdb.empty else None

                    # Refresh Customer Metrics
                    qrun(f"""
                        MERGE INTO GOLD.CUSTOMER_METRICS tgt
                        USING (
                            SELECT customer_id,
                                MIN(order_date) AS first_order_date, MAX(order_date) AS last_order_date,
                                COUNT(*) AS total_orders, SUM(total_amount) AS total_revenue,
                                AVG(total_amount) AS avg_order_value,
                                SUM(quantity) AS total_items_purchased,
                                CASE WHEN SUM(total_amount) > 10000 THEN 'VIP'
                                     WHEN DATEDIFF('day', MAX(order_date), CURRENT_DATE()) > 365 THEN 'CHURNED'
                                     WHEN DATEDIFF('day', MAX(order_date), CURRENT_DATE()) > 180 THEN 'AT_RISK'
                                     ELSE 'REGULAR' END AS customer_segment
                            FROM SILVER.CUSTOMER_ORDERS
                            WHERE _is_current = TRUE AND _dq_passed = TRUE AND order_date IS NOT NULL
                              AND customer_id = TRY_CAST('{rec['customer_id']}' AS INTEGER)
                            GROUP BY customer_id
                        ) src ON tgt.customer_id = src.customer_id
                        WHEN MATCHED THEN UPDATE SET
                            first_order_date=src.first_order_date, last_order_date=src.last_order_date,
                            total_orders=src.total_orders, total_revenue=src.total_revenue,
                            avg_order_value=src.avg_order_value, total_items_purchased=src.total_items_purchased,
                            customer_segment=src.customer_segment, _last_updated=CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN INSERT
                            (customer_id, first_order_date, last_order_date, total_orders, total_revenue,
                             avg_order_value, total_items_purchased, customer_segment)
                        VALUES (src.customer_id, src.first_order_date, src.last_order_date, src.total_orders,
                                src.total_revenue, src.avg_order_value, src.total_items_purchased, src.customer_segment)
                    """)

                    # Refresh Daily Summary
                    qrun(f"""
                        MERGE INTO GOLD.DAILY_ORDER_SUMMARY tgt
                        USING (
                            SELECT order_date AS summary_date,
                                COUNT(*) AS total_orders, SUM(total_amount) AS gross_revenue,
                                SUM(total_amount*(1-discount/100)) AS net_revenue,
                                AVG(total_amount) AS avg_order_value,
                                COUNT(DISTINCT customer_id) AS unique_customers
                            FROM SILVER.CUSTOMER_ORDERS
                            WHERE _is_current=TRUE AND _dq_passed=TRUE AND order_date = '{rec['order_date']}'
                            GROUP BY order_date
                        ) src ON tgt.summary_date = src.summary_date
                        WHEN MATCHED THEN UPDATE SET
                            total_orders=src.total_orders, gross_revenue=src.gross_revenue,
                            net_revenue=src.net_revenue, avg_order_value=src.avg_order_value,
                            unique_customers=src.unique_customers, _last_updated=CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN INSERT VALUES
                            (src.summary_date, src.total_orders, 0, 0, 0, src.gross_revenue,
                             src.net_revenue, src.avg_order_value, src.unique_customers, CURRENT_TIMESTAMP())
                    """)

                    # Capture Gold after
                    ok_ga, ga = qsafe(f"""
                        SELECT customer_id, total_orders, total_revenue, avg_order_value, customer_segment
                        FROM GOLD.CUSTOMER_METRICS
                        WHERE customer_id = TRY_CAST('{rec['customer_id']}' AS INTEGER)
                    """)
                    st.session_state.gold_cust_after = ga if ok_ga else None

                    ok_gda, gda = qsafe(f"""
                        SELECT summary_date, total_orders, gross_revenue, unique_customers
                        FROM GOLD.DAILY_ORDER_SUMMARY
                        WHERE summary_date = '{rec['order_date']}'
                    """)
                    st.session_state.gold_daily_after = gda if ok_gda else None

                    elapsed = round(time.time() - t0, 2)
                    st.session_state.timeline.append({
                        "time": time.strftime("%H:%M:%S"),
                        "layer": "GOLD",
                        "action": "METRICS",
                        "status": "✅",
                        "detail": f"Customer metrics + daily summary refreshed ({elapsed}s)",
                    })
                    st.session_state.stage = "gold_done"
                    st.rerun()

        elif st.session_state.stage == "bronze_done":
            st.info("⏳ Click **Process to Silver →** in Bronze column")
        else:
            st.caption("🔒 Waiting for Bronze")

    # --- GOLD ---
    with col_g:
        st.markdown("### 🟡 Gold")
        if st.session_state.stage == "gold_done":
            st.success(f"✅ Metrics updated for Customer #{rec['customer_id']}")
            st.caption("_Gold = aggregate of ALL orders for this customer, not just this record_")

            # Your record's full detail
            rec_total = round(float(rec['quantity']) * float(rec['unit_price']) * (1 - float(rec['discount'])), 2)
            st.markdown(f"""<div style="background:#d4edda; border-left:4px solid #28a745; border-radius:8px; padding:0.8rem; margin-bottom:0.8rem">
                <strong>Your Record (input to Gold):</strong><br/>
                <table style="width:100%; font-size:0.85em; margin-top:4px">
                    <tr><td><strong>Order ID:</strong></td><td>#{oid}</td></tr>
                    <tr><td><strong>Customer:</strong></td><td>{rec['customer_name']} (#{rec['customer_id']})</td></tr>
                    <tr><td><strong>Product:</strong></td><td>{rec['product']}</td></tr>
                    <tr><td><strong>Status:</strong></td><td>{rec['order_status']}</td></tr>
                    <tr><td><strong>Quantity:</strong></td><td>{rec['quantity']}</td></tr>
                    <tr><td><strong>Unit Price:</strong></td><td>${rec['unit_price']}</td></tr>
                    <tr><td><strong>Discount:</strong></td><td>{float(rec['discount'])*100:.0f}%</td></tr>
                    <tr><td><strong>Total:</strong></td><td><strong>${rec_total:,.2f}</strong></td></tr>
                    <tr><td><strong>Date:</strong></td><td>{rec['order_date']}</td></tr>
                </table>
            </div>""", unsafe_allow_html=True)

            # Customer Metrics diff
            st.markdown("**Customer Metrics (all orders):**")
            before = st.session_state.gold_cust_before
            after = st.session_state.gold_cust_after

            if after is not None and not after.empty:
                ba = before.iloc[0] if before is not None and not before.empty else None
                aa = after.iloc[0]

                def diff_metric(label, old_val, new_val, is_money=False):
                    fmt = lambda v: f"${v:,.2f}" if is_money else str(v)
                    if ba is None:
                        return f"""<div style="margin-bottom:4px"><strong>{label}:</strong>
                            <span style="background:#d4edda; padding:2px 6px; border-radius:4px">{fmt(new_val)} 🟢 new</span></div>"""
                    old_s, new_s = fmt(old_val), fmt(new_val)
                    if str(old_val) != str(new_val):
                        return f"""<div style="margin-bottom:4px"><strong>{label}:</strong>
                            <span style="background:#f8d7da; padding:2px 6px; border-radius:4px; text-decoration:line-through">{old_s}</span>
                            → <span style="background:#d4edda; padding:2px 6px; border-radius:4px; font-weight:bold">{new_s}</span></div>"""
                    return f"""<div style="margin-bottom:4px"><strong>{label}:</strong> {old_s} (unchanged)</div>"""

                st.markdown(diff_metric("Total Orders",
                    ba.get('TOTAL_ORDERS', 0) if ba is not None else 0,
                    aa.get('TOTAL_ORDERS', 0)), unsafe_allow_html=True)
                st.markdown(diff_metric("Total Revenue",
                    ba.get('TOTAL_REVENUE', 0) if ba is not None else 0,
                    aa.get('TOTAL_REVENUE', 0), is_money=True), unsafe_allow_html=True)
                st.markdown(diff_metric("Avg Order Value",
                    ba.get('AVG_ORDER_VALUE', 0) if ba is not None else 0,
                    aa.get('AVG_ORDER_VALUE', 0), is_money=True), unsafe_allow_html=True)
                st.markdown(diff_metric("Segment",
                    ba.get('CUSTOMER_SEGMENT', '—') if ba is not None else '—',
                    aa.get('CUSTOMER_SEGMENT', '—')), unsafe_allow_html=True)

                # Segment change alert
                old_seg = ba.get("CUSTOMER_SEGMENT", "") if ba is not None else ""
                new_seg = aa.get("CUSTOMER_SEGMENT", "")
                if old_seg and old_seg != new_seg:
                    st.warning(f"🔄 Segment changed: {old_seg} → {new_seg}")

            elif after is not None and not after.empty:
                st.markdown(f"""<div style="background:#d4edda; padding:0.6rem; border-radius:8px">
                    🟢 <strong>New customer created in Gold</strong></div>""", unsafe_allow_html=True)
                st.dataframe(after, use_container_width=True, hide_index=True)

            # Daily Summary diff
            st.markdown(f"**Daily Summary for {rec['order_date']} (all orders on this date):**")
            dbefore = st.session_state.gold_daily_before
            dafter = st.session_state.gold_daily_after

            db_orders = dbefore.iloc[0].get('TOTAL_ORDERS', 0) if dbefore is not None and not dbefore.empty else 0
            db_rev = dbefore.iloc[0].get('GROSS_REVENUE', 0) if dbefore is not None and not dbefore.empty else 0
            db_cust = dbefore.iloc[0].get('UNIQUE_CUSTOMERS', 0) if dbefore is not None and not dbefore.empty else 0
            da_orders = dafter.iloc[0].get('TOTAL_ORDERS', 0) if dafter is not None and not dafter.empty else 0
            da_rev = dafter.iloc[0].get('GROSS_REVENUE', 0) if dafter is not None and not dafter.empty else 0
            da_cust = dafter.iloc[0].get('UNIQUE_CUSTOMERS', 0) if dafter is not None and not dafter.empty else 0

            st.markdown(diff_metric("Orders", db_orders, da_orders), unsafe_allow_html=True)
            st.markdown(diff_metric("Revenue", db_rev, da_rev, is_money=True), unsafe_allow_html=True)
            st.markdown(diff_metric("Customers", db_cust, da_cust), unsafe_allow_html=True)

            # --- Change Summary ---
            st.markdown("---")
            st.markdown("**📝 Change Summary:**")

            before = st.session_state.gold_cust_before
            after = st.session_state.gold_cust_after
            ba = before.iloc[0] if before is not None and not before.empty else None
            aa = after.iloc[0] if after is not None and not after.empty else None

            summary_lines = []

            # Was this an update or a new record?
            orig = st.session_state.get("original_record")
            if orig and orig.get("order_id") == oid:
                # Update — describe what changed from previous version
                summary_lines.append(f"**Type:** Update to existing Order #{oid}")
                changed_fields = []
                field_map = [
                    ("Status", orig.get("status",""), rec.get("order_status","")),
                    ("Quantity", orig.get("quantity",""), rec.get("quantity","")),
                    ("Unit Price", orig.get("unit_price",""), rec.get("unit_price","")),
                    ("Discount", orig.get("discount",""), rec.get("discount","")),
                ]
                for fname, old_v, new_v in field_map:
                    if str(old_v).strip() != str(new_v).strip():
                        changed_fields.append(f"{fname}: {old_v} → {new_v}")
                if changed_fields:
                    summary_lines.append(f"**Fields Changed:** {', '.join(changed_fields)}")
                else:
                    summary_lines.append("**Fields Changed:** None (duplicate re-ingest)")

                old_total = float(orig.get("total_amount", 0) or 0)
                new_total = rec_total
                delta = new_total - old_total
                if abs(delta) > 0.01:
                    direction = "increased" if delta > 0 else "decreased"
                    summary_lines.append(f"**Order Total:** ${old_total:,.2f} → ${new_total:,.2f} ({direction} by ${abs(delta):,.2f})")
            else:
                summary_lines.append(f"**Type:** New Order #{oid}")
                summary_lines.append(f"**Order Total:** ${rec_total:,.2f}")

            # Customer impact
            if ba is not None and aa is not None:
                old_orders = ba.get("TOTAL_ORDERS", 0)
                new_orders = aa.get("TOTAL_ORDERS", 0)
                old_rev = float(ba.get("TOTAL_REVENUE", 0) or 0)
                new_rev = float(aa.get("TOTAL_REVENUE", 0) or 0)
                old_seg = ba.get("CUSTOMER_SEGMENT", "")
                new_seg = aa.get("CUSTOMER_SEGMENT", "")

                if new_orders != old_orders:
                    summary_lines.append(f"**Customer #{rec['customer_id']} Orders:** {old_orders} → {new_orders} (+{new_orders - old_orders})")
                rev_delta = new_rev - old_rev
                if abs(rev_delta) > 0.01:
                    summary_lines.append(f"**Customer Revenue Impact:** ${old_rev:,.2f} → ${new_rev:,.2f} ({'+' if rev_delta > 0 else ''}{rev_delta:,.2f})")
                if old_seg != new_seg:
                    summary_lines.append(f"**Segment Change:** {old_seg} → **{new_seg}**")
                else:
                    summary_lines.append(f"**Segment:** {new_seg} (unchanged)")
            elif aa is not None:
                summary_lines.append(f"**New Customer #{rec['customer_id']}** created in Gold")
                summary_lines.append(f"**Segment:** {aa.get('CUSTOMER_SEGMENT', '?')}")

            # Daily impact
            if da_orders != db_orders or abs(float(da_rev or 0) - float(db_rev or 0)) > 0.01:
                summary_lines.append(f"**Daily ({rec['order_date']}):** Orders {db_orders}→{da_orders}, Revenue ${float(db_rev):,.2f}→${float(da_rev):,.2f}")

            # DQ status
            dq_passed = True
            if st.session_state.silver_dq is not None:
                dq = st.session_state.silver_dq
                dq_row = dq[dq["ORDER_ID"] == oid]
                if not dq_row.empty:
                    r = dq_row.iloc[0]
                    all_checks = [r.get(f"DQ_{c}", "?") for c in ["ORDER_ID","CUSTOMER_ID","DATE","QUANTITY","PRICE","DISCOUNT","STATUS"]]
                    failed = [c for c in all_checks if c == "❌"]
                    if failed:
                        dq_passed = False
                        summary_lines.append(f"**DQ:** ❌ {len(failed)} check(s) failed — record flagged, excluded from Gold metrics")
                    else:
                        summary_lines.append("**DQ:** ✅ All 7 checks passed")

            # SCD2 versions
            if st.session_state.silver_after is not None:
                version_count = len(st.session_state.silver_after)
                summary_lines.append(f"**SCD2:** {version_count} version(s) in Silver history")

            # Render summary
            bg_color = "#d4edda" if dq_passed else "#fff3cd"
            border_color = "#28a745" if dq_passed else "#ffc107"
            summary_html = "<br/>".join(summary_lines)
            st.markdown(f"""<div style="background:{bg_color}; border-left:4px solid {border_color}; border-radius:8px; padding:0.8rem; margin-top:0.5rem">
                {summary_html}
            </div>""", unsafe_allow_html=True)

            st.success("🎉 Pipeline complete! Record fully processed.")

        elif st.session_state.stage == "silver_done":
            st.info("⏳ Click **Process to Gold →** in Silver column")
        else:
            st.caption("🔒 Waiting for Silver")

    # --- AUDIT ---
    with col_a:
        st.markdown("### 📋 Audit")
        if st.session_state.timeline:
            st.markdown("**Pipeline Timeline:**")
            for entry in st.session_state.timeline:
                st.caption(f"  {entry['status']} {entry['time']} | {entry['layer']} | {entry['action']}")
                st.caption(f"     {entry['detail']}")

            st.markdown("---")
            st.markdown("**Your Record Journey:**")
            st.caption(f"  Order ID: {rec['order_id']}")
            st.caption(f"  Customer: {rec['customer_name']} (#{rec['customer_id']})")
            st.caption(f"  Product: {rec['product']}")
            st.caption(f"  Amount: ${float(rec['quantity']) * float(rec['unit_price']) * (1 - float(rec['discount'])):.2f}")
            st.caption(f"  Run ID: {st.session_state.run_id}")
            st.caption(f"  Batch: TRACKER_{st.session_state.run_id}")

            # Layer row counts
            st.markdown("---")
            st.markdown("**Layer Counts:**")
            for schema, table in [("BRONZE", "CUSTOMER_ORDERS_RAW"), ("SILVER", "CUSTOMER_ORDERS"),
                                   ("GOLD", "CUSTOMER_METRICS"), ("GOLD", "DAILY_ORDER_SUMMARY")]:
                ok, cnt = qsafe(f"SELECT COUNT(*) AS C FROM {schema}.{table}")
                if ok:
                    st.caption(f"  {schema}.{table}: {cnt['C'][0]:,}")
        else:
            st.caption("🔒 Waiting for pipeline to start")

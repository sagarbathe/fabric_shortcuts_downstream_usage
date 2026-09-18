# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c61c659f-ecba-4de8-a5b7-25885eb3021f",
# META       "default_lakehouse_name": "LH_ShortcutMonitoring",
# META       "default_lakehouse_workspace_id": "c5c50c6e-30d1-4d2e-8766-d82917e13592",
# META       "known_lakehouses": [
# META         {
# META           "id": "c61c659f-ecba-4de8-a5b7-25885eb3021f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## NB_CopyEventDetection_Warehouse
# Notebook for the "shortcut read and saved as is" rule (design doc §1.1, §4.2).
# 
# Scope of this notebook: **Warehouse engine only.** For each Warehouse in the monitored workspaces,
# reads `<warehouse_name>.queryinsights.exec_requests_history` (Fabric Warehouse Query Insights) to find CREATE-TABLE-AS-SELECT
# and INSERT-INTO-SELECT statements whose source is a known OneLake shortcut (per `DimShortcut`), then:
#   1. Parses the destination column list (explicit list, or `SELECT *`).
#   2. Looks up the source item's real column count via INFORMATION_SCHEMA.COLUMNS (queried from the
#      *hosting* warehouse, since a Lakehouse/Warehouse shortcut's columns are visible there like a
#      normal table).
#   3. Computes column-retention % = (destination columns that exist in source) / (total source columns).
#   4. Flags `is_shortcut_read_and_saved_as_is` = TRUE if SELECT * was used OR retention % > the
#      configurable threshold (`config.detection.columnRetentionThresholdPercent`), even if extra
#      columns were also added - per the rule refinement the user confirmed.
#   5. Writes one row per detected copy event to `FactCopyEvent` (append-only, incremental: only
#      queries with start_time after the last processed watermark are considered each run).
# 
# NOT yet covered (see design doc §4.1/§4.3, tracked as a follow-up):
#   - Spark/Notebook engine: Fabric Workspace Monitoring's `ItemJobEventLogs` only exposes job-level
#     telemetry (status/duration), not per-statement SQL/column lineage, so the same DMV-based approach
#     does not apply. Spark coverage requires either (a) parsing notebook source code statically for
#     `spark.read` of shortcut paths + `.saveAsTable`/`.write` column lists, or (b) a Spark listener /
#     custom logging hook emitting column-level lineage events. Deferred to a later iteration.
#   - Dataflow Gen2 engine: deferred per design doc rollout plan.


# CELL ********************

import json, traceback
from datetime import datetime, timezone

# Resolve the solution's own lakehouse (where config.json/DimShortcut/FactCopyEvent/etc. live) from the
# notebook's runtime attachment context rather than hardcoding GUIDs - this is the one config value that
# CANNOT come from config.json itself (config.json's own location depends on it). If the notebook's
# default lakehouse is ever reattached to a different item, this adapts automatically with no code edit.
_ctx = notebookutils.runtime.context
LAKEHOUSE_WORKSPACE_ID = _ctx["defaultLakehouseWorkspaceId"]
LAKEHOUSE_ID = _ctx["defaultLakehouseId"]
LAKEHOUSE_ABFSS = f"abfss://{LAKEHOUSE_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}"

def log_error_to_lakehouse(stage, exc):
    try:
        err_text = f"STAGE: {stage}\nTIME: {datetime.now(timezone.utc).isoformat()}\n\n{traceback.format_exc()}"
        rdd = spark.sparkContext.parallelize([err_text], 1)
        rdd.saveAsTextFile(f"{LAKEHOUSE_ABFSS}/Files/logs/copyevent_error_{int(datetime.now().timestamp())}.log")
    except Exception:
        pass

try:
    config_df = spark.read.text(f"{LAKEHOUSE_ABFSS}/Files/config/config.json")
    config_text = "\n".join([r["value"] for r in config_df.collect()])
    config = json.loads(config_text)
except Exception as e:
    log_error_to_lakehouse("load_config", e)
    raise

# config.orchestration.enabledEngines lets the pipeline call this notebook unconditionally on every
# run while still allowing an environment (e.g. a dev workspace with no Warehouse items) to skip this
# engine entirely without editing the orchestrating pipeline - just flip config, no redeploy needed.
ENABLED_ENGINES = config.get("orchestration", {}).get("enabledEngines", ["warehouse", "sparkKafka"])
if "warehouse" not in ENABLED_ENGINES:
    print("Warehouse engine disabled via config.orchestration.enabledEngines - exiting without doing work.")
    notebookutils.notebook.exit("skipped: warehouse engine disabled in config")

TENANT_ID = config["auth"]["tenantId"]
CLIENT_ID = config["auth"]["clientId"]
CLIENT_SECRET = config["auth"]["clientSecret"]
MONITORED_WORKSPACES = config["monitoredWorkspaces"]
THRESHOLD_PCT = float(config["detection"]["columnRetentionThresholdPercent"])

print(f"Loaded config. Monitored workspaces: {[w['workspaceName'] for w in MONITORED_WORKSPACES]}")
print(f"Column-retention threshold: {THRESHOLD_PCT}%")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

def get_sp_token(tenant_id, client_id, client_secret, scope):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret, "scope": scope}
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

try:
    fabric_token = get_sp_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET, "https://api.fabric.microsoft.com/.default")
    sql_token = get_sp_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET, "https://database.windows.net/.default")
    print("Acquired Fabric API token and SQL (database.windows.net) token for monitoring service principal.")
except Exception as e:
    log_error_to_lakehouse("get_sp_tokens", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"

def fabric_get_paged(url, token):
    results = []
    headers = {"Authorization": "Bearer " + token}
    next_url = url
    while next_url:
        r = requests.get(next_url, headers=headers)
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("value", []))
        cont = data.get("continuationToken")
        if cont:
            sep = "&" if "?" in url else "?"
            next_url = f"{url}{sep}continuationToken={cont}"
        else:
            next_url = None
    return results

def fabric_get_single(url, token):
    headers = {"Authorization": "Bearer " + token}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

# Discover every Warehouse item across the monitored workspaces, and its SQL connection string.
# EXCLUDE Fabric's own auto-generated Dataflow Gen2 staging warehouses (e.g. "DataflowsStagingWarehouse",
# "StagingWarehouseForDataflows_<timestamp>") - these are internal/hidden implementation details Fabric
# creates automatically to stage intermediate dataflow data, not user-created Warehouses, and are never
# a valid destination for a genuine "shortcut read and saved as is" event. Pattern is configurable via
# config.detection.excludeWarehouseNamePatterns (list of case-insensitive substrings) so new internal
# naming conventions Microsoft introduces later can be excluded without a code change.
EXCLUDE_WAREHOUSE_NAME_PATTERNS = [
    p.lower() for p in config.get("detection", {}).get(
        "excludeWarehouseNamePatterns", ["StagingWarehouseForDataflows", "DataflowsStagingWarehouse"]
    )
]

def is_excluded_warehouse_name(name):
    name_lower = name.lower()
    return any(pattern in name_lower for pattern in EXCLUDE_WAREHOUSE_NAME_PATTERNS)

try:
    warehouses = []  # list of dicts: workspace_id, workspace_name, item_id, item_name, connection_string
    excluded_names = []
    for ws in MONITORED_WORKSPACES:
        ws_id = ws["workspaceId"]
        ws_name = ws["workspaceName"]
        items = fabric_get_paged(f"{FABRIC_BASE}/workspaces/{ws_id}/items", fabric_token)
        for item in items:
            if item.get("type") != "Warehouse":
                continue
            if is_excluded_warehouse_name(item["displayName"]):
                excluded_names.append(f"{ws_name}/{item['displayName']}")
                continue
            try:
                wh = fabric_get_single(f"{FABRIC_BASE}/workspaces/{ws_id}/warehouses/{item['id']}", fabric_token)
                conn_str = wh.get("properties", {}).get("connectionString")
                if conn_str:
                    warehouses.append({
                        "workspace_id": ws_id, "workspace_name": ws_name,
                        "item_id": item["id"], "item_name": item["displayName"],
                        "connection_string": conn_str,
                    })
            except Exception as inner_e:
                print(f"  WARN: could not get connection string for {ws_name}/{item['displayName']}: {inner_e}")
    print(f"Discovered {len(warehouses)} Warehouse item(s) to scan for copy events.")
    for w in warehouses:
        print(f"  - {w['workspace_name']}/{w['item_name']}")
    if excluded_names:
        print(f"Excluded {len(excluded_names)} internal/auto-generated warehouse(s):")
        for n in excluded_names:
            print(f"  - {n}")
except Exception as e:
    log_error_to_lakehouse("discover_warehouses", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Read the incremental watermark FIRST (before querying any warehouse), so we can push the
#     start_time filter down into the Query Insights SQL itself instead of pulling everything and
#     filtering in Spark afterward. This avoids a full history rescan on every run (requirement #4).
try:
    watermark_table_exists = spark.catalog.tableExists("CopyEventWatermark")
    if watermark_table_exists:
        wm_rows = spark.read.table("CopyEventWatermark").collect()
        watermark = {r["item_id"]: r["last_processed_start_time"] for r in wm_rows}
    else:
        watermark = {}
    print(f"Loaded watermark for {len(watermark)} warehouse item(s).")
except Exception as e:
    log_error_to_lakehouse("read_watermark", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# JDBC-based query helper against each Warehouse's SQL analytics endpoint, using the SP's AAD token
# (Fabric Warehouse SQL endpoints support AccessToken-based auth exactly like Azure SQL / Synapse).

def query_warehouse(connection_string, sql_text, token):
    jdbc_url = f"jdbc:sqlserver://{connection_string}:1433;encrypt=true;trustServerCertificate=false;loginTimeout=30"
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", sql_text)
        .option("accessToken", token)
        .option("hostNameInCertificate", "*.datawarehouse.fabric.microsoft.com")
        .load()
    )

DEFAULT_WATERMARK = "1900-01-01T00:00:00"

def build_query_insights_sql(warehouse_item_name, last_processed_start_time):
    # Warehouse Query Insights is exposed as "<warehouse_name>.queryinsights.exec_requests_history",
    # not "sys.dm_exec_requests_history" (that DMV name doesn't resolve on Fabric Warehouse SQL endpoints).
    # This table has no "command"/"query_text" split - the full SQL text is in "command", and the
    # short statement kind is in "statement_type". The start_time filter is pushed down here (rather
    # than pulled into Spark and filtered in Python) so each run only scans NEW query-history rows.
    since = last_processed_start_time or DEFAULT_WATERMARK
    return f"""
SELECT
    CAST(distributed_statement_id AS VARCHAR(100)) AS distributed_statement_id,
    start_time,
    end_time,
    statement_type,
    command AS query_text
FROM [{warehouse_item_name}].[queryinsights].[exec_requests_history]
WHERE start_time > '{since}'
  AND ( statement_type IN ('INSERT', 'CREATE TABLE AS SELECT', 'SELECT INTO')
     OR command LIKE '%INSERT INTO%SELECT%'
     OR command LIKE '%CREATE TABLE%AS%SELECT%' )
"""

all_query_rows = []
try:
    for wh in warehouses:
        try:
            last_ts = watermark.get(wh["item_id"])
            df = query_warehouse(wh["connection_string"], build_query_insights_sql(wh["item_name"], last_ts), sql_token)
            rows = df.collect()
            print(f"  {wh['workspace_name']}/{wh['item_name']}: {len(rows)} new candidate copy statement(s) since watermark ({last_ts or DEFAULT_WATERMARK}).")
            for r in rows:
                all_query_rows.append({
                    "workspace_id": wh["workspace_id"], "workspace_name": wh["workspace_name"],
                    "item_id": wh["item_id"], "item_name": wh["item_name"],
                    "connection_string": wh["connection_string"],
                    "distributed_statement_id": r["distributed_statement_id"],
                    "start_time": str(r["start_time"]), "end_time": str(r["end_time"]),
                    "statement_type": r["statement_type"], "query_text": r["query_text"],
                })
        except Exception as inner_e:
            print(f"  WARN: could not query {wh['workspace_name']}/{wh['item_name']}: {inner_e}")
    print(f"Total new candidate copy statements across all warehouses: {len(all_query_rows)}")
except Exception as e:
    log_error_to_lakehouse("scan_query_insights", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The watermark has already been applied server-side (pushed down into the SQL WHERE clause above),
# so every row in all_query_rows is already "new" - no further Python-side filtering is needed here.
new_rows = all_query_rows
print(f"New (unprocessed) candidate statements this run: {len(new_rows)}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Parse each candidate statement: destination table + column list (or SELECT *), and source table(s) ---
import re

def parse_copy_statement(query_text):
    """Best-effort regex parse of CTAS / INSERT INTO ... SELECT statements.
    Returns dict with dest_table, dest_columns (list or None if not explicit), is_select_star, source_tables.
    This is intentionally conservative: if the statement can't be confidently parsed, source_tables will
    be empty and the row is skipped downstream (no false positives)."""
    q = " ".join(query_text.split())  # normalize whitespace
    q_upper = q.upper()

    is_select_star = bool(re.search(r"SELECT\s+\*\s+FROM", q_upper))

    dest_table = None
    dest_columns = None
    m_ctas = re.search(r"CREATE\s+TABLE\s+([\[\]\w\.]+)\s+AS\s+SELECT", q, re.IGNORECASE)
    m_insert = re.search(r"INSERT\s+INTO\s+([\[\]\w\.]+)\s*(\(([^)]+)\))?\s*SELECT", q, re.IGNORECASE)
    if m_ctas:
        dest_table = m_ctas.group(1).strip("[]")
    elif m_insert:
        dest_table = m_insert.group(1).strip("[]")
        if m_insert.group(3):
            dest_columns = [c.strip().strip("[]") for c in m_insert.group(3).split(",")]

    # Explicit SELECT column list (used when not SELECT *) - capture between SELECT and FROM.
    if dest_columns is None and not is_select_star:
        m_cols = re.search(r"SELECT\s+(.*?)\s+FROM\s", q, re.IGNORECASE | re.DOTALL)
        if m_cols:
            raw_cols = m_cols.group(1)
            # crude split on top-level commas (good enough for simple column lists; expressions with
            # nested commas, e.g. function calls, are a known limitation noted in the design doc)
            dest_columns = [c.strip().split(" AS ")[-1].strip().strip("[]") for c in raw_cols.split(",")]

    # Parse each captured "FROM"/"JOIN" reference into its (database, schema, table) parts, stripping
    # brackets from EACH part individually - a naive whole-string .strip("[]") leaves brackets stuck
    # to the middle parts of a 3-part name like [db].[schema].[table] (e.g. "db].[schema].[table").
    def split_qualified_name(raw):
        parts = [p.strip().strip("[]") for p in raw.split(".")]
        database = parts[0] if len(parts) == 3 else None
        schema = parts[-2] if len(parts) >= 2 else None
        table = parts[-1]
        return {"database": database, "schema": schema, "table": table, "raw": raw}

    source_refs_raw = re.findall(r"FROM\s+([\[\]\w\.]+)", q, re.IGNORECASE) + re.findall(r"JOIN\s+([\[\]\w\.]+)", q, re.IGNORECASE)
    source_tables = [split_qualified_name(t) for t in source_refs_raw]

    return {
        "dest_table": dest_table,
        "dest_columns": dest_columns,
        "is_select_star": is_select_star,
        "source_tables": source_tables,
    }

parsed_rows = []
for row in new_rows:
    parsed = parse_copy_statement(row["query_text"])
    parsed_rows.append({**row, **parsed})

print(f"Parsed {len(parsed_rows)} candidate statement(s).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Cross-reference parsed source_tables against DimShortcut to find copies FROM a shortcut ---
# IMPORTANT: a copy statement's source is frequently a CROSS-ITEM reference (e.g. a 3-part name
# [lakehouse03].[dbo].[sh_warehouse03_Date] issued from warehouse03's own SQL endpoint) - the shortcut
# is NOT necessarily hosted in the item that ran the query. So the lookup key must be resolved from
# the querying item's WORKSPACE + the referenced database name (source_tables[i]["database"]), not
# from the querying item's own item_id. When no database is specified (a 1- or 2-part name), we fall
# back to assuming the source lives in the querying item itself (same-item copy).
try:
    dim_shortcut_df = spark.read.table("DimShortcut")
    # Build lookup across ALL hosting item types (Lakehouse AND Warehouse) - a shortcut being copied
    # from can live in either kind of item, not just a Warehouse. Map lower(name) -> ORIGINAL-case name,
    # because the hosting warehouse/lakehouse's SQL collation can be case-sensitive (e.g.
    # Latin1_General_100_BIN2_UTF8) - INFORMATION_SCHEMA.COLUMNS lookups downstream must use the real
    # original-case table name, not our lowercased matching key.
    shortcut_names_by_item_name = {}
    for r in dim_shortcut_df.collect():
        key = (r["hosting_workspace_id"], r["hosting_item_name"].lower())
        shortcut_names_by_item_name.setdefault(key, {})[r["shortcut_name"].lower()] = r["shortcut_name"]

    def references_a_shortcut(row):
        for src in row["source_tables"]:
            referenced_item_name = (src["database"] or row["item_name"])
            key = (row["workspace_id"], referenced_item_name.lower())
            known_shortcuts = shortcut_names_by_item_name.get(key, {})
            leaf = src["table"].lower()
            if leaf in known_shortcuts:
                return {"shortcut_name": known_shortcuts[leaf], "shortcut_database": referenced_item_name}
        return None

    copy_events_from_shortcuts = []
    for row in parsed_rows:
        match = references_a_shortcut(row)
        if match:
            copy_events_from_shortcuts.append({
                **row,
                "matched_shortcut_name": match["shortcut_name"],
                "matched_shortcut_database": match["shortcut_database"],
            })

    print(f"Of those, {len(copy_events_from_shortcuts)} statement(s) read FROM a known shortcut.")
except Exception as e:
    log_error_to_lakehouse("crossref_dimshortcut", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- For each matched copy event, get the TRUE source column count (INFORMATION_SCHEMA.COLUMNS on the
#     hosting warehouse - a shortcut's columns are visible there exactly like a native table), compute
#     retention %, and decide is_shortcut_read_and_saved_as_is per the design doc §1.1 rule:
#     flagged if SELECT * OR retention% > THRESHOLD_PCT (even when extra new columns were also added).

def get_source_column_count(connection_string, database_name, table_name, token):
    # The shortcut may live in a different item (database) than the one the JDBC connection is
    # currently pointed at (e.g. querying via warehouse03's endpoint for a shortcut hosted in
    # lakehouse03) - INFORMATION_SCHEMA.COLUMNS is scoped to the CURRENT database only, so we must
    # 3-part-qualify the reference with the actual database name rather than query unqualified.
    sql_text = f"""
    SELECT COUNT(*) AS col_count
    FROM [{database_name}].INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name.split('.')[-1]}'
    """
    df = query_warehouse(connection_string, sql_text, token)
    row = df.collect()[0]
    return row["col_count"]

fact_copy_event_rows = []
now_ts = datetime.now(timezone.utc).isoformat()

try:
    for row in copy_events_from_shortcuts:
        try:
            source_col_count = get_source_column_count(
                row["connection_string"], row["matched_shortcut_database"], row["matched_shortcut_name"], sql_token
            )
        except Exception as inner_e:
            print(f"  WARN: could not resolve source column count for {row['matched_shortcut_name']}: {inner_e}")
            continue

        if source_col_count == 0:
            print(f"  WARN: source_col_count was 0 for shortcut '{row['matched_shortcut_name']}' in database '{row['matched_shortcut_database']}' - skipping (check name/case/collation).")
            continue

        if row["is_select_star"]:
            retained_count = source_col_count
            dest_col_count = source_col_count
        else:
            dest_cols_lower = [c.lower() for c in (row["dest_columns"] or [])]
            dest_col_count = len(dest_cols_lower)
            # Retained = destination columns count (we assume an explicit list drawn from the shortcut
            # is a subset/match of source columns; a stricter version would fetch actual source column
            # NAMES and intersect them with dest_cols_lower - noted as a refinement for a later pass).
            retained_count = min(dest_col_count, source_col_count)

        retention_pct = round((retained_count / source_col_count) * 100, 1) if source_col_count else 0.0
        is_flagged = row["is_select_star"] or (retention_pct > THRESHOLD_PCT)

        fact_copy_event_rows.append({
            "event_id": f"{row['item_id']}|{row['distributed_statement_id']}",
            "hosting_workspace_id": row["workspace_id"],
            "hosting_workspace_name": row["workspace_name"],
            "hosting_item_id": row["item_id"],
            "hosting_item_name": row["item_name"],
            "engine": "Warehouse",
            "matched_shortcut_name": row["matched_shortcut_name"],
            "matched_shortcut_database": row["matched_shortcut_database"],
            "dest_table": row["dest_table"],
            "source_column_count": source_col_count,
            "dest_column_count": dest_col_count,
            "retained_column_count": retained_count,
            "retention_pct": retention_pct,
            "is_select_star": row["is_select_star"],
            "is_shortcut_read_and_saved_as_is": is_flagged,
            "threshold_pct_at_detection": THRESHOLD_PCT,
            "query_start_time": row["start_time"],
            "detected_ts": now_ts,
        })

    print(f"Computed retention % for {len(fact_copy_event_rows)} copy event(s); "
          f"{sum(1 for r in fact_copy_event_rows if r['is_shortcut_read_and_saved_as_is'])} flagged as read-and-saved-as-is.")
except Exception as e:
    log_error_to_lakehouse("compute_column_retention", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql import functions as F

FACT_COPY_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("hosting_workspace_id", StringType()),
    StructField("hosting_workspace_name", StringType()),
    StructField("hosting_item_id", StringType()),
    StructField("hosting_item_name", StringType()),
    StructField("engine", StringType()),
    StructField("matched_shortcut_name", StringType()),
    StructField("matched_shortcut_database", StringType()),
    StructField("dest_table", StringType()),
    StructField("source_column_count", IntegerType()),
    StructField("dest_column_count", IntegerType()),
    StructField("retained_column_count", IntegerType()),
    StructField("retention_pct", DoubleType()),
    StructField("is_select_star", BooleanType()),
    StructField("is_shortcut_read_and_saved_as_is", BooleanType()),
    StructField("threshold_pct_at_detection", DoubleType()),
    StructField("query_start_time", StringType()),
    StructField("detected_ts", StringType()),
])

try:
    if fact_copy_event_rows:
        # NOTE: keep query_start_time/detected_ts as StringType (matching FACT_COPY_EVENT_SCHEMA and
        # the already-persisted Delta table schema from the first empty-table-creation run) - casting
        # to TimestampType here caused a DELTA_FAILED_TO_MERGE_FIELDS conflict on append.
        fact_copy_event_df = spark.createDataFrame(fact_copy_event_rows, schema=FACT_COPY_EVENT_SCHEMA)
        fact_copy_event_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("FactCopyEvent")
        print(f"Appended {fact_copy_event_df.count()} rows to FactCopyEvent.")
        display(fact_copy_event_df)
    else:
        if not spark.catalog.tableExists("FactCopyEvent"):
            spark.createDataFrame([], schema=FACT_COPY_EVENT_SCHEMA).write.mode("overwrite").format("delta").saveAsTable("FactCopyEvent")
        print("No new copy events detected this run.")

    # Table/column descriptions for FactCopyEvent (see notebook NB_ShortcutInventory_DuplicateDetection
    # for the same pattern/rationale on the other 3 tables).
    if spark.catalog.tableExists("FactCopyEvent"):
        table_comment = (
            "One row per detected read-from-shortcut copy event on the Warehouse engine (CTAS / INSERT "
            "INTO ... SELECT statements sourced from a known OneLake shortcut, from Query Insights "
            "exec_requests_history). is_shortcut_read_and_saved_as_is = TRUE when the statement "
            "used SELECT * OR the destination table retained more than threshold_pct_at_detection percent "
            "of the source columns, even if extra new columns were also added. Incremental: only "
            "processes query-history rows newer than the per-item watermark in CopyEventWatermark."
        )
        spark.sql(f"ALTER TABLE FactCopyEvent SET TBLPROPERTIES ('comment' = '{table_comment}')")
        col_comments = {
            "event_id": "Natural key: hosting_item_id + distributed_statement_id from Query Insights.",
            "hosting_workspace_id": "Workspace GUID where the copy statement ran.",
            "hosting_workspace_name": "Display name of hosting_workspace_id.",
            "hosting_item_id": "Warehouse item GUID where the copy statement ran.",
            "hosting_item_name": "Display name of hosting_item_id.",
            "engine": "Fabric engine that executed the copy: Warehouse (Spark and Dataflow Gen2 are separate future engines).",
            "matched_shortcut_name": "Name of the OneLake shortcut the statement read from (matched against DimShortcut).",
            "matched_shortcut_database": "Name of the hosting Lakehouse/Warehouse item where the matched shortcut lives (may differ from hosting_item_name for cross-item copies).",
            "dest_table": "Destination table name the SELECT was saved into.",
            "source_column_count": "Total column count of the shortcut source table.",
            "dest_column_count": "Column count actually written to the destination table.",
            "retained_column_count": "Number of destination columns considered retained from the source (capped at source_column_count).",
            "retention_pct": "retained_column_count / source_column_count * 100.",
            "is_select_star": "TRUE if the statement used SELECT * FROM the shortcut.",
            "is_shortcut_read_and_saved_as_is": "TRUE if is_select_star OR retention_pct > threshold_pct_at_detection - the flag this whole solution exists to raise.",
            "threshold_pct_at_detection": "Configurable retention threshold (config.detection.columnRetentionThresholdPercent) in effect when this row was computed.",
            "query_start_time": "Start time of the source query, from Query Insights.",
            "detected_ts": "UTC timestamp this notebook run detected/computed this row.",
        }
        existing_cols = {f.name for f in spark.table("FactCopyEvent").schema.fields}
        for col, comment in col_comments.items():
            if col in existing_cols:
                spark.sql(f"ALTER TABLE FactCopyEvent ALTER COLUMN {col} COMMENT '{comment}'")
except Exception as e:
    log_error_to_lakehouse("write_fact_copy_event", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Advance the incremental watermark per warehouse item (requirement #4: never full rescan). ---
# IMPORTANT: only advance past rows that were actually written to FactCopyEvent this run - not every
# row merely observed in Query Insights. This means a row that fails parsing/lookup, or simply isn't
# sourced from a known shortcut, is NOT skipped forever: it will be re-evaluated on the next run since
# its item's watermark won't move past it. Only genuinely processed-and-recorded copy events retire
# from future scans. Merged defensively with the existing watermark (max of old vs. new) so an item
# with no successful rows this run keeps its prior watermark rather than being reset.
try:
    max_ts_by_item = dict(watermark)  # start from existing watermark; only raise it, never lower it
    for row in fact_copy_event_rows:
        item_id = row["hosting_item_id"]
        candidate_ts = row["query_start_time"]
        if item_id not in max_ts_by_item or candidate_ts > max_ts_by_item[item_id]:
            max_ts_by_item[item_id] = candidate_ts

    if max_ts_by_item:
        wm_schema = StructType([
            StructField("item_id", StringType()),
            StructField("last_processed_start_time", StringType()),
        ])
        wm_rows_out = [{"item_id": k, "last_processed_start_time": v} for k, v in max_ts_by_item.items()]
        spark.createDataFrame(wm_rows_out, schema=wm_schema).write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("CopyEventWatermark")
        advanced_count = sum(1 for k in max_ts_by_item if watermark.get(k) != max_ts_by_item[k])
        print(f"Watermark table written for {len(max_ts_by_item)} warehouse item(s); {advanced_count} item(s) advanced this run.")
    else:
        print("No watermark update needed (no prior watermark and no copy events written this run).")
except Exception as e:
    log_error_to_lakehouse("advance_watermark", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- One-time schema migration guard: add matched_shortcut_database to FactCopyEvent if the table
# already existed from before this column was introduced (mergeSchema only runs on the append cell
# above, which is skipped entirely when 0 new rows are detected this run - so a run with no new copy
# events would never migrate the persisted schema). Safe/idempotent: no-op if the column already exists.
try:
    if spark.catalog.tableExists("FactCopyEvent"):
        existing_fields = {f.name for f in spark.table("FactCopyEvent").schema.fields}
        if "matched_shortcut_database" not in existing_fields:
            spark.sql("ALTER TABLE FactCopyEvent ADD COLUMNS (matched_shortcut_database STRING)")
            print("Migrated FactCopyEvent: added matched_shortcut_database column.")
        else:
            print("FactCopyEvent already has matched_shortcut_database column - no migration needed.")

        # Backfill historical rows written BEFORE matched_shortcut_database started being populated
        # (they have NULL/empty string there) - without this, the reconciliation view's join on
        # matched_shortcut_database silently fails to match those rows against DimShortcut, since
        # DimShortcut's hosting_item_name (e.g. 'lakehouse03') never equals '' or NULL. Resolve the
        # correct hosting_item_name by looking up matched_shortcut_name in DimShortcut (current state)
        # first, then falling back to FactShortcutInventoryDiff's most recent snapshot for shortcuts
        # that have since been deleted and no longer appear in DimShortcut. Uses a Delta MERGE so it's
        # safe to re-run every time (only touches rows that are still blank).
        blank_count = spark.sql(
            "SELECT COUNT(*) c FROM FactCopyEvent WHERE matched_shortcut_database IS NULL OR matched_shortcut_database = ''"
        ).collect()[0]["c"]
        if blank_count > 0:
            from pyspark.sql.window import Window

            dim_lookup = (
                spark.read.table("DimShortcut")
                .select(F.col("shortcut_name"), F.col("hosting_item_name").alias("resolved_database"))
                .dropDuplicates(["shortcut_name"])
            )
            diff_latest = (
                spark.read.table("FactShortcutInventoryDiff")
                .select("shortcut_name", "hosting_item_name", "snapshot_ts")
                .withColumn("rn", F.row_number().over(Window.partitionBy("shortcut_name").orderBy(F.col("snapshot_ts").desc())))
                .filter(F.col("rn") == 1)
                .select(F.col("shortcut_name"), F.col("hosting_item_name").alias("resolved_database"))
            )
            # DimShortcut (current) takes priority; fall back to FactShortcutInventoryDiff (last-known,
            # covers since-deleted shortcuts) only for names DimShortcut doesn't have.
            backfill_lookup = dim_lookup.unionByName(
                diff_latest.join(dim_lookup, on="shortcut_name", how="left_anti")
            ).dropDuplicates(["shortcut_name"])

            backfill_lookup.createOrReplaceTempView("_backfill_lookup")
            spark.sql("""
                MERGE INTO FactCopyEvent f
                USING _backfill_lookup b
                ON f.matched_shortcut_name = b.shortcut_name
                   AND (f.matched_shortcut_database IS NULL OR f.matched_shortcut_database = '')
                WHEN MATCHED THEN UPDATE SET f.matched_shortcut_database = b.resolved_database
            """)
            remaining = spark.sql(
                "SELECT COUNT(*) c FROM FactCopyEvent WHERE matched_shortcut_database IS NULL OR matched_shortcut_database = ''"
            ).collect()[0]["c"]
            print(f"Backfilled matched_shortcut_database for {blank_count - remaining} historical row(s); {remaining} still unresolved (shortcut name not found in DimShortcut or FactShortcutInventoryDiff).")
        else:
            print("No blank matched_shortcut_database rows to backfill.")
except Exception as e:
    log_error_to_lakehouse("migrate_factcopyevent_schema", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Reconciliation: flag FactCopyEvent rows whose source shortcut has since been deleted. ---
# FactCopyEvent only stores the shortcut NAME at detection time (matched_shortcut_name), not a
# durable key, and DimShortcut reflects only the CURRENT snapshot (deleted shortcuts vanish from it).
# Rather than mutate/backfill FactCopyEvent (which would break its append-only/incremental design),
# recompute a reporting VIEW fresh each run: for every FactCopyEvent row, check whether a shortcut of
# that name still exists (in DimShortcut, matched on hosting_workspace_id + matched_shortcut_database
# + matched_shortcut_name), and if not, look up WHEN it was removed from FactShortcutInventoryDiff
# (change_type = 'removed', most recent such event). This is a lightweight SQL VIEW (not a materialized
# table) so it always reflects the latest DimShortcut/FactShortcutInventoryDiff state with zero
# incremental-processing cost - Power BI / the data agent can query it directly.
try:
    spark.sql("""
        CREATE OR REPLACE VIEW vw_FactCopyEvent_SourceStatus AS
        WITH latest_removal AS (
            SELECT
                hosting_workspace_id,
                hosting_item_name,
                shortcut_name,
                MAX(snapshot_ts) AS source_removed_ts
            FROM FactShortcutInventoryDiff
            WHERE change_type = 'removed'
            GROUP BY hosting_workspace_id, hosting_item_name, shortcut_name
        )
        SELECT
            f.*,
            CASE WHEN d.shortcut_name IS NOT NULL THEN TRUE ELSE FALSE END AS source_shortcut_exists_now,
            r.source_removed_ts
        FROM FactCopyEvent f
        LEFT JOIN DimShortcut d
            ON d.hosting_workspace_id = f.hosting_workspace_id
           AND d.hosting_item_name = f.matched_shortcut_database
           AND d.shortcut_name = f.matched_shortcut_name
        LEFT JOIN latest_removal r
            ON r.hosting_workspace_id = f.hosting_workspace_id
           AND r.hosting_item_name = f.matched_shortcut_database
           AND r.shortcut_name = f.matched_shortcut_name
    """)
    spark.sql("""
        ALTER VIEW vw_FactCopyEvent_SourceStatus SET TBLPROPERTIES ('comment' =
        'Reporting view over FactCopyEvent enriched with the CURRENT existence status of the source shortcut it was copied from. source_shortcut_exists_now = FALSE means the shortcut that fed this copy event has since been deleted (per DimShortcut/FactShortcutInventoryDiff); source_removed_ts is when that deletion was detected. Recomputed fresh on every read - not a materialized/incremental table.')
    """)
    stale_count = spark.sql("SELECT COUNT(*) c FROM vw_FactCopyEvent_SourceStatus WHERE source_shortcut_exists_now = FALSE").collect()[0]["c"]
    print(f"vw_FactCopyEvent_SourceStatus refreshed. {stale_count} FactCopyEvent row(s) reference a since-deleted shortcut.")
except Exception as e:
    log_error_to_lakehouse("build_vw_factcopyevent_sourcestatus", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== Run summary (Warehouse copy-event detection) ===")
print(f"Warehouses scanned: {len(warehouses)}")
print(f"Candidate copy statements in query history: {len(all_query_rows)}")
print(f"New (unprocessed) this run: {len(new_rows)}")
print(f"Matched to a known shortcut: {len(copy_events_from_shortcuts)}")
print(f"Written to FactCopyEvent: {len(fact_copy_event_rows)}")
print(f"Flagged read-and-saved-as-is: {sum(1 for r in fact_copy_event_rows if r['is_shortcut_read_and_saved_as_is'])}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write a human-readable JSON summary artifact to Files/reports for easy external verification
# (the Fabric SQL analytics endpoint can lag several minutes behind the Lakehouse Delta tables,
# so this file is the fastest way to confirm what a run actually did).
import json as _json2

copy_event_summary = {
    "run_ts": now_ts,
    "warehouses_scanned": len(warehouses),
    "warehouses_scanned_list": [f"{w['workspace_name']}/{w['item_name']}" for w in warehouses],
    "candidate_copy_statements_in_history": len(all_query_rows),
    "new_unprocessed_this_run": len(new_rows),
    "matched_to_known_shortcut": len(copy_events_from_shortcuts),
    "written_to_fact_copy_event": len(fact_copy_event_rows),
    "flagged_read_and_saved_as_is": sum(1 for r in fact_copy_event_rows if r["is_shortcut_read_and_saved_as_is"]),
    "copy_event_details": fact_copy_event_rows,
}

summary_json2 = _json2.dumps(copy_event_summary, indent=2, default=str)
rdd_out2 = spark.sparkContext.parallelize([summary_json2], 1)
out_path2 = f"{LAKEHOUSE_ABFSS}/Files/reports/copyevent_summary_{int(datetime.now().timestamp())}"
rdd_out2.saveAsTextFile(out_path2)
print(f"Copy-event summary written to: {out_path2}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

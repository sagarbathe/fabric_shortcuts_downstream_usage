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

# ## NB_ShortcutInventory_DuplicateDetection
# Notebook for the Shortcut "Read-and-Saved-As-Is" Monitoring solution.
# 
# Responsibilities (see design doc §5, §6, §1.1a):
# 1. Read the solution config (monitored workspaces, thresholds) from `Files/config/config.json`.
# 2. Authenticate as the monitoring service principal (client-credentials flow).
# 3. Enumerate Lakehouse/Warehouse items and their OneLake shortcuts across all monitored workspaces,
#    resolving human-readable workspace/item names for BOTH the hosting side and the shortcut's source side.
# 4. Upsert `DimShortcut`, compute `FactShortcutInventoryDiff` (new/removed shortcut lifecycle events).
# 5. Compute `FactDuplicateShortcutGroup` (High = same hosting item; Medium = same hosting workspace,
#    different hosting item; cross-workspace same-source is explicitly NOT flagged).
# 
# All fact/dim tables use a deterministic BIGINT surrogate key `shortcut_sk` (xxhash64 of the natural
# key `shortcut_key`) for joins between tables - cheaper and simpler to join on than the string key,
# and stable across incremental runs since it is a pure hash (not an auto-increment identity).


# CELL ********************

import json, requests, traceback
from datetime import datetime, timezone

# Resolve the solution's own lakehouse (where config.json/DimShortcut/etc. live) from the notebook's
# runtime attachment context rather than hardcoding GUIDs - this is the one config value that CANNOT
# come from config.json itself (config.json's own location depends on it). If the notebook's default
# lakehouse is ever reattached to a different item, this adapts automatically with no code edit.
_ctx = notebookutils.runtime.context
LAKEHOUSE_WORKSPACE_ID = _ctx["defaultLakehouseWorkspaceId"]
LAKEHOUSE_ID = _ctx["defaultLakehouseId"]
LAKEHOUSE_ABFSS = f"abfss://{LAKEHOUSE_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}"

def log_error_to_lakehouse(stage, exc):
    """Best-effort error logging to Files/logs so failures are inspectable after the job ends."""
    try:
        err_text = f"STAGE: {stage}\nTIME: {datetime.now(timezone.utc).isoformat()}\n\n{traceback.format_exc()}"
        rdd = spark.sparkContext.parallelize([err_text], 1)
        rdd.saveAsTextFile(f"{LAKEHOUSE_ABFSS}/Files/logs/last_run_error_{int(datetime.now().timestamp())}.log")
    except Exception:
        pass  # never let logging itself break the job

try:
    config_df = spark.read.text(f"{LAKEHOUSE_ABFSS}/Files/config/config.json")
    config_text = "\n".join([r["value"] for r in config_df.collect()])
    config = json.loads(config_text)
except Exception as e:
    log_error_to_lakehouse("load_config", e)
    raise

TENANT_ID = config["auth"]["tenantId"]
CLIENT_ID = config["auth"]["clientId"]
CLIENT_SECRET = config["auth"]["clientSecret"]
MONITORED_WORKSPACES = config["monitoredWorkspaces"]
THRESHOLD_PCT = config["detection"]["columnRetentionThresholdPercent"]

print(f"Loaded config via ABFSS. Monitored workspaces: {[w['workspaceName'] for w in MONITORED_WORKSPACES]}")
print(f"Configured column-retention threshold: {THRESHOLD_PCT}%")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_sp_token(tenant_id, client_id, client_secret, scope="https://api.fabric.microsoft.com/.default"):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

try:
    token = get_sp_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    print("Acquired Fabric API token for monitoring service principal.")
except Exception as e:
    log_error_to_lakehouse("get_sp_token", e)
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
    auth_header_value = "Bearer " + token
    headers = {"Authorization": auth_header_value}
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
    auth_header_value = "Bearer " + token
    headers = {"Authorization": auth_header_value}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

SHORTCUT_ELIGIBLE_TYPES = {"Lakehouse", "Warehouse"}

# Name-resolution caches, seeded from the monitored-workspaces config so the common case
# (source item also lives in a monitored workspace) needs zero extra API calls.
WORKSPACE_NAME_CACHE = {w["workspaceId"]: w["workspaceName"] for w in MONITORED_WORKSPACES}
ITEM_NAME_CACHE = {}  # item_id -> (item_name, item_type)

def resolve_workspace_name(ws_id, token):
    if not ws_id:
        return None
    if ws_id not in WORKSPACE_NAME_CACHE:
        try:
            ws = fabric_get_single(f"{FABRIC_BASE}/workspaces/{ws_id}", token)
            WORKSPACE_NAME_CACHE[ws_id] = ws.get("displayName", "(unresolved)")
        except Exception:
            WORKSPACE_NAME_CACHE[ws_id] = "(unresolved)"
    return WORKSPACE_NAME_CACHE[ws_id]

def resolve_item_name(ws_id, item_id, token):
    if not item_id:
        return None
    if item_id not in ITEM_NAME_CACHE:
        try:
            item = fabric_get_single(f"{FABRIC_BASE}/workspaces/{ws_id}/items/{item_id}", token)
            ITEM_NAME_CACHE[item_id] = item.get("displayName", "(unresolved)")
        except Exception:
            ITEM_NAME_CACHE[item_id] = "(unresolved)"
    return ITEM_NAME_CACHE[item_id]

def enumerate_shortcuts(token, monitored_workspaces):
    snapshot_ts = datetime.now(timezone.utc).isoformat()
    rows = []
    for ws in monitored_workspaces:
        ws_id = ws["workspaceId"]
        ws_name = ws["workspaceName"]
        items = fabric_get_paged(f"{FABRIC_BASE}/workspaces/{ws_id}/items", token)
        for item in items:
            # Seed the item-name cache for every item we see, so source-side lookups for
            # shortcuts pointing back into another monitored workspace resolve for free.
            ITEM_NAME_CACHE[item["id"]] = item["displayName"]
            if item.get("type") not in SHORTCUT_ELIGIBLE_TYPES:
                continue
            item_id = item["id"]
            item_name = item["displayName"]
            item_type = item["type"]
            try:
                shortcuts = fabric_get_paged(
                    f"{FABRIC_BASE}/workspaces/{ws_id}/items/{item_id}/shortcuts", token
                )
            except requests.HTTPError as e:
                print(f"  WARN: could not list shortcuts for {ws_name}/{item_name}: {e}")
                continue
            for sc in shortcuts:
                target = sc.get("target", {}) or {}
                target_type = target.get("type")
                onelake = target.get("oneLake", {}) if target_type == "OneLake" else {}
                src_ws_id = onelake.get("workspaceId")
                src_item_id = onelake.get("itemId")
                shortcut_key = f"{ws_id}|{item_id}|{sc.get('path')}|{sc.get('name')}"
                rows.append({
                    "shortcut_key": shortcut_key,
                    "shortcut_name": sc.get("name"),
                    "hosting_workspace_id": ws_id,
                    "hosting_workspace_name": ws_name,
                    "hosting_item_id": item_id,
                    "hosting_item_name": item_name,
                    "hosting_item_type": item_type,
                    "shortcut_path": sc.get("path"),
                    "target_type": target_type,
                    "source_workspace_id": src_ws_id,
                    "source_item_id": src_item_id,
                    "source_path": onelake.get("path"),
                    "is_internal_onelake": target_type == "OneLake",
                    "snapshot_ts": snapshot_ts,
                })
    # Second pass: resolve source_workspace_name / source_item_name for every row now that all
    # monitored-workspace items have seeded the caches (only unresolved cross-scope sources need
    # an extra API round-trip here).
    for row in rows:
        row["source_workspace_name"] = resolve_workspace_name(row["source_workspace_id"], token)
        row["source_item_name"] = resolve_item_name(row["source_workspace_id"], row["source_item_id"], token)
    return rows, snapshot_ts

try:
    shortcut_rows, snapshot_ts = enumerate_shortcuts(token, MONITORED_WORKSPACES)
    print(f"Discovered {len(shortcut_rows)} shortcuts across {len(MONITORED_WORKSPACES)} monitored workspaces at {snapshot_ts}.")
except Exception as e:
    log_error_to_lakehouse("enumerate_shortcuts", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

schema = StructType([
    StructField("shortcut_key", StringType()),
    StructField("shortcut_name", StringType()),
    StructField("hosting_workspace_id", StringType()),
    StructField("hosting_workspace_name", StringType()),
    StructField("hosting_item_id", StringType()),
    StructField("hosting_item_name", StringType()),
    StructField("hosting_item_type", StringType()),
    StructField("shortcut_path", StringType()),
    StructField("target_type", StringType()),
    StructField("source_workspace_id", StringType()),
    StructField("source_workspace_name", StringType()),
    StructField("source_item_id", StringType()),
    StructField("source_item_name", StringType()),
    StructField("source_path", StringType()),
    StructField("is_internal_onelake", BooleanType()),
    StructField("snapshot_ts", StringType()),
])

try:
    new_snapshot_df = spark.createDataFrame(shortcut_rows, schema=schema) \
        .withColumn("snapshot_ts", F.to_timestamp("snapshot_ts")) \
        .withColumn("shortcut_sk", F.xxhash64(F.col("shortcut_key")))

    new_snapshot_df.createOrReplaceTempView("new_snapshot")
    print("New snapshot loaded into temp view 'new_snapshot'.")
    display(new_snapshot_df)
except Exception as e:
    log_error_to_lakehouse("build_new_snapshot_df", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Incremental diff against prior DimShortcut snapshot (requirement #4: never full rescan of history) ---
try:
    table_exists = spark.catalog.tableExists("DimShortcut")

    if table_exists:
        prior_df = spark.read.table("DimShortcut")
        prior_keys = set(r["shortcut_key"] for r in prior_df.select("shortcut_key").collect())
    else:
        prior_df = None
        prior_keys = set()

    new_keys = set(r["shortcut_key"] for r in new_snapshot_df.select("shortcut_key").collect())

    added_keys = new_keys - prior_keys
    removed_keys = prior_keys - new_keys

    print(f"Prior shortcut count: {len(prior_keys)} | New snapshot count: {len(new_keys)}")
    print(f"  Added:   {len(added_keys)}")
    print(f"  Removed: {len(removed_keys)}")

    # 'new' events are enriched from the fresh snapshot; 'removed' events are enriched from the prior
    # snapshot (the shortcut and its hosting/source item no longer exist in the new snapshot at all).
    diff_parts = []
    if added_keys:
        diff_parts.append(
            new_snapshot_df.filter(F.col("shortcut_key").isin(list(added_keys)))
            .withColumn("change_type", F.lit("new"))
        )
    if removed_keys and prior_df is not None:
        diff_parts.append(
            prior_df.filter(F.col("shortcut_key").isin(list(removed_keys)))
            .withColumn("change_type", F.lit("removed"))
        )

    diff_row_count = 0
    if diff_parts:
        diff_df = diff_parts[0]
        for part in diff_parts[1:]:
            diff_df = diff_df.unionByName(part)
        diff_df = diff_df.select(
            "shortcut_sk", "shortcut_key", "change_type",
            "shortcut_name",
            "hosting_workspace_id", "hosting_workspace_name",
            "hosting_item_id", "hosting_item_name", "hosting_item_type",
            "shortcut_path", "target_type",
            "source_workspace_id", "source_workspace_name",
            "source_item_id", "source_item_name", "source_path",
            "is_internal_onelake", "snapshot_ts",
        )
        diff_row_count = diff_df.count()
        diff_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("FactShortcutInventoryDiff")
        print(f"Appended {diff_row_count} rows to FactShortcutInventoryDiff.")
    else:
        print("No inventory changes since last run.")
except Exception as e:
    log_error_to_lakehouse("incremental_diff", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Upsert DimShortcut (full snapshot replace is acceptable here since this table is small metadata,
#     not event history; history of changes lives in FactShortcutInventoryDiff above) ---
try:
    new_snapshot_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("DimShortcut")
    print("DimShortcut table refreshed.")
except Exception as e:
    log_error_to_lakehouse("upsert_dimshortcut", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Duplicate shortcut detection (design doc §1.1a) ---
# High: 2+ shortcuts in the SAME hosting item pointing at the SAME source.
# Medium: 2+ shortcuts in the SAME hosting workspace but DIFFERENT hosting items pointing at the SAME source.
# Not flagged: same source shortcuted into a DIFFERENT hosting workspace (normal shortcut reuse).

import hashlib

def make_group_id(*parts):
    return hashlib.sha256("|".join([str(p) for p in parts]).encode()).hexdigest()[:16]

make_group_id_udf = F.udf(make_group_id, StringType())

try:
    onelake_df = spark.read.table("DimShortcut").filter(F.col("is_internal_onelake") == True)

    FACT_COLUMNS = [
        "duplicate_group_id", "shortcut_sk", "shortcut_key", "shortcut_name",
        "hosting_workspace_id", "hosting_workspace_name",
        "hosting_item_id", "hosting_item_name",
        "source_workspace_id", "source_workspace_name",
        "source_item_id", "source_item_name", "source_path",
        "group_member_count", "severity", "explanation", "detected_ts",
    ]

    now_ts = datetime.now(timezone.utc).isoformat()

    # Pass 1: High severity - same hosting_workspace_id + hosting_item_id + source triple.
    high_groups = (
        onelake_df.groupBy("hosting_workspace_id", "hosting_item_id", "source_workspace_id", "source_item_id", "source_path")
        .agg(F.count("shortcut_key").alias("group_member_count"))
        .filter(F.col("group_member_count") > 1)
        .withColumn("duplicate_group_id", make_group_id_udf(
            F.lit("H"), "hosting_workspace_id", "hosting_item_id", "source_workspace_id", "source_item_id", "source_path"))
    )

    high_fact_df = (
        onelake_df.join(high_groups, on=["hosting_workspace_id", "hosting_item_id", "source_workspace_id", "source_item_id", "source_path"], how="inner")
        .withColumn("severity", F.lit("High"))
        .withColumn("explanation", F.lit(
            "Two or more shortcuts exist in the SAME Lakehouse/Warehouse pointing at the identical source - "
            "pure redundancy, no legitimate reason for both to exist."))
        .withColumn("detected_ts", F.to_timestamp(F.lit(now_ts)))
        .select(*FACT_COLUMNS)
    )

    high_keys = set(r["shortcut_key"] for r in high_fact_df.select("shortcut_key").collect())

    # Pass 2: Medium severity - same hosting_workspace_id + source triple, different hosting_item_id,
    # excluding shortcuts already covered by a High group.
    remaining_df = onelake_df.filter(~F.col("shortcut_key").isin(list(high_keys)) if high_keys else F.lit(True))

    medium_groups = (
        remaining_df.groupBy("hosting_workspace_id", "source_workspace_id", "source_item_id", "source_path")
        .agg(F.count("shortcut_key").alias("group_member_count"))
        .filter(F.col("group_member_count") > 1)
        .withColumn("duplicate_group_id", make_group_id_udf(
            F.lit("M"), "hosting_workspace_id", "source_workspace_id", "source_item_id", "source_path"))
    )

    medium_fact_df = (
        remaining_df.join(medium_groups, on=["hosting_workspace_id", "source_workspace_id", "source_item_id", "source_path"], how="inner")
        .withColumn("severity", F.lit("Medium"))
        .withColumn("explanation", F.lit(
            "Two or more shortcuts exist in the SAME workspace (different Lakehouse/Warehouse items) pointing at the "
            "identical source. Not High because different items may have distinct security boundaries, but still "
            "unnecessary proliferation worth reviewing."))
        .withColumn("detected_ts", F.to_timestamp(F.lit(now_ts)))
        .select(*FACT_COLUMNS)
    )

    fact_df = high_fact_df.unionByName(medium_fact_df)
    fact_row_count = fact_df.count()
    high_group_count = high_groups.count()
    medium_group_count = medium_groups.count()

    if fact_row_count > 0:
        fact_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("FactDuplicateShortcutGroup")
        print(f"FactDuplicateShortcutGroup refreshed: {fact_row_count} rows ({high_group_count} High groups, {medium_group_count} Medium groups).")
        display(fact_df.orderBy("severity", "duplicate_group_id"))
    else:
        # still (re)create an empty table with correct schema so downstream report/model doesn't break
        empty_schema = StructType([
            StructField("duplicate_group_id", StringType()),
            StructField("shortcut_sk", StringType()),
            StructField("shortcut_key", StringType()),
            StructField("shortcut_name", StringType()),
            StructField("hosting_workspace_id", StringType()),
            StructField("hosting_workspace_name", StringType()),
            StructField("hosting_item_id", StringType()),
            StructField("hosting_item_name", StringType()),
            StructField("source_workspace_id", StringType()),
            StructField("source_workspace_name", StringType()),
            StructField("source_item_id", StringType()),
            StructField("source_item_name", StringType()),
            StructField("source_path", StringType()),
            StructField("group_member_count", StringType()),
            StructField("severity", StringType()),
            StructField("explanation", StringType()),
            StructField("detected_ts", StringType()),
        ])
        spark.createDataFrame([], schema=empty_schema).write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("FactDuplicateShortcutGroup")
        print("No duplicate shortcut groups detected this run.")
except Exception as e:
    log_error_to_lakehouse("duplicate_detection", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Table & column descriptions (COMMENT metadata) so the Lakehouse SQL analytics endpoint / Power BI
#     / Data Agent surface self-documenting business meaning to consumers. Safe to re-run (idempotent).

TABLE_COMMENTS = {
    "DimShortcut": (
        "Current-state snapshot of every OneLake shortcut discovered across the monitored workspaces "
        "(see Files/config/config.json). Fully replaced on every scheduled run - this is a dimension of "
        "shortcuts that exist right now, not a history. Row grain: one row per shortcut."
    ),
    "FactShortcutInventoryDiff": (
        "Append-only lifecycle event log of shortcut creations and deletions detected between consecutive "
        "scheduled runs (every 15 minutes). This is what makes the solution incremental: consumers read only "
        "the new rows appended since they last looked, instead of re-diffing the full DimShortcut snapshot. "
        "Row grain: one row per shortcut per change event (new = created since last run, removed = no "
        "longer present since last run)."
    ),
    "FactDuplicateShortcutGroup": (
        "One row per shortcut that is a member of a duplicate-shortcut group, i.e. two or more shortcuts point "
        "at the exact same source item/path. High severity = duplicates live in the SAME hosting Lakehouse/"
        "Warehouse/KQL item (pure redundancy). Medium severity = duplicates live in the SAME hosting workspace "
        "but DIFFERENT hosting items (possibly intentional per-item security boundary, still worth reviewing). "
        "The same source shortcuted into a DIFFERENT workspace is normal reuse and is never flagged here."
    ),
}

COLUMN_COMMENTS = {
    "DimShortcut": {
        "shortcut_sk": "Deterministic BIGINT surrogate key (xxhash64 of shortcut_key). Prefer this over shortcut_key for joins to Fact tables.",
        "shortcut_key": "Natural key: hosting_workspace_id|hosting_item_id|shortcut_path|shortcut_name. Kept for readability/debugging.",
        "shortcut_name": "The shortcut display name as it appears in the hosting Lakehouse/Warehouse.",
        "hosting_workspace_id": "Workspace GUID where this shortcut physically lives (the workspace being monitored).",
        "hosting_workspace_name": "Display name of hosting_workspace_id, resolved at scan time.",
        "hosting_item_id": "Lakehouse/Warehouse item GUID that hosts (contains) this shortcut.",
        "hosting_item_name": "Display name of hosting_item_id.",
        "hosting_item_type": "Fabric item type of the hosting item, e.g. Lakehouse or Warehouse.",
        "shortcut_path": "Path within the hosting item where the shortcut is placed, e.g. /Tables or /Files.",
        "target_type": "Type of the shortcut target, e.g. OneLake (internal, tracked for duplicates), AmazonS3, ADLS, GCS, Dataverse (external).",
        "source_workspace_id": "For OneLake targets: workspace GUID of the shortcut source item. Null for external (non-OneLake) targets.",
        "source_workspace_name": "Display name of source_workspace_id, resolved at scan time.",
        "source_item_id": "For OneLake targets: item GUID the shortcut points to (its source Lakehouse/Warehouse/KQL DB).",
        "source_item_name": "Display name of source_item_id.",
        "source_path": "Path within the source item that the shortcut points to (excludes the shortcut own name, by design, so two shortcuts with different names to the same path are still recognized as duplicates).",
        "is_internal_onelake": "True if target_type = OneLake (i.e. this shortcut is eligible for duplicate-shortcut detection); false for external targets.",
        "snapshot_ts": "UTC timestamp of the scan run that produced this row.",
    },
    "FactShortcutInventoryDiff": {
        "shortcut_sk": "Deterministic BIGINT surrogate key (xxhash64 of shortcut_key). Prefer this over shortcut_key for joins.",
        "shortcut_key": "Natural key of the shortcut this event refers to (see DimShortcut.shortcut_key).",
        "change_type": "new = this shortcut was created since the prior scheduled run. removed = this shortcut existed at the prior run but no longer exists now (deleted, or its hosting item was deleted).",
        "shortcut_name": "Shortcut display name at the time of this event.",
        "hosting_workspace_id": "Workspace GUID hosting the shortcut at the time of this event.",
        "hosting_workspace_name": "Display name of hosting_workspace_id.",
        "hosting_item_id": "Hosting Lakehouse/Warehouse item GUID at the time of this event.",
        "hosting_item_name": "Display name of hosting_item_id.",
        "hosting_item_type": "Fabric item type of the hosting item.",
        "shortcut_path": "Path within the hosting item, e.g. /Tables or /Files.",
        "target_type": "Type of the shortcut target (OneLake, AmazonS3, ADLS, GCS, Dataverse, etc.).",
        "source_workspace_id": "For OneLake targets: source item workspace GUID.",
        "source_workspace_name": "Display name of source_workspace_id.",
        "source_item_id": "For OneLake targets: source item GUID.",
        "source_item_name": "Display name of source_item_id.",
        "source_path": "Path within the source item that the shortcut points to.",
        "is_internal_onelake": "True if this was an internal OneLake shortcut (duplicate-detection eligible).",
        "snapshot_ts": "UTC timestamp of the scan run in which this change was detected.",
    },
    "FactDuplicateShortcutGroup": {
        "duplicate_group_id": "Stable hash identifying one duplicate group (all shortcuts pointing at the same source, grouped per the High/Medium rule). Group all rows sharing this id to see the full duplicate set.",
        "shortcut_sk": "Deterministic BIGINT surrogate key of the duplicated shortcut (xxhash64 of shortcut_key) - join to DimShortcut.shortcut_sk.",
        "shortcut_key": "Natural key of the duplicated shortcut.",
        "shortcut_name": "Display name of this specific duplicate shortcut.",
        "hosting_workspace_id": "Workspace GUID hosting this specific duplicate shortcut.",
        "hosting_workspace_name": "Display name of hosting_workspace_id.",
        "hosting_item_id": "Lakehouse/Warehouse item GUID hosting this specific duplicate shortcut.",
        "hosting_item_name": "Display name of hosting_item_id.",
        "source_workspace_id": "Workspace GUID of the common source all group members point to.",
        "source_workspace_name": "Display name of source_workspace_id.",
        "source_item_id": "Item GUID of the common source all group members point to.",
        "source_item_name": "Display name of source_item_id.",
        "source_path": "Common source path all group members point to (this is the field that makes them duplicates).",
        "group_member_count": "Total number of shortcuts in this duplicate group.",
        "severity": "High = duplicates share the same hosting item (pure redundancy). Medium = duplicates share the hosting workspace but live in different hosting items.",
        "explanation": "Human-readable reason for the assigned severity, for display in reports/alerts.",
        "detected_ts": "UTC timestamp when this duplicate group was last (re)computed.",
    },
}

try:
    for tbl, comment in TABLE_COMMENTS.items():
        if spark.catalog.tableExists(tbl):
            escaped = comment.replace("'", "''")
            # Note: "COMMENT ON TABLE" is Unity-Catalog/Databricks-SQL syntax and is NOT supported by
            # Fabric's Spark SQL for managed Lakehouse tables - use TBLPROPERTIES instead, which works
            # for both Hive-style and Delta tables and is what powers the description shown in the UI.
            spark.sql(f"ALTER TABLE {tbl} SET TBLPROPERTIES ('comment' = '{escaped}')")

    for tbl, cols in COLUMN_COMMENTS.items():
        if spark.catalog.tableExists(tbl):
            existing_cols = {f.name for f in spark.table(tbl).schema.fields}
            for col, comment in cols.items():
                if col in existing_cols:
                    escaped = comment.replace("'", "''")
                    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN {col} COMMENT '{escaped}'")

    print("Table and column descriptions applied.")
except Exception as e:
    log_error_to_lakehouse("apply_table_column_comments", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== Run summary ===")
print(f"Snapshot timestamp: {snapshot_ts}")
print(f"Total shortcuts inventoried: {len(shortcut_rows)}")
print(f"Inventory changes this run: {diff_row_count}")
print(f"Duplicate groups (High): {high_group_count}")
print(f"Duplicate groups (Medium): {medium_group_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write a human-readable JSON summary artifact to Files/reports so results can be verified externally
import json as _json

summary = {
    "snapshot_ts": snapshot_ts,
    "total_shortcuts": len(shortcut_rows),
    "inventory_changes_this_run": diff_row_count,
    "duplicate_groups_high": high_group_count,
    "duplicate_groups_medium": medium_group_count,
    "duplicate_details": [row.asDict() for row in fact_df.orderBy("severity", "duplicate_group_id").collect()],
}

summary_json = _json.dumps(summary, indent=2, default=str)
rdd_out = spark.sparkContext.parallelize([summary_json], 1)
out_path = f"{LAKEHOUSE_ABFSS}/Files/reports/duplicate_summary_{int(datetime.now().timestamp())}"
rdd_out.saveAsTextFile(out_path)
print(f"Summary written to: {out_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

"""One-off generator for the SM_ShortcutMonitoring semantic model (model.bim / TMSL).
Run once to (re)generate the repo files under fabric/semanticmodels/SM_ShortcutMonitoring.SemanticModel/.
Not part of the deployed solution itself - a build-time authoring helper only.
"""
import json
import uuid

SQL_ENDPOINT = "cnfzy3l2lhkuxgxslgdsleid7u-nygmlrorgaxe3b3g3aurpyjvsi.datawarehouse.fabric.microsoft.com"
LAKEHOUSE_SQL_DB = "LH_ShortcutMonitoring"


def tag():
    return str(uuid.uuid4())


def m_source(table_name):
    return {
        "type": "m",
        "expression": [
            "let",
            f"    Source = Sql.Database(\"{SQL_ENDPOINT}\", \"{LAKEHOUSE_SQL_DB}\"),",
            f"    dbo_{table_name} = Source{{[Schema=\"dbo\",Item=\"{table_name}\"]}}[Data]",
            "in",
            f"    dbo_{table_name}",
        ],
    }


def col(name, dataType, description, hidden=False, formatString=None, summarizeBy="none", isKey=False):
    c = {
        "name": name,
        "dataType": dataType,
        "sourceColumn": name,
        "summarizeBy": summarizeBy,
        "isHidden": hidden,
        "lineageTag": tag(),
        "description": description,
    }
    if formatString:
        c["formatString"] = formatString
    if isKey:
        c["isKey"] = True
    return c


def calc_col(name, dataType, expression, description, hidden=False, formatString=None, summarizeBy="none"):
    c = {
        "name": name,
        "dataType": dataType,
        "type": "calculated",
        "expression": expression,
        "summarizeBy": summarizeBy,
        "isHidden": hidden,
        "lineageTag": tag(),
        "description": description,
    }
    if formatString:
        c["formatString"] = formatString
    return c


def measure(name, expression, description, formatString=None):
    m = {
        "name": name,
        "expression": expression,
        "lineageTag": tag(),
        "description": description,
    }
    if formatString:
        m["formatString"] = formatString
    return m


def partition(table_name, source):
    return {
        "name": f"{table_name}",
        "mode": "import",
        "source": source,
    }


# --- DimShortcut ---------------------------------------------------------
DimShortcut = {
    "name": "DimShortcut",
    "description": (
        "Current-state snapshot of every OneLake shortcut discovered across the monitored workspaces. "
        "Fully replaced on every scheduled run (every 15 minutes) - a dimension of shortcuts that exist "
        "right now, not a history (see FactShortcutInventoryDiff for history). Row grain: one row per "
        "shortcut."
    ),
    "lineageTag": tag(),
    "columns": [
        col("shortcut_sk", "int64", "Deterministic surrogate key (xxhash64 of shortcut_key). The preferred join key to every Fact table in this model.", hidden=True, isKey=True),
        col("shortcut_key", "string", "Natural key: hosting_workspace_id|hosting_item_id|shortcut_path|shortcut_name. Kept for debugging only - use shortcut_sk for joins.", hidden=True),
        col("shortcut_name", "string", "The shortcut display name as it appears in the hosting Lakehouse/Warehouse."),
        col("hosting_workspace_id", "string", "Workspace GUID where this shortcut physically lives.", hidden=True),
        col("hosting_workspace_name", "string", "Display name of the workspace hosting this shortcut."),
        col("hosting_item_id", "string", "Lakehouse/Warehouse item GUID that hosts (contains) this shortcut.", hidden=True),
        col("hosting_item_name", "string", "Display name of the Lakehouse/Warehouse item hosting this shortcut."),
        col("hosting_item_type", "string", "Fabric item type of the hosting item, e.g. Lakehouse or Warehouse."),
        col("shortcut_path", "string", "Path within the hosting item where the shortcut is placed, e.g. /Tables or /Files."),
        col("target_type", "string", "Type of the shortcut target: OneLake (internal, tracked for duplicates), AmazonS3, ADLS, GCS, Dataverse (external)."),
        col("source_workspace_id", "string", "For OneLake targets: workspace GUID of the shortcut source item. Blank for external targets.", hidden=True),
        col("source_workspace_name", "string", "Display name of the source workspace (OneLake targets only)."),
        col("source_item_id", "string", "For OneLake targets: item GUID the shortcut points to.", hidden=True),
        col("source_item_name", "string", "Display name of the source item the shortcut points to (OneLake targets only)."),
        col("source_path", "string", "Path within the source item that the shortcut points to. Two shortcuts sharing this value (and source item) are duplicates, regardless of their own names."),
        col("is_internal_onelake", "boolean", "True if target_type = OneLake, i.e. eligible for duplicate-shortcut detection; false for external targets."),
        col("snapshot_ts", "dateTime", "UTC timestamp of the scan run that produced this row.", formatString="General Date"),
    ],
    "measures": [
        measure("Total Shortcuts", "COUNTROWS(DimShortcut)", "Count of all currently-existing shortcuts across monitored workspaces."),
        measure("Internal OneLake Shortcuts", "CALCULATE([Total Shortcuts], DimShortcut[is_internal_onelake] = TRUE)", "Count of currently-existing shortcuts pointing at another OneLake item (eligible for duplicate detection)."),
        measure("External Shortcuts", "[Total Shortcuts] - [Internal OneLake Shortcuts]", "Count of currently-existing shortcuts pointing outside OneLake (S3, ADLS, GCS, Dataverse, etc.)."),
    ],
    "partitions": [partition("DimShortcut", m_source("DimShortcut"))],
}

# --- FactShortcutInventoryDiff --------------------------------------------
FactShortcutInventoryDiff = {
    "name": "FactShortcutInventoryDiff",
    "description": (
        "Append-only lifecycle event log of shortcut creations and deletions detected between "
        "consecutive scheduled runs (every 15 minutes). Row grain: one row per shortcut per change "
        "event (new = created since last run, removed = no longer present since last run)."
    ),
    "lineageTag": tag(),
    "columns": [
        col("shortcut_sk", "int64", "Deterministic surrogate key of the shortcut this event refers to - join to DimShortcut.shortcut_sk (rows for shortcuts already removed will not match any current DimShortcut row - expected).", hidden=True),
        col("shortcut_key", "string", "Natural key of the shortcut this event refers to.", hidden=True),
        col("change_type", "string", "'new' = created since the prior run. 'removed' = existed at the prior run but no longer exists now."),
        col("shortcut_name", "string", "Shortcut display name at the time of this event."),
        col("hosting_workspace_id", "string", "Workspace GUID hosting the shortcut at the time of this event.", hidden=True),
        col("hosting_workspace_name", "string", "Display name of the hosting workspace at the time of this event."),
        col("hosting_item_id", "string", "Hosting Lakehouse/Warehouse item GUID at the time of this event.", hidden=True),
        col("hosting_item_name", "string", "Display name of the hosting item at the time of this event."),
        col("hosting_item_type", "string", "Fabric item type of the hosting item."),
        col("shortcut_path", "string", "Path within the hosting item, e.g. /Tables or /Files."),
        col("target_type", "string", "Type of the shortcut target (OneLake, AmazonS3, ADLS, GCS, Dataverse, etc.)."),
        col("source_workspace_id", "string", "For OneLake targets: source item workspace GUID.", hidden=True),
        col("source_workspace_name", "string", "Display name of the source workspace (OneLake targets only)."),
        col("source_item_id", "string", "For OneLake targets: source item GUID.", hidden=True),
        col("source_item_name", "string", "Display name of the source item (OneLake targets only)."),
        col("source_path", "string", "Path within the source item that the shortcut points to."),
        col("is_internal_onelake", "boolean", "True if this was an internal OneLake shortcut (duplicate-detection eligible)."),
        col("snapshot_ts", "dateTime", "UTC timestamp of the scan run in which this change was detected.", formatString="General Date"),
    ],
    "measures": [
        measure("Shortcuts Added", "CALCULATE(COUNTROWS(FactShortcutInventoryDiff), FactShortcutInventoryDiff[change_type] = \"new\")", "Count of shortcut-creation events in the current filter context."),
        measure("Shortcuts Removed", "CALCULATE(COUNTROWS(FactShortcutInventoryDiff), FactShortcutInventoryDiff[change_type] = \"removed\")", "Count of shortcut-deletion events in the current filter context."),
        measure("Net Shortcut Change", "[Shortcuts Added] - [Shortcuts Removed]", "Net change in shortcut count in the current filter context (positive = growing, negative = shrinking)."),
    ],
    "partitions": [partition("FactShortcutInventoryDiff", m_source("FactShortcutInventoryDiff"))],
}

# --- FactDuplicateShortcutGroup --------------------------------------------
FactDuplicateShortcutGroup = {
    "name": "FactDuplicateShortcutGroup",
    "description": (
        "One row per shortcut that is a member of a duplicate-shortcut group, i.e. two or more "
        "shortcuts point at the exact same source item/path. High severity = duplicates live in the "
        "SAME hosting item (pure redundancy). Medium severity = duplicates live in the SAME hosting "
        "workspace but DIFFERENT hosting items. The same source shortcut reused in a DIFFERENT "
        "workspace is normal reuse and is never flagged here. Not consumed by either copy-event "
        "engine - a control-plane governance signal only."
    ),
    "lineageTag": tag(),
    "columns": [
        col("duplicate_group_id", "string", "Stable hash identifying one duplicate group. Group all rows sharing this id to see the full duplicate set."),
        col("shortcut_sk", "int64", "Deterministic surrogate key of the duplicated shortcut - join to DimShortcut.shortcut_sk.", hidden=True),
        col("shortcut_key", "string", "Natural key of the duplicated shortcut.", hidden=True),
        col("shortcut_name", "string", "Display name of this specific duplicate shortcut."),
        col("hosting_workspace_id", "string", "Workspace GUID hosting this specific duplicate shortcut.", hidden=True),
        col("hosting_workspace_name", "string", "Display name of the workspace hosting this specific duplicate shortcut."),
        col("hosting_item_id", "string", "Lakehouse/Warehouse item GUID hosting this specific duplicate shortcut.", hidden=True),
        col("hosting_item_name", "string", "Display name of the item hosting this specific duplicate shortcut."),
        col("source_workspace_id", "string", "Workspace GUID of the common source all group members point to.", hidden=True),
        col("source_workspace_name", "string", "Display name of the common source workspace."),
        col("source_item_id", "string", "Item GUID of the common source all group members point to.", hidden=True),
        col("source_item_name", "string", "Display name of the common source item."),
        col("source_path", "string", "Common source path all group members point to - this is the field that makes them duplicates."),
        col("group_member_count", "string", "Total number of shortcuts in this duplicate group, as text (raw Delta type). Use the 'Group Member Count' calculated column for numeric aggregation.", hidden=True),
        col("severity", "string", "High = duplicates share the same hosting item. Medium = duplicates share the hosting workspace but live in different hosting items."),
        col("explanation", "string", "Human-readable reason for the assigned severity."),
        col("detected_ts", "string", "UTC timestamp when this duplicate group was last (re)computed, as text (raw Delta type). Use the 'Detected At' calculated column for a proper datetime.", hidden=True),
        calc_col("Group Member Count", "int64", "VALUE(FactDuplicateShortcutGroup[group_member_count])", "Numeric version of group_member_count, for aggregation/sorting in visuals."),
        calc_col("Detected At", "dateTime", "DATETIMEVALUE(SUBSTITUTE(LEFT(FactDuplicateShortcutGroup[detected_ts], 19), \"T\", \" \"))", "detected_ts parsed into a proper datetime, for use in date slicers/axes.", formatString="General Date"),
    ],
    "measures": [
        measure("Duplicate Groups", "DISTINCTCOUNT(FactDuplicateShortcutGroup[duplicate_group_id])", "Count of distinct duplicate-shortcut groups in the current filter context."),
        measure("High Severity Groups", "CALCULATE([Duplicate Groups], FactDuplicateShortcutGroup[severity] = \"High\")", "Count of distinct High-severity duplicate groups (duplicates in the same hosting item)."),
        measure("Medium Severity Groups", "CALCULATE([Duplicate Groups], FactDuplicateShortcutGroup[severity] = \"Medium\")", "Count of distinct Medium-severity duplicate groups (duplicates in the same workspace, different hosting items)."),
        measure("Duplicate Shortcuts (rows)", "COUNTROWS(FactDuplicateShortcutGroup)", "Total count of individual shortcuts that are members of any duplicate group (not deduplicated by group)."),
    ],
    "partitions": [partition("FactDuplicateShortcutGroup", m_source("FactDuplicateShortcutGroup"))],
}

# --- FactCopyEvent ----------------------------------------------------------
FactCopyEvent = {
    "name": "FactCopyEvent",
    "description": (
        "One row per detected read-from-shortcut copy event, across the implemented engines: "
        "Warehouse (CTAS/INSERT-SELECT via Query Insights) and SparkKafka (OpenLineage column-lineage "
        "detection via the ES_OpenLineageEvents Eventstream), distinguished by the engine column. "
        "is_shortcut_read_and_saved_as_is = TRUE when the write retained ALL source columns (SELECT * "
        "equivalent) OR retention_pct > threshold_pct_at_detection, even if extra new columns were "
        "also added - this is the core signal the whole solution exists to raise. This table does NOT "
        "physically include vw_FactCopyEvent_SourceStatus's two enrichment columns - they are "
        "reproduced here as the 'Source Shortcut Exists Now' and 'Source Removed At' calculated "
        "columns instead, to avoid importing a second overlapping copy of this same grain."
    ),
    "lineageTag": tag(),
    "columns": [
        col("event_id", "string", "Natural key. Warehouse: {item_id}|{distributed_statement_id}. SparkKafka: an OpenLineage-run-derived identifier. Unique per engine.", hidden=True),
        col("hosting_workspace_id", "string", "Workspace GUID where the copy statement/notebook ran.", hidden=True),
        col("hosting_workspace_name", "string", "Display name of the workspace where the copy ran."),
        col("hosting_item_id", "string", "Warehouse item GUID (Warehouse engine) or notebook item GUID (SparkKafka engine) where the copy ran.", hidden=True),
        col("hosting_item_name", "string", "Warehouse: the Warehouse item name. SparkKafka: the monitored notebook's name."),
        col("engine", "string", "Fabric engine that executed the copy: Warehouse or SparkKafka. (Dataflow Gen2 is designed but not implemented - see design doc.)"),
        col("matched_shortcut_name", "string", "Name of the OneLake shortcut the statement/notebook read from (matched against DimShortcut)."),
        col("matched_shortcut_database", "string", "Name of the hosting Lakehouse/Warehouse item where the matched shortcut lives (may differ from hosting_item_name for cross-item copies)."),
        col("dest_table", "string", "Destination table name the SELECT/write was saved into."),
        col("source_column_count", "int64", "Total column count of the shortcut source table."),
        col("dest_column_count", "int64", "Column count actually written to the destination table."),
        col("retained_column_count", "int64", "Number of destination columns considered retained from the source (capped at source_column_count)."),
        col("retention_pct", "double", "retained_column_count / source_column_count * 100.", formatString="0.0\"%\"", summarizeBy="average"),
        col("is_select_star", "boolean", "TRUE if the statement used SELECT * (Warehouse) / retained all columns (SparkKafka)."),
        col("is_shortcut_read_and_saved_as_is", "boolean", "TRUE if is_select_star OR retention_pct > threshold_pct_at_detection - the flag this whole solution exists to raise."),
        col("threshold_pct_at_detection", "double", "Configurable retention threshold (config.detection.columnRetentionThresholdPercent) in effect when this row was computed.", formatString="0.0\"%\""),
        col("query_start_time", "string", "Start time of the source query/write, as text (raw Delta type, kept as STRING to avoid a Delta schema-merge conflict). Use the 'Query Start At' calculated column for a proper datetime.", hidden=True),
        col("detected_ts", "string", "UTC timestamp this row was detected/computed, as text (raw Delta type). Use the 'Detected At' calculated column for a proper datetime.", hidden=True),
        calc_col(
            "shortcut_sk", "int64",
            "LOOKUPVALUE(\n\tDimShortcut[shortcut_sk],\n\tDimShortcut[hosting_workspace_id], FactCopyEvent[hosting_workspace_id],\n\tDimShortcut[hosting_item_name], FactCopyEvent[matched_shortcut_database],\n\tDimShortcut[shortcut_name], FactCopyEvent[matched_shortcut_name]\n)",
            "Bridge key added at the semantic-model layer only (FactCopyEvent has no native shortcut_sk column) - resolves the matched shortcut's surrogate key so this table can relate to DimShortcut. Blank if the matched shortcut is no longer in the current DimShortcut snapshot.",
            hidden=True,
        ),
        calc_col(
            "Source Shortcut Exists Now", "boolean",
            "NOT ISBLANK(RELATED(DimShortcut[shortcut_name]))",
            "TRUE if the shortcut this copy event read from still exists in the current DimShortcut snapshot. Reproduces vw_FactCopyEvent_SourceStatus.source_shortcut_exists_now at the semantic-model layer.",
        ),
        calc_col(
            "Source Removed At", "dateTime",
            "CALCULATE(\n\tMAX(FactShortcutInventoryDiff[snapshot_ts]),\n\tFactShortcutInventoryDiff[hosting_workspace_id] = FactCopyEvent[hosting_workspace_id],\n\tFactShortcutInventoryDiff[hosting_item_name] = FactCopyEvent[matched_shortcut_database],\n\tFactShortcutInventoryDiff[shortcut_name] = FactCopyEvent[matched_shortcut_name],\n\tFactShortcutInventoryDiff[change_type] = \"removed\"\n)",
            "If the source shortcut no longer exists, the most recent time its removal was detected. Reproduces vw_FactCopyEvent_SourceStatus.source_removed_ts at the semantic-model layer. Blank if the shortcut still exists or was never seen being removed.",
            formatString="General Date",
        ),
        calc_col(
            "Detected At", "dateTime",
            "DATETIMEVALUE(SUBSTITUTE(LEFT(FactCopyEvent[detected_ts], 19), \"T\", \" \"))",
            "detected_ts parsed into a proper datetime, for use in date slicers/axes/trend visuals.",
            formatString="General Date",
        ),
        calc_col(
            "Query Start At", "dateTime",
            "DATETIMEVALUE(SUBSTITUTE(LEFT(FactCopyEvent[query_start_time], 19), \"T\", \" \"))",
            "query_start_time parsed into a proper datetime, for use in date slicers/axes/trend visuals.",
            formatString="General Date",
        ),
    ],
    "measures": [
        measure("Total Copy Events", "COUNTROWS(FactCopyEvent)", "Count of all detected copy events (both engines) in the current filter context."),
        measure("Flagged Copy Events", "CALCULATE([Total Copy Events], FactCopyEvent[is_shortcut_read_and_saved_as_is] = TRUE)", "Count of copy events flagged as 'shortcut read and saved as-is' - the core risk signal."),
        measure("Flagged %", "DIVIDE([Flagged Copy Events], [Total Copy Events])", "Share of copy events flagged as read-and-saved-as-is.", formatString="0.0%"),
        measure("Avg Retention %", "AVERAGE(FactCopyEvent[retention_pct])", "Average column-retention percentage across copy events in the current filter context.", formatString="0.0\"%\""),
        measure("Copy Events - Warehouse", "CALCULATE([Total Copy Events], FactCopyEvent[engine] = \"Warehouse\")", "Count of copy events detected by the Warehouse engine."),
        measure("Copy Events - Spark", "CALCULATE([Total Copy Events], FactCopyEvent[engine] = \"SparkKafka\")", "Count of copy events detected by the Spark/OpenLineage engine."),
        measure("Copy Events - Orphaned Source", "CALCULATE([Total Copy Events], FactCopyEvent[Source Shortcut Exists Now] = FALSE)", "Count of copy events whose source shortcut has since been deleted."),
    ],
    "partitions": [partition("FactCopyEvent", m_source("FactCopyEvent"))],
}

TABLES = [DimShortcut, FactShortcutInventoryDiff, FactDuplicateShortcutGroup, FactCopyEvent]

RELATIONSHIPS = [
    {
        "name": tag(),
        "fromTable": "FactCopyEvent",
        "fromColumn": "shortcut_sk",
        "toTable": "DimShortcut",
        "toColumn": "shortcut_sk",
        "crossFilteringBehavior": "oneDirection",
    },
    {
        "name": tag(),
        "fromTable": "FactDuplicateShortcutGroup",
        "fromColumn": "shortcut_sk",
        "toTable": "DimShortcut",
        "toColumn": "shortcut_sk",
        "crossFilteringBehavior": "oneDirection",
    },
    {
        "name": tag(),
        "fromTable": "FactShortcutInventoryDiff",
        "fromColumn": "shortcut_sk",
        "toTable": "DimShortcut",
        "toColumn": "shortcut_sk",
        "crossFilteringBehavior": "oneDirection",
    },
]

model_bim = {
    "compatibilityLevel": 1567,
    "model": {
        "culture": "en-US",
        "defaultPowerBIDataSourceVersion": "powerBI_V3",
        "sourceQueryCulture": "en-US",
        "dataAccessOptions": {"legacyRedirects": True, "returnErrorValuesAsNull": True},
        "annotations": [
            {"name": "PBI_QueryOrder", "value": json.dumps([t["name"] for t in TABLES])},
            {"name": "__PBI_TimeIntelligenceEnabled", "value": "0"},
        ],
        "tables": TABLES,
        "relationships": RELATIONSHIPS,
    },
}

definition_pbism = {
    "version": "4.2",
    "settings": {"qnaEnabled": True},
}

if __name__ == "__main__":
    with open("model.bim.json", "w", encoding="utf-8") as f:
        json.dump(model_bim, f, indent=2)
    with open("definition.pbism.json", "w", encoding="utf-8") as f:
        json.dump(definition_pbism, f, indent=2)
    print("Wrote model.bim.json and definition.pbism.json")

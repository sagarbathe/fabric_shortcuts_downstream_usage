"""One-off generator for the DA_ShortcutMonitoring Fabric Data Agent.
Run once to (re)generate the repo files under fabric/dataagents/DA_ShortcutMonitoring.DataAgent/.
Not part of the deployed solution itself - a build-time authoring helper only.
"""
import json
import os
import uuid

WORKSPACE_ID = "c5c50c6e-30d1-4d2e-8766-d82917e13592"
SEMANTIC_MODEL_ID = "77bd6b7a-bfde-49cb-85dd-7d9357db30fd"
OUT_DIR = os.path.join(os.path.dirname(__file__), "_dataagent_out")

DATA_AGENT_JSON = {"$schema": "2.1.0"}

TABLE_ELEMENTS = [
    {
        "display_name": "DimShortcut",
        "type": "semantic_model.table",
        "is_selected": True,
        "description": "Current-state snapshot of every OneLake shortcut across monitored workspaces (one row per shortcut that exists right now).",
    },
    {
        "display_name": "FactCopyEvent",
        "type": "semantic_model.table",
        "is_selected": True,
        "description": "One row per detected read-from-shortcut copy event (Warehouse CTAS/INSERT-SELECT or Spark/OpenLineage). is_shortcut_read_and_saved_as_is=TRUE / 'Flagged Copy Events' measure is the core risk signal - a copy that retained all (or nearly all) source columns as-is.",
    },
    {
        "display_name": "FactDuplicateShortcutGroup",
        "type": "semantic_model.table",
        "is_selected": True,
        "description": "One row per shortcut that is part of a duplicate-shortcut group (two+ shortcuts pointing at the same source item/path). High severity = duplicates in the same hosting item; Medium = same workspace, different hosting item.",
    },
    {
        "display_name": "FactShortcutInventoryDiff",
        "type": "semantic_model.table",
        "is_selected": True,
        "description": "Append-only log of shortcut creation ('new') and deletion ('removed') events between consecutive 15-minute scans - the history/churn behind DimShortcut's current-state snapshot.",
    },
]

DATASOURCE_JSON = {
    "$schema": "1.0.0",
    "artifactId": SEMANTIC_MODEL_ID,
    "workspaceId": WORKSPACE_ID,
    "displayName": "SM_ShortcutMonitoring",
    "type": "semantic_model",
    "userDescription": (
        "Semantic model for the Fabric Shortcuts Downstream Usage Monitoring solution: OneLake "
        "shortcut inventory, duplicate-shortcut governance, and copy-event (shortcut read-and-saved-as-is) risk detection."
    ),
    "dataSourceInstructions": (
        "Use FactCopyEvent for questions about copy events / data being read from a shortcut and saved "
        "as a new table (retention_pct, is_shortcut_read_and_saved_as_is, engine=Warehouse|SparkKafka). "
        "Use FactDuplicateShortcutGroup for duplicate-shortcut governance questions (severity, "
        "duplicate_group_id). Use DimShortcut for 'what shortcuts exist right now' questions, and "
        "FactShortcutInventoryDiff for 'what changed / was created or removed' history questions. Join "
        "all Fact tables to DimShortcut via shortcut_sk."
    ),
    "elements": TABLE_ELEMENTS,
}

AI_INSTRUCTIONS = (
    "You are the analytics assistant for the Fabric Shortcuts Downstream Usage Monitoring solution. "
    "This solution watches OneLake shortcuts across Fabric workspaces for two governance risks: "
    "(1) COPY EVENTS - a shortcut being read and its data saved as a new physical table 'as-is' "
    "(all or nearly all source columns retained), which defeats the purpose of a shortcut (no data "
    "duplication) and creates a stale, disconnected copy; and (2) DUPLICATE SHORTCUTS - two or more "
    "shortcuts pointing at the exact same source item/path, which is redundant and confusing.\n\n"
    "Key guidance:\n"
    "- For copy-event questions, lead with FactCopyEvent[Flagged Copy Events] / [Flagged %] as the "
    "headline risk metrics, and mention the engine (Warehouse or SparkKafka) and destination table. "
    "If asked whether a copy is a real problem, check 'Source Shortcut Exists Now' - if FALSE, the "
    "source shortcut has since been deleted, and 'Source Removed At' shows when.\n"
    "- For duplicate-shortcut questions, group by duplicate_group_id and lead with severity (High = "
    "same hosting item, i.e. pure redundancy; Medium = same workspace, different hosting item = likely "
    "reuse but still worth reviewing). Never flag the same source shortcut reused across DIFFERENT "
    "workspaces as a duplicate - that is normal, intentional reuse.\n"
    "- For inventory/churn questions, use DimShortcut for 'what exists now' and "
    "FactShortcutInventoryDiff for 'what changed' (change_type = new/removed) over a time range.\n"
    "- Always express counts and percentages using the model's existing measures rather than writing "
    "ad hoc aggregations, and prefer the calculated 'Detected At' / 'Query Start At' / 'Source Removed "
    "At' datetime columns (not the raw string columns) for any date filtering or trending.\n"
    "- Be concise, quantify findings (counts, %, since when), and proactively suggest a natural "
    "follow-up drill-down (e.g. by workspace, by engine, by severity) when useful."
)

STAGE_CONFIG_JSON = {"$schema": "1.0.0", "aiInstructions": AI_INSTRUCTIONS}

FEWSHOTS_JSON = {
    "$schema": "1.0.0",
    "fewShots": [
        {
            "id": str(uuid.uuid4()),
            "question": "How many copy events have been flagged as read-and-saved-as-is this month?",
            "query": (
                "EVALUATE SUMMARIZECOLUMNS(\"Flagged Copy Events\", [Flagged Copy Events]) "
                "-- filter FactCopyEvent[Detected At] to the current month in the report/query context"
            ),
        },
        {
            "id": str(uuid.uuid4()),
            "question": "Which duplicate shortcut groups are high severity?",
            "query": (
                "EVALUATE FILTER(FactDuplicateShortcutGroup, FactDuplicateShortcutGroup[severity] = \"High\")"
            ),
        },
        {
            "id": str(uuid.uuid4()),
            "question": "Which copy events read from a shortcut that has since been deleted?",
            "query": (
                "EVALUATE FILTER(FactCopyEvent, FactCopyEvent[Source Shortcut Exists Now] = FALSE)"
            ),
        },
        {
            "id": str(uuid.uuid4()),
            "question": "How many shortcuts were added vs removed in the last 24 hours?",
            "query": (
                "EVALUATE SUMMARIZECOLUMNS(FactShortcutInventoryDiff[change_type], \"Count\", "
                "COUNTROWS(FactShortcutInventoryDiff)) -- filter snapshot_ts to the last 24 hours"
            ),
        },
    ],
}

PUBLISH_INFO_JSON = {
    "$schema": "1.0.0",
    "description": "DA_ShortcutMonitoring - published version covering copy-event risk detection, duplicate-shortcut governance, and shortcut inventory/churn.",
}

PLATFORM_JSON = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {"type": "DataAgent", "displayName": "DA_ShortcutMonitoring"},
    "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000000"},
}


def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def main():
    import shutil
    if os.path.isdir(OUT_DIR):
        shutil.rmtree(OUT_DIR)

    write_json(os.path.join(OUT_DIR, "Files", "Config", "data_agent.json"), DATA_AGENT_JSON)
    write_json(os.path.join(OUT_DIR, "Files", "Config", "draft", "stage_config.json"), STAGE_CONFIG_JSON)
    ds_folder = "semantic_model-SM_ShortcutMonitoring"
    write_json(os.path.join(OUT_DIR, "Files", "Config", "draft", ds_folder, "datasource.json"), DATASOURCE_JSON)
    write_json(os.path.join(OUT_DIR, "Files", "Config", "draft", ds_folder, "fewshots.json"), FEWSHOTS_JSON)
    write_json(os.path.join(OUT_DIR, ".platform"), PLATFORM_JSON)
    print(f"Data Agent definition written to {OUT_DIR}")


if __name__ == "__main__":
    main()

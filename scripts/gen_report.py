"""One-off generator for the RPT_ShortcutMonitoring sample Power BI report (PBIR format).
Run once to (re)generate the repo files under fabric/reports/RPT_ShortcutMonitoring.Report/.
Not part of the deployed solution itself - a build-time authoring helper only.
"""
import json
import os
import secrets

SEMANTIC_MODEL_ID = "10838a99-9e4e-4b30-8871-68d37f036d20"
OUT_DIR = os.path.join(os.path.dirname(__file__), "_report_out")
THEME_NAME = "CY24SU10"
THEME_SRC = os.path.join(os.path.dirname(__file__), "theme_CY24SU10.json")


def hid():
    return secrets.token_hex(10)


def col_field(table, column):
    return {
        "field": {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": column}},
        "queryRef": f"{table}.{column}",
        "active": True,
    }


def measure_field(table, measure):
    return {
        "field": {"Measure": {"Expression": {"SourceRef": {"Entity": table}}, "Property": measure}},
        "queryRef": f"{table}.{measure}",
        "active": True,
    }


def visual(name, visual_type, x, y, w, h, query_state, title=None, extra_objects=None):
    v = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.0.0/schema.json",
        "name": name,
        "position": {"x": x, "y": y, "z": 1000, "height": h, "width": w, "tabOrder": 1000},
        "visual": {
            "visualType": visual_type,
            "query": {"queryState": query_state},
            "objects": {},
        },
    }
    if title:
        v["visual"]["visualContainerObjects"] = {
            "title": [
                {
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "text": {"expr": {"Literal": {"Value": f"'{title}'"}}},
                    }
                }
            ]
        }
    if extra_objects:
        v["visual"]["objects"] = extra_objects
    return v


def page(page_id, display_name, ordinal, visuals_defs):
    """visuals_defs: list of (visual_id, visual_json)"""
    return page_id, {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
        "name": page_id,
        "displayName": display_name,
        "displayOption": "FitToPage",
        "height": 720,
        "width": 1280,
    }, visuals_defs


# ---------------------------------------------------------------------------
# Page 1: Executive Summary
# ---------------------------------------------------------------------------
exec_visuals = [
    (hid(), visual(hid(), "card", 20, 100, 200, 110, {
        "Values": {"projections": [measure_field("FactCopyEvent", "Total Copy Events")]},
    }, title="Total Copy Events")),
    (hid(), visual(hid(), "card", 240, 100, 200, 110, {
        "Values": {"projections": [measure_field("FactCopyEvent", "Flagged Copy Events")]},
    }, title="Flagged (Read-and-Saved-As-Is)")),
    (hid(), visual(hid(), "card", 460, 100, 200, 110, {
        "Values": {"projections": [measure_field("FactCopyEvent", "Flagged %")]},
    }, title="Flagged %")),
    (hid(), visual(hid(), "card", 680, 100, 200, 110, {
        "Values": {"projections": [measure_field("FactDuplicateShortcutGroup", "Duplicate Groups")]},
    }, title="Duplicate Shortcut Groups")),
    (hid(), visual(hid(), "card", 900, 100, 200, 110, {
        "Values": {"projections": [measure_field("DimShortcut", "Total Shortcuts")]},
    }, title="Total Shortcuts (current)")),
    (hid(), visual(hid(), "clusteredColumnChart", 20, 240, 550, 320, {
        "Category": {"projections": [col_field("FactCopyEvent", "engine")]},
        "Y": {"projections": [measure_field("FactCopyEvent", "Total Copy Events"), measure_field("FactCopyEvent", "Flagged Copy Events")]},
    }, title="Copy Events by Engine")),
    (hid(), visual(hid(), "pieChart", 590, 240, 340, 320, {
        "Category": {"projections": [col_field("FactDuplicateShortcutGroup", "severity")]},
        "Y": {"projections": [measure_field("FactDuplicateShortcutGroup", "Duplicate Groups")]},
    }, title="Duplicate Groups by Severity")),
    (hid(), visual(hid(), "clusteredColumnChart", 950, 240, 300, 320, {
        "Category": {"projections": [col_field("FactShortcutInventoryDiff", "change_type")]},
        "Y": {"projections": [measure_field("FactShortcutInventoryDiff", "Shortcuts Added"), measure_field("FactShortcutInventoryDiff", "Shortcuts Removed")]},
    }, title="Shortcut Inventory Changes")),
]

# ---------------------------------------------------------------------------
# Page 2: Copy Events
# ---------------------------------------------------------------------------
copy_events_visuals = [
    (hid(), visual(hid(), "slicer", 20, 90, 220, 550, {
        "Values": {"projections": [col_field("FactCopyEvent", "engine")]},
    }, title="Engine")),
    (hid(), visual(hid(), "clusteredColumnChart", 260, 90, 700, 240, {
        "Category": {"projections": [col_field("FactCopyEvent", "Detected At")]},
        "Y": {"projections": [measure_field("FactCopyEvent", "Total Copy Events"), measure_field("FactCopyEvent", "Flagged Copy Events")]},
    }, title="Copy Events Over Time")),
    (hid(), visual(hid(), "gauge", 980, 90, 270, 240, {
        "Y": {"projections": [measure_field("FactCopyEvent", "Avg Retention %")]},
    }, title="Avg Column Retention %")),
    (hid(), visual(hid(), "tableEx", 260, 350, 990, 320, {
        "Values": {"projections": [
            col_field("FactCopyEvent", "Detected At"),
            col_field("FactCopyEvent", "engine"),
            col_field("FactCopyEvent", "hosting_workspace_name"),
            col_field("FactCopyEvent", "matched_shortcut_database"),
            col_field("FactCopyEvent", "matched_shortcut_name"),
            col_field("FactCopyEvent", "dest_table"),
            col_field("FactCopyEvent", "retention_pct"),
            col_field("FactCopyEvent", "is_shortcut_read_and_saved_as_is"),
            col_field("FactCopyEvent", "Source Shortcut Exists Now"),
        ]},
    }, title="Copy Event Detail")),
]

# ---------------------------------------------------------------------------
# Page 3: Duplicate Shortcuts
# ---------------------------------------------------------------------------
duplicates_visuals = [
    (hid(), visual(hid(), "card", 20, 90, 200, 110, {
        "Values": {"projections": [measure_field("FactDuplicateShortcutGroup", "Duplicate Groups")]},
    }, title="Duplicate Groups")),
    (hid(), visual(hid(), "card", 240, 90, 200, 110, {
        "Values": {"projections": [measure_field("FactDuplicateShortcutGroup", "High Severity Groups")]},
    }, title="High Severity Groups")),
    (hid(), visual(hid(), "card", 460, 90, 200, 110, {
        "Values": {"projections": [measure_field("FactDuplicateShortcutGroup", "Duplicate Shortcuts (rows)")]},
    }, title="Duplicate Shortcuts (rows)")),
    (hid(), visual(hid(), "slicer", 700, 90, 550, 110, {
        "Values": {"projections": [col_field("FactDuplicateShortcutGroup", "severity")]},
    }, title="Severity")),
    (hid(), visual(hid(), "tableEx", 20, 220, 1230, 450, {
        "Values": {"projections": [
            col_field("FactDuplicateShortcutGroup", "duplicate_group_id"),
            col_field("FactDuplicateShortcutGroup", "severity"),
            col_field("FactDuplicateShortcutGroup", "hosting_workspace_name"),
            col_field("FactDuplicateShortcutGroup", "hosting_item_name"),
            col_field("FactDuplicateShortcutGroup", "shortcut_name"),
            col_field("FactDuplicateShortcutGroup", "source_item_name"),
            col_field("FactDuplicateShortcutGroup", "source_path"),
            col_field("FactDuplicateShortcutGroup", "Group Member Count"),
            col_field("FactDuplicateShortcutGroup", "explanation"),
        ]},
    }, title="Duplicate Shortcut Groups Detail")),
]

# ---------------------------------------------------------------------------
# Page 4: Inventory & Churn
# ---------------------------------------------------------------------------
inventory_visuals = [
    (hid(), visual(hid(), "card", 20, 90, 200, 110, {
        "Values": {"projections": [measure_field("DimShortcut", "Total Shortcuts")]},
    }, title="Total Shortcuts (current)")),
    (hid(), visual(hid(), "card", 240, 90, 200, 110, {
        "Values": {"projections": [measure_field("DimShortcut", "Internal OneLake Shortcuts")]},
    }, title="Internal OneLake Shortcuts")),
    (hid(), visual(hid(), "card", 460, 90, 200, 110, {
        "Values": {"projections": [measure_field("FactShortcutInventoryDiff", "Net Shortcut Change")]},
    }, title="Net Shortcut Change")),
    (hid(), visual(hid(), "lineClusteredColumnComboChart", 700, 90, 550, 320, {
        "Category": {"projections": [col_field("FactShortcutInventoryDiff", "snapshot_ts")]},
        "Y": {"projections": [measure_field("FactShortcutInventoryDiff", "Shortcuts Added")]},
        "Y2": {"projections": [measure_field("FactShortcutInventoryDiff", "Shortcuts Removed")]},
    }, title="Shortcut Churn Over Time")),
    (hid(), visual(hid(), "tableEx", 20, 220, 1230, 450, {
        "Values": {"projections": [
            col_field("FactShortcutInventoryDiff", "snapshot_ts"),
            col_field("FactShortcutInventoryDiff", "change_type"),
            col_field("FactShortcutInventoryDiff", "hosting_workspace_name"),
            col_field("FactShortcutInventoryDiff", "hosting_item_name"),
            col_field("FactShortcutInventoryDiff", "shortcut_name"),
            col_field("FactShortcutInventoryDiff", "target_type"),
            col_field("FactShortcutInventoryDiff", "source_item_name"),
            col_field("FactShortcutInventoryDiff", "source_path"),
        ]},
    }, title="Shortcut Lifecycle Events")),
]

PAGES = [
    page(hid(), "Executive Summary", 0, exec_visuals),
    page(hid(), "Copy Events", 1, copy_events_visuals),
    page(hid(), "Duplicate Shortcuts", 2, duplicates_visuals),
    page(hid(), "Inventory & Churn", 3, inventory_visuals),
]

REPORT_JSON = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.3.0/schema.json",
    "themeCollection": {
        "baseTheme": {
            "name": THEME_NAME,
            "reportVersionAtImport": {"visual": "1.8.95", "report": "2.0.95", "page": "1.3.95"},
            "type": "SharedResources",
        }
    },
    "resourcePackages": [
        {
            "name": "SharedResources",
            "type": "SharedResources",
            "items": [
                {"name": THEME_NAME, "path": f"BaseThemes/{THEME_NAME}.json", "type": "BaseTheme"},
            ],
        }
    ],
    "settings": {
        "useStylableVisualContainerHeader": True,
        "defaultFilterActionIsDataFilter": True,
        "defaultDrillFilterOtherVisuals": True,
        "allowChangeFilterTypes": True,
        "allowInlineExploration": True,
        "useEnhancedTooltips": True,
    },
}

VERSION_JSON = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
    "version": "2.0.0",
}

DEFINITION_PBIR = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
    "version": "4.0",
    "datasetReference": {"byConnection": {"connectionString": f"semanticmodelid={SEMANTIC_MODEL_ID}"}},
}

PLATFORM_JSON = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {"type": "Report", "displayName": "RPT_ShortcutMonitoring"},
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
    write_json(os.path.join(OUT_DIR, "definition", "report.json"), REPORT_JSON)
    write_json(os.path.join(OUT_DIR, "definition", "version.json"), VERSION_JSON)

    pages_json = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json",
        "pageOrder": [p[0] for p in PAGES],
        "activePageName": PAGES[0][0],
    }
    write_json(os.path.join(OUT_DIR, "definition", "pages", "pages.json"), pages_json)

    for page_id, page_json, visuals_defs in PAGES:
        write_json(os.path.join(OUT_DIR, "definition", "pages", page_id, "page.json"), page_json)
        for visual_id, visual_json in visuals_defs:
            visual_json["name"] = visual_id
            write_json(
                os.path.join(OUT_DIR, "definition", "pages", page_id, "visuals", visual_id, "visual.json"),
                visual_json,
            )

    write_json(os.path.join(OUT_DIR, "definition.pbir"), DEFINITION_PBIR)
    write_json(os.path.join(OUT_DIR, ".platform"), PLATFORM_JSON)

    theme_dest = os.path.join(OUT_DIR, "StaticResources", "SharedResources", "BaseThemes", f"{THEME_NAME}.json")
    os.makedirs(os.path.dirname(theme_dest), exist_ok=True)
    shutil.copyfile(THEME_SRC, theme_dest)

    print(f"Report definition written to {OUT_DIR}")


if __name__ == "__main__":
    main()

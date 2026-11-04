# Databricks notebook source
dbutils.widgets.text("endpoint_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("notebook_root", "/Shared/UberEats/notebooks", "Silver notebook folder")
dbutils.widgets.text("bronze_root", "abfss://bronze@ubereatsrisstorage.dfs.core.windows.net", "Bronze storage root")

endpoint_name = dbutils.widgets.get("endpoint_name").strip().lower()
legacy_table_name = dbutils.widgets.get("table_name").strip().lower()
notebook_root = dbutils.widgets.get("notebook_root").strip().rstrip("/")
bronze_root = dbutils.widgets.get("bronze_root").strip().rstrip("/")

# ADF supplies endpoint_name. table_name remains as a compatibility alias for
# manually triggered historical job definitions.
resource_name = endpoint_name or legacy_table_name
notebook_map = {
    "payments": "Silver_Payments",
    "deliveries": "Silver_Deliveries",
    "ratings": "Silver_Ratings",
    "order-status": "Silver_Order_Status",
    "order_status": "Silver_Order_Status",
}

if resource_name not in notebook_map:
    raise ValueError(f"Unsupported API endpoint: {resource_name!r}")

target_notebook = notebook_map[resource_name]
print(f"API endpoint received: {resource_name}")
print(f"Dispatching to: {notebook_root}/{target_notebook}")
dbutils.notebook.run(
    f"{notebook_root}/{target_notebook}",
    0,
    {"bronze_root": bronze_root},
)

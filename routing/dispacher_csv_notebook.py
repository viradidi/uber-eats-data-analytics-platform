# Databricks notebook source
dbutils.widgets.text("file_name", "")
file_name = dbutils.widgets.get("file_name")
dbutils.widgets.text("notebook_root", "../notebooks", "Silver notebook folder")
notebook_root = dbutils.widgets.get("notebook_root").strip().rstrip("/")
print("================================")
print("TABLE NAME RECEIVED:")
print(repr(file_name))
print("================================")

# Clean value
file_name = file_name.strip().lower()
notebook_map = {
     "drivers.csv": "Silver_Drivers",
    "promotions.csv": "Silver_Promotions",
    "zones.csv": "Silver_Zones",
}

# CHECK FOR TABLE EXSISTENCE
if file_name not in notebook_map:
     raise ValueError(f"Unsupported table: {file_name}")
target_notebook  = notebook_map[file_name]
print("target_notebook:", target_notebook)
dbutils.notebook.run(f"{notebook_root}/{target_notebook}", 0)
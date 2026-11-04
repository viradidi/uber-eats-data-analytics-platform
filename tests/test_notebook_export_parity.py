"""Cell-level parity checks for paired Databricks SOURCE and Jupyter exports."""

import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAIRED_NOTEBOOKS = (
    "Silver_Deliveries",
    "Silver_Order_Status",
    "Silver_Payments",
    "Silver_Promotions",
    "Silver_Ratings",
    "Gold_Analytics",
)


def databricks_source_cells(source):
    """Convert Databricks SOURCE export markers into ordered Jupyter cells."""
    source = source.replace("\r\n", "\n").replace("\r", "\n")
    sections = re.split(r"(?m)^# COMMAND ----------\s*$", source)
    cells = []
    for section in sections:
        section = re.sub(r"^# Databricks notebook source\s*\n?", "", section, count=1)
        lines = section.split("\n")
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        if not lines:
            continue

        markdown_index = next(
            (index for index, line in enumerate(lines) if line.strip() == "# MAGIC %md"),
            None,
        )
        if markdown_index is not None:
            code_lines = lines[:markdown_index]
            if any(line.strip() for line in code_lines):
                cells.append(("code", "\n".join(code_lines)))
            markdown_lines = lines[markdown_index + 1 :]
            invalid_lines = [
                line for line in markdown_lines
                if line.strip() and not line.startswith("# MAGIC")
            ]
            if invalid_lines:
                raise AssertionError(
                    "Unsupported text after Databricks %md marker: "
                    + " | ".join(invalid_lines[:3])
                )
            markdown = "\n".join(
                "" if line.strip() == "# MAGIC" else re.sub(r"^# MAGIC ?", "", line)
                for line in markdown_lines
            ).rstrip()
            if markdown.strip():
                cells.append(("markdown", markdown))
        else:
            magic_lines = [line for line in lines if line.strip().startswith("# MAGIC")]
            if magic_lines:
                raise AssertionError(
                    "Unexpected Databricks MAGIC command: " + " | ".join(magic_lines)
                )
            cells.append(("code", "\n".join(lines)))
    return cells


def notebook_cells(notebook):
    return [
        (
            cell.get("cell_type"),
            "".join(cell.get("source", []))
            if isinstance(cell.get("source", []), list)
            else cell.get("source", ""),
        )
        for cell in notebook.get("cells", [])
        if cell.get("cell_type") in {"code", "markdown"}
    ]


class NotebookExportParityTests(unittest.TestCase):
    def test_jupyter_exports_match_canonical_databricks_source_cells(self):
        for name in PAIRED_NOTEBOOKS:
            with self.subTest(notebook=name):
                source_path = ROOT / "notebooks" / f"{name}.py"
                notebook_path = ROOT / "notebooks" / f"{name}.ipynb"
                source_cells = databricks_source_cells(
                    source_path.read_text(encoding="utf-8")
                )
                notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
                self.assertEqual(
                    source_cells,
                    notebook_cells(notebook),
                    f"{name}.ipynb has drifted from canonical {name}.py",
                )

    def test_workspace_importer_uses_canonical_python_exports_for_paired_notebooks(self):
        importer = (
            ROOT / "deployment" / "databricks" / "import_workspace_assets.ps1"
        ).read_text(encoding="utf-8")
        for name in PAIRED_NOTEBOOKS:
            with self.subTest(notebook=name):
                expected = (
                    f'@{{ Source = "notebooks/{name}.py"; '
                    f'Name = "{name}"; Format = "SOURCE" }}'
                )
                self.assertIn(expected, importer)
                self.assertNotIn(
                    f'Source = "notebooks/{name}.ipynb"',
                    importer,
                )


if __name__ == "__main__":
    unittest.main()

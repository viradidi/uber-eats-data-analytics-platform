# Databricks notebook export consistency

## Source of truth

For notebook names that have both a `.py` and an `.ipynb` export, the Databricks SOURCE-format `.py` file is the canonical deployment artifact. The workspace importer intentionally deploys that file as a Databricks SOURCE notebook. The paired `.ipynb` file is kept as a Jupyter-readable representation of the same cells for reviewers and local inspection.

The paired exports covered by the parity check are:

- `Silver_Deliveries`
- `Silver_Order_Status`
- `Silver_Payments`
- `Silver_Promotions`
- `Silver_Ratings`
- `Gold_Analytics`

The notebook source conversion preserves ordered code and Markdown cells. The Jupyter copies are regenerated from the canonical Databricks SOURCE exports, and their code-cell outputs/execution counts are cleared rather than presenting stale run output.

## Notebooks without a paired export

- `Silver_Drivers.py` is the only committed export for Drivers and is deployed as SOURCE.
- `Silver_Zones.ipynb` is the only committed export for Zones and is deployed as JUPYTER.
- The API/CSV dispatchers are Python Databricks SOURCE exports under `routing/`.

## Automated guard

`tests/test_notebook_export_parity.py` compares every code and Markdown cell, in order, between each canonical `.py` export and its paired `.ipynb` representation. It also verifies that the Databricks workspace importer deploys the canonical `.py` file for each paired notebook.

Run the focused check from the repository root:

```powershell
python -m unittest discover -s tests -p "test_notebook_export_parity.py" -v
```

The complete test suite runs this check automatically in GitHub Actions. Update the canonical `.py` export first, regenerate the paired `.ipynb`, and keep both in the same commit. This parity check validates repository artifacts; it does not execute a notebook in a Databricks workspace or establish live runtime acceptance.

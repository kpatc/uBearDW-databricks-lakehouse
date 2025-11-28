#!/bin/bash
set -euo pipefail

# Usage: ./scripts/databricks_import.sh <destination_repo_path>
# Example: ./scripts/databricks_import.sh /Repos/your-org/ubear_dw

DEST_REPO_PATH=${1:-${DATABRICKS_REPO_PATH:-}}
if [[ -z "${DEST_REPO_PATH}" ]]; then
  echo "No destination repo path supplied. Exiting; your repo is likely already linked in Databricks Repos. If you want import, pass the path as argument or set DATABRICKS_REPO_PATH secret."
  exit 0
fi

# Exit if DATABRICKS_HOST or DATABRICKS_TOKEN not available
if [[ -z "${DATABRICKS_HOST:-}" || -z "${DATABRICKS_TOKEN:-}" ]]; then
  echo "DATABRICKS_HOST and DATABRICKS_TOKEN must be defined as environment variables."
  exit 1
fi

# For each python notebook file under databricks_notebooks, import into Databricks workspace
for f in databricks_notebooks/*.py; do
  base=$(basename "$f")
  notebook_name="${base%.*}"
  dest_path="$DEST_REPO_PATH/$notebook_name.py"
  echo "Importing $f to $dest_path"
  databricks workspace import --overwrite --format SOURCE --language PYTHON "$f" "$dest_path"
done

echo "Import completed."

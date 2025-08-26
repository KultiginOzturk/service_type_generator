import os
import json
from typing import Any, Dict, Iterable, List, Optional

from google.cloud import bigquery

from bq_client import get_bq_client
from utils.logger import Logger


logger = Logger(__name__)


def _ensure_output_dir(path: str) -> None:
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def _schema_to_serializable(
    schema: Iterable[bigquery.SchemaField],
) -> List[Dict[str, Any]]:
    def convert_field(field: bigquery.SchemaField) -> Dict[str, Any]:
        item: Dict[str, Any] = {
            "name": field.name,
            "type": field.field_type,
            "mode": field.mode,
            "description": field.description,
        }
        if field.field_type == "RECORD" and field.fields:
            item["fields"] = [
                convert_field(nested) for nested in field.fields
            ]
        return item

    return [convert_field(f) for f in schema]

def _get_table_summary(
    client: bigquery.Client,
    table_ref: bigquery.TableReference,
) -> Dict[str, Any]:
    table = client.get_table(table_ref)
    fqtn = f"{table.project}.{table.dataset_id}.{table.table_id}"
    schema_serialized = _schema_to_serializable(table.schema)

    summary: Dict[str, Any] = {
        "table": fqtn,
        "num_rows": int(table.num_rows or 0),
        "description": table.description,
        "schema": schema_serialized,
    }
    return summary


def export_bigquery_catalog(
    projects: List[str],
    *,
    location: Optional[str] = None,
) -> List[Dict[str, Any]]:
    client = get_bq_client()

    summaries: List[Dict[str, Any]] = []
    for project in projects:
        logger.info(f"Enumerating datasets in project {project}...")
        for ds in client.list_datasets(project=project):
            if location and getattr(ds, "location", None) and ds.location != location:
                continue
            dataset_ref = bigquery.DatasetReference(project, ds.dataset_id)
            logger.info(f"Enumerating tables in {project}.{ds.dataset_id}...")
            for tbl in client.list_tables(dataset_ref):
                table_ref = dataset_ref.table(tbl.table_id)
                try:
                    summaries.append(
                        _get_table_summary(client, table_ref)
                    )
                except Exception as e:
                    fqtn = f"{project}.{ds.dataset_id}.{tbl.table_id}"
                    logger.warning(f"Failed to summarize {fqtn}", error=str(e))
                    summaries.append({"table": fqtn, "error": str(e)})
    return summaries


def write_outputs(
    summaries: List[Dict[str, Any]],
    json_path: str = "output/bq_catalog.json",
    md_path: str = "output/bq_catalog.md",
) -> None:
    _ensure_output_dir(json_path)
    _ensure_output_dir(md_path)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summaries, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"Wrote {json_path}")

    with open(md_path, "w", encoding="utf-8") as f:
        for t in summaries:
            table_name = t.get("table") or "<unknown>"
            f.write(f"## {table_name}\n\n")
            if t.get("error"):
                f.write(f"- error: {t['error']}\n\n")
                continue
            f.write(f"- num_rows: {t.get('num_rows')}\n")
            if t.get("description"):
                f.write(f"- description: {t['description']}\n")
            f.write("\n- schema:\n")
            def write_field(col: Dict[str, Any], indent: int = 2) -> None:
                pad = " " * indent
                name = col.get("name")
                col_type = col.get("type")
                mode = col.get("mode")
                desc = col.get("description") or ""
                f.write(f"{pad}- {name}: {col_type} {mode} - {desc}\n")
                for nested in col.get("fields", []) or []:
                    write_field(nested, indent + 2)

            for col in t.get("schema", []):
                write_field(col)
            f.write("\n")
    logger.info(f"Wrote {md_path}")


def _parse_projects_arg(arg: Optional[str], fallback_project: Optional[str]) -> List[str]:
    if arg:
        return [p.strip() for p in arg.split(",") if p.strip()]
    if fallback_project:
        return [fallback_project]
    return []


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export BigQuery catalog with schemas only")
    parser.add_argument(
        "--projects",
        help="Comma-separated GCP project IDs. Defaults to your credentials' project.",
    )
    parser.add_argument(
        "--location",
        help="Optional BigQuery location to filter datasets (e.g., US, EU, asia-northeast1).",
    )
    parser.add_argument(
        "--json-path",
        default=os.getenv("BQ_CATALOG_JSON_PATH", "output/bq_catalog.json"),
        help="Where to write the JSON output",
    )
    parser.add_argument(
        "--md-path",
        default=os.getenv("BQ_CATALOG_MD_PATH", "output/bq_catalog.md"),
        help="Where to write the Markdown output",
    )

    args = parser.parse_args()

    # Discover default project from credentials
    credentials_client = get_bq_client()
    default_project = credentials_client.project
    projects = _parse_projects_arg(args.projects, default_project)
    if not projects:
        raise SystemExit("No GCP project specified or discoverable from credentials.")

    summaries = export_bigquery_catalog(
        projects,
        location=args.location,
    )
    write_outputs(summaries, json_path=args.json_path, md_path=args.md_path)



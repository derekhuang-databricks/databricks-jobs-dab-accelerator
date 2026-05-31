# Databricks notebook source
# MAGIC %md
# MAGIC # Inventory a job and all its dependencies
# MAGIC
# MAGIC Given a Databricks `job_id`, uses the SDK to collect everything that job touches —
# MAGIC the job settings, every referenced notebook (with source), every referenced pipeline
# MAGIC (with spec + library notebooks), every referenced workspace file, and any child jobs.
# MAGIC
# MAGIC Writes a single JSON manifest to a workspace `target_path`.
# MAGIC
# MAGIC Intended as input to a DAB scaffolder.

# COMMAND ----------

import base64, json, os
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat

# COMMAND ----------

w = WorkspaceClient()
HOME_PATH = f"/Workspace/Users/{w.current_user.me().user_name}"

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID to inventory")
dbutils.widgets.text("manifest_prefix", "migration", "Path to manifest file before job name")
dbutils.widgets.dropdown("include_notebook_source", "true", ["true", "false"], "Include notebook source in manifest")
dbutils.widgets.dropdown("recurse_run_job_tasks", "true", ["true", "false"], "Recursively inventory child jobs")

JOB_ID = int(dbutils.widgets.get("job_id")) if dbutils.widgets.get("job_id") else 0
MANIFEST_PREFIX = f"{dbutils.widgets.get("manifest_prefix")}/" if dbutils.widgets.get("manifest_prefix") else ""
INCLUDE_SOURCE = dbutils.widgets.get("include_notebook_source") == "true"
RECURSE_JOBS = dbutils.widgets.get("recurse_run_job_tasks") == "true"
print(f"job_id={JOB_ID}  include_source={INCLUDE_SOURCE}  recurse_jobs={RECURSE_JOBS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def to_dict(obj):
    if obj is None:
        return None
    if hasattr(obj, "as_dict"):
        try:
            return obj.as_dict()
        except Exception:
            pass
    if isinstance(obj, list):
        return [to_dict(x) for x in obj]
    if isinstance(obj, dict):
        return {k: to_dict(v) for k, v in obj.items()}
    return obj

def fetch_notebook(path: str) -> dict:
    """Return ObjectInfo + (optionally) base64 source for a notebook path."""
    rec = {"path": path}
    try:
        status = w.workspace.get_status(path)
        rec["object_type"] = status.object_type.value if status.object_type else None
        rec["language"] = status.language.value if status.language else None
        rec["object_id"] = status.object_id
    except Exception as e:
        rec["error"] = f"get_status failed: {e}"
        return rec
    if INCLUDE_SOURCE:
        try:
            exp = w.workspace.export(path, format=ExportFormat.SOURCE)
            rec["source_b64"] = exp.content
            rec["source_format"] = "SOURCE"
        except Exception as e:
            rec["source_error"] = str(e)
    return rec

def fetch_file(path: str) -> dict:
    rec = {"path": path}
    try:
        status = w.workspace.get_status(path)
        rec["object_type"] = status.object_type.value if status.object_type else None
        rec["object_id"] = status.object_id
    except Exception as e:
        rec["error"] = str(e)
        return rec
    if INCLUDE_SOURCE:
        try:
            exp = w.workspace.export(path, format=ExportFormat.AUTO)
            rec["source_b64"] = exp.content
        except Exception as e:
            rec["source_error"] = str(e)
    return rec

def fetch_pipeline(pipeline_id: str) -> dict:
    rec = {"pipeline_id": pipeline_id}
    try:
        full = w.pipelines.get(pipeline_id)
        rec["name"] = full.name
        rec["state"] = full.state.value if full.state else None
        rec["creator_user_name"] = getattr(full, "creator_user_name", None)
        rec["run_as_user_name"] = getattr(full, "run_as_user_name", None)
        rec["spec"] = to_dict(full.spec)
        rec["library_notebooks"] = []
        rec["library_files"] = []
        for lib in (getattr(full.spec, "libraries", None) or []):
            nb = getattr(lib, "notebook", None)
            if nb and getattr(nb, "path", None):
                rec["library_notebooks"].append(fetch_notebook(nb.path))
            f = getattr(lib, "file", None)
            if f and getattr(f, "path", None):
                rec["library_files"].append(fetch_file(f.path))
    except Exception as e:
        rec["error"] = str(e)
    return rec

# COMMAND ----------

# MAGIC %md
# MAGIC ## Walk one job

# COMMAND ----------

def inventory_job(job_id: int, _visited: set) -> dict:
    if job_id in _visited:
        return {"job_id": job_id, "note": "already visited (cycle guard)"}
    _visited.add(job_id)

    full = w.jobs.get(job_id)
    rec = {
        "job_id": full.job_id,
        "creator_user_name": getattr(full, "creator_user_name", None),
        "run_as_user_name": getattr(full, "run_as_user_name", None),
        "created_time": getattr(full, "created_time", None),
        "settings": to_dict(full.settings),
        "notebooks": [],
        "files": [],
        "pipelines": [],
        "child_jobs": [],
        "task_refs": [],
    }

    seen_nb, seen_file, seen_pipe = set(), set(), set()
    for t in (getattr(full.settings, "tasks", None) or []):
        task_key = t.task_key

        nb_task = getattr(t, "notebook_task", None)
        if nb_task and getattr(nb_task, "notebook_path", None):
            p = nb_task.notebook_path
            rec["task_refs"].append({"task_key": task_key, "kind": "notebook", "ref": p})
            if p not in seen_nb:
                seen_nb.add(p)
                rec["notebooks"].append(fetch_notebook(p))

        py_task = getattr(t, "spark_python_task", None)
        if py_task and getattr(py_task, "python_file", None):
            p = py_task.python_file
            rec["task_refs"].append({"task_key": task_key, "kind": "python_file", "ref": p})
            if p.startswith("/Workspace") or p.startswith("/Repos"):
                if p not in seen_file:
                    seen_file.add(p)
                    rec["files"].append(fetch_file(p))

        sql_task = getattr(t, "sql_task", None)
        if sql_task and getattr(sql_task, "file", None) and getattr(sql_task.file, "path", None):
            p = sql_task.file.path
            rec["task_refs"].append({"task_key": task_key, "kind": "sql_file", "ref": p})
            if p not in seen_file:
                seen_file.add(p)
                rec["files"].append(fetch_file(p))

        dbt_task = getattr(t, "dbt_task", None)
        if dbt_task and getattr(dbt_task, "project_directory", None):
            rec["task_refs"].append({"task_key": task_key, "kind": "dbt_project", "ref": dbt_task.project_directory})

        pipe_task = getattr(t, "pipeline_task", None)
        if pipe_task and getattr(pipe_task, "pipeline_id", None):
            pid = pipe_task.pipeline_id
            rec["task_refs"].append({"task_key": task_key, "kind": "pipeline", "ref": pid})
            if pid not in seen_pipe:
                seen_pipe.add(pid)
                rec["pipelines"].append(fetch_pipeline(pid))

        run_job_task = getattr(t, "run_job_task", None)
        if run_job_task and getattr(run_job_task, "job_id", None):
            child_id = run_job_task.job_id
            rec["task_refs"].append({"task_key": task_key, "kind": "run_job", "ref": child_id})
            if RECURSE_JOBS:
                rec["child_jobs"].append(inventory_job(child_id, _visited))

    return rec

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

root = inventory_job(JOB_ID, _visited=set())

manifest = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "workspace_host": w.config.host,
    "root_job_id": JOB_ID,
    "include_notebook_source": INCLUDE_SOURCE,
    "recurse_run_job_tasks": RECURSE_JOBS,
    "job": root,
}

def _count_jobs(j):
    return 1 + sum(_count_jobs(c) for c in j.get("child_jobs", []))
def _count_notebooks(j):
    return len(j.get("notebooks", [])) + sum(_count_notebooks(c) for c in j.get("child_jobs", []))
def _count_files(j):
    return len(j.get("files", [])) + sum(_count_files(c) for c in j.get("child_jobs", []))
def _count_pipelines(j):
    n = len(j.get("pipelines", []))
    for p in j.get("pipelines", []):
        n += len(p.get("library_notebooks", []))
    return n + sum(_count_pipelines(c) for c in j.get("child_jobs", []))

manifest["summary"] = {
    "jobs": _count_jobs(root),
    "notebooks_referenced_by_tasks": _count_notebooks(root),
    "files_referenced_by_tasks": _count_files(root),
    "pipelines_referenced_by_tasks": _count_pipelines(root),
}
print(json.dumps(manifest["summary"], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write manifest to workspace

# COMMAND ----------

# Get job name from job ID
job = w.jobs.get(JOB_ID)
job_name = getattr(job.settings, "name", None)

# Strip "[dev <user>] [dev] " prefix from job_name segment in job_name
_short = w.current_user.me().user_name.split("@")[0].replace(".", "_")
DEV_PREFIX = f"[dev {_short}] [dev] "
job_name = job_name.replace(DEV_PREFIX, "")

path_to_target = f"{HOME_PATH}/{MANIFEST_PREFIX}{job_name}/dab_inventory.json"

parent = path_to_target.rsplit("/", 1)[0]
print(parent)
if parent:
    w.workspace.mkdirs(parent)

content = json.dumps(manifest, indent=2, default=str).encode()
w.workspace.upload(
    path=path_to_target,
    content=content,
    format=ImportFormat.AUTO,
    overwrite=True,
)
print(f"""Wrote manifest to {path_to_target} ({len(content):,} bytes)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview

# COMMAND ----------

rows = []
def _collect(j, parent=None):
    label = (j.get("settings") or {}).get("name") or str(j.get("job_id"))
    rows.append(("job", str(j.get("job_id")), label, parent))
    for nb in j.get("notebooks", []):
        rows.append(("notebook", nb.get("path"), nb.get("language"), label))
    for f in j.get("files", []):
        rows.append(("file", f.get("path"), "", label))
    for p in j.get("pipelines", []):
        rows.append(("pipeline", p.get("pipeline_id"), p.get("name"), label))
        for nb in p.get("library_notebooks", []):
            rows.append(("pipeline_notebook", nb.get("path"), nb.get("language"), p.get("name")))
    for c in j.get("child_jobs", []):
        _collect(c, parent=label)
_collect(root)

display(spark.createDataFrame(rows, ["kind", "id_or_path", "detail", "parent"]))
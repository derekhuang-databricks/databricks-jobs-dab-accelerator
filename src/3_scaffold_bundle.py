# Databricks notebook source
# MAGIC %md
# MAGIC # Scaffold a Databricks Asset Bundle from a job
# MAGIC
# MAGIC Given a `job_id`, queries the job's task lineage via the SDK — every referenced notebook,
# MAGIC python file, and pipeline (with its library notebooks) — and recreates everything as a
# MAGIC deployable bundle in the workspace:
# MAGIC
# MAGIC ```
# MAGIC <bundle_name>/
# MAGIC   databricks.yml
# MAGIC   resources/
# MAGIC     <job_resource_name>.job.yml
# MAGIC     <pipeline_resource_name>.pipeline.yml
# MAGIC     <dashboard_resource_name>.dashboard.yml
# MAGIC   src/
# MAGIC     ... all assets referenced by the job ...
# MAGIC ```
# MAGIC
# MAGIC The job + pipeline YAMLs are populated from the live job settings and pipeline spec,
# MAGIC with paths rewritten to point into `src/`.

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
HOME_PATH = f"/Workspace/Users/{w.current_user.me().user_name}"

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID to inventory")
dbutils.widgets.text("manifest_prefix", "migration", "Manifest Prefix")
dbutils.widgets.text("job_resource_name", "main", "Basename for resources/<x>.job.yml")
dbutils.widgets.text("pipeline_resource_name", "main", "Basename for resources/<x>.pipeline.yml")
dbutils.widgets.text("dashboard_resource_name", "main", "Basename for resources/<x>.dashboard.yml")
dbutils.widgets.dropdown("overwrite", "true", ["true", "false"], "Overwrite existing bundle files")

JOB_ID = int(dbutils.widgets.get("job_id")) if dbutils.widgets.get("job_id") else 0
# Get job name from job ID
job = w.jobs.get(JOB_ID)
job_name = getattr(job.settings, "name", '')

# Strip "[dev <user>] [dev] " prefix from job_name segment in job_name
_short = w.current_user.me().user_name.split("@")[0].replace(".", "_")
DEV_PREFIX = f"[dev {_short}] [dev] "
job_name = job_name.replace(DEV_PREFIX, "")

MANIFEST_PREFIX = dbutils.widgets.get("manifest_prefix")
bundle_name = [f for f in os.listdir(f"{HOME_PATH}/{MANIFEST_PREFIX}/{job_name}/") if '.' not in f][0] if os.path.exists(f"{HOME_PATH}/{MANIFEST_PREFIX}/{job_name}") else ''
bundle_root = f"""{HOME_PATH}/{MANIFEST_PREFIX}/{job_name}/{bundle_name}"""

JOB_RES = dbutils.widgets.get("job_resource_name").strip()
PIPE_RES = dbutils.widgets.get("pipeline_resource_name").strip()
DASH_RES = dbutils.widgets.get("dashboard_resource_name").strip()
OVERWRITE = dbutils.widgets.get("overwrite") == "true"

SRC_ROOT = f"{bundle_root}/src"
RES_ROOT = f"{bundle_root}/resources"
print(f"job_id      = {JOB_ID}")
print(f"bundle_root = {bundle_root}")
print(f"src/        = {SRC_ROOT}")
print(f"resources/  = {RES_ROOT}")


# COMMAND ----------

import base64, json, yaml
from os.path import dirname, basename, commonpath
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language, ObjectType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Query job & collect lineage
# MAGIC
# MAGIC Pulls the job, then walks `settings.tasks` to discover every referenced asset:
# MAGIC notebooks, python files, sql files, pipelines (and their library notebooks/files),
# MAGIC and child jobs.

# COMMAND ----------

def to_dict(obj):
    if obj is None: return None
    if hasattr(obj, "as_dict"):
        try: return obj.as_dict()
        except Exception: pass
    if isinstance(obj, list):  return [to_dict(x) for x in obj]
    if isinstance(obj, dict):  return {k: to_dict(v) for k, v in obj.items()}
    return obj

def lookup_language(path):
    try:
        status = w.workspace.get_status(path)
        return status.language.value if status.language else "PYTHON"
    except Exception:
        return "PYTHON"

job = w.jobs.get(JOB_ID)
job_settings_dict = to_dict(job.settings)
print(f"Job: {job_settings_dict.get('name')} ({JOB_ID})")

referenced_notebooks = {}    # path -> language
referenced_files     = set()  # path
referenced_pipelines = []     # list of {pipeline_id, name, spec, lib_notebooks, lib_files}
referenced_child_jobs = []

for t in (job.settings.tasks or []):
    if t.notebook_task and t.notebook_task.notebook_path:
        p = t.notebook_task.notebook_path
        referenced_notebooks.setdefault(p, lookup_language(p))
    if t.spark_python_task and t.spark_python_task.python_file:
        if t.spark_python_task.python_file.startswith("/"):
            referenced_files.add(t.spark_python_task.python_file)
    if t.sql_task and t.sql_task.file and t.sql_task.file.path:
        referenced_files.add(t.sql_task.file.path)
    if t.pipeline_task and t.pipeline_task.pipeline_id:
        pid = t.pipeline_task.pipeline_id
        try:
            full = w.pipelines.get(pid)
            spec_dict = to_dict(full.spec) or {}
            lib_nbs, lib_fs = {}, set()
            for lib in (full.spec.libraries or []):
                if lib.notebook and lib.notebook.path:
                    lib_nbs.setdefault(lib.notebook.path, lookup_language(lib.notebook.path))
                if lib.file and lib.file.path:
                    lib_fs.add(lib.file.path)
            referenced_pipelines.append({
                "pipeline_id": pid,
                "name": full.name,
                "spec": spec_dict,
                "lib_notebooks": lib_nbs,
                "lib_files": lib_fs,
            })
        except Exception as e:
            print(f"  ! pipeline {pid}: {e}")
    if t.run_job_task and t.run_job_task.job_id:
        referenced_child_jobs.append(t.run_job_task.job_id)

print(f"Notebooks:   {len(referenced_notebooks)}")
print(f"Files:       {len(referenced_files)}")
print(f"Pipelines:   {len(referenced_pipelines)} (with "
      f"{sum(len(p['lib_notebooks']) for p in referenced_pipelines)} library notebooks)")
print(f"Child jobs:  {len(referenced_child_jobs)}")
for p, l in list(referenced_notebooks.items())[:5]:
    print(f"  notebook  {l:6s}  {p}")
for pipe in referenced_pipelines:
    print(f"  pipeline  {pipe['pipeline_id']}  {pipe['name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Path mapping — common prefix → `src/`
# MAGIC
# MAGIC Computes the deepest directory shared by every referenced asset and uses it as the
# MAGIC project root. Each asset's path under that root becomes its location under `src/`.

# COMMAND ----------

all_paths = list(referenced_notebooks.keys()) + list(referenced_files)
for pipe in referenced_pipelines:
    all_paths += list(pipe["lib_notebooks"].keys())
    all_paths += list(pipe["lib_files"])
all_paths = [p for p in all_paths if p and p.startswith("/")]

if len(all_paths) > 1:
    PROJECT_ROOT = commonpath(all_paths)
elif len(all_paths) == 1:
    PROJECT_ROOT = dirname(all_paths[0])
else:
    PROJECT_ROOT = HOME_PATH
print(f"Detected project root: {PROJECT_ROOT}")

def to_src_rel(p):
    if p.startswith(PROJECT_ROOT + "/"):
        return p[len(PROJECT_ROOT):].lstrip("/")
    return basename(p)

for p in all_paths[:5]:
    print(f"  {p}  ->  src/{to_src_rel(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Copy referenced assets to `<bundle>/src/`

# COMMAND ----------

LANG_MAP = {
    "PYTHON": Language.PYTHON,
    "SQL": Language.SQL,
    "SCALA": Language.SCALA,
    "R": Language.R,
}

EXT_MAP = {
    "PYTHON": ".ipynb",
    "SQL": ".sql",
    "R": ".r",
    "SCALA": ".scala",
}

w.workspace.mkdirs(SRC_ROOT)
w.workspace.mkdirs(RES_ROOT)

copy_results = []

def copy_notebook(src, lang):
    lang_upper = (lang or "PYTHON").upper()
    ext = EXT_MAP.get(lang_upper, "")
    # yaml_dst keeps the language-specific extension (e.g. .ipynb) so YAML
    # references resolve; dst is the actual workspace path, which for PYTHON
    # has no extension because we import as JUPYTER format.
    yaml_dst = f"{SRC_ROOT}/{to_src_rel(src)}{ext}"
    dst = yaml_dst
    try:
        w.workspace.mkdirs(dirname(dst))
        if lang_upper == "PYTHON":
            dst = dst[: -len(ext)] if ext else dst
            exp = w.workspace.export(src, format=ExportFormat.JUPYTER)
            w.workspace.import_(
                path=dst, format=ImportFormat.JUPYTER,
                content=exp.content, overwrite=OVERWRITE,
            )
        else:
            # Write as a workspace FILE (not a notebook) so the language
            # extension on disk matches the YAML reference.
            exp = w.workspace.export(src, format=ExportFormat.SOURCE)
            raw = base64.b64decode(exp.content)
            w.workspace.upload(
                path=dst, content=raw, format=ImportFormat.AUTO, overwrite=OVERWRITE,
            )
        copy_results.append({"kind": "notebook", "src": src, "dst": dst, "yaml_dst": yaml_dst, "status": "ok"})
    except Exception as e:
        copy_results.append({"kind": "notebook", "src": src, "dst": dst, "yaml_dst": yaml_dst, "status": f"error: {e}"})

def copy_file(src):
    dst = f"{SRC_ROOT}/{to_src_rel(src)}"
    try:
        exp = w.workspace.export(src, format=ExportFormat.AUTO)
        raw = base64.b64decode(exp.content)
        w.workspace.mkdirs(dirname(dst))
        w.workspace.upload(path=dst, content=raw, format=ImportFormat.AUTO, overwrite=OVERWRITE)
        copy_results.append({"kind": "file", "src": src, "dst": dst, "yaml_dst": dst, "status": "ok"})
    except Exception as e:
        copy_results.append({"kind": "file", "src": src, "dst": dst, "yaml_dst": dst, "status": f"error: {e}"})

for p, lang in referenced_notebooks.items():
    copy_notebook(p, lang)
for p in referenced_files:
    copy_file(p)
for pipe in referenced_pipelines:
    for p, lang in pipe["lib_notebooks"].items():
        copy_notebook(p, lang)
    for p in pipe["lib_files"]:
        copy_file(p)

ok = sum(1 for r in copy_results if r["status"] == "ok")
print(f"Copied {ok}/{len(copy_results)} assets to {SRC_ROOT}")
for r in copy_results:
    if r["status"] != "ok":
        print(f"  ! {r['src']} -> {r['status']}")

# Verify each Python .ipynb reference exists at that exact path in the workspace.
# If it landed at .py instead, rename it to .ipynb so the generated YAML resolves.
for r in copy_results:
    if r["status"] != "ok" or r["kind"] != "notebook" or not r["dst"].endswith(".ipynb"):
        continue
    dst = r["dst"]
    try:
        w.workspace.get_status(dst)
        continue
    except Exception:
        pass
    py_alt = dst[:-len(".ipynb")] + ".py"
    try:
        w.workspace.get_status(py_alt)
    except Exception:
        print(f"  ! missing {dst} and no .py alternative")
        continue
    try:
        exp = w.workspace.export(py_alt, format=ExportFormat.JUPYTER)
        w.workspace.import_(
            path=dst, format=ImportFormat.JUPYTER,
            content=exp.content, overwrite=True,
        )
        w.workspace.delete(py_alt)
        print(f"  renamed {py_alt} -> {dst}")
    except Exception as e:
        print(f"  ! rename failed {py_alt} -> {dst}: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. YAML helpers

# COMMAND ----------

def write_yaml(path, doc, header_comment=None):
    body = yaml.safe_dump(doc, sort_keys=False, default_flow_style=False)
    if header_comment:
        body = header_comment.rstrip() + "\n" + body
    w.workspace.upload(
        path=path, content=body.encode(), format=ImportFormat.AUTO, overwrite=OVERWRITE,
    )
    print(f"  wrote {path} ({len(body)} bytes)")

def rewrite_to_src(original_path: str) -> str:
    """Rewrite an absolute workspace path to `../src/<rel>` using yaml_dst
    (which keeps the language-specific extension, e.g. .ipynb, even though
    the actual file on disk has no extension)."""
    if not original_path:
        return original_path
    for r in copy_results:
        if r["status"] == "ok" and r["src"] == original_path:
            return "../src/" + r["yaml_dst"][len(SRC_ROOT) + 1:]
    return original_path


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `databricks.yml`

# COMMAND ----------

def build_databricks_yml():
    detected_vars = {}
    detected_target_vars = {}
    if referenced_pipelines:
        spec = referenced_pipelines[0]["spec"] or {}
        if spec.get("catalog"):
            detected_vars["default_catalog"] = "Default catalog"
            detected_target_vars["default_catalog"] = spec["catalog"]
        schema_val = spec.get("schema") or spec.get("target")
        if schema_val:
            detected_vars["default_schema"] = "Default schema"
            detected_target_vars["default_schema"] = schema_val

    host = w.config.host if w.config.host.endswith("/") else w.config.host + "/"
    lines = [
        f"# This is a Databricks asset bundle definition for {bundle_name}.",
        "# See https://docs.databricks.com/dev-tools/bundles/index.html for documentation.",
        "bundle:",
        f"  name: {bundle_name}",
        "",
        "# The .yml files in the resources/ folder contain all resources in the bundle.",
        "include:",
        "  - resources/*.yml",
        "  - resources/**/*.yml",
        "",
    ]
    if detected_vars:
        lines.append("# Variable declarations. Values assigned in the dev/prod targets below.")
        lines.append("variables:")
        for name, desc in detected_vars.items():
            lines.append(f"  {name}:")
            lines.append(f"    description: {desc}")
        lines.append("")

    lines += [
        "targets:",
        "  dev:",
        "    mode: development",
        "    default: true",
        "    workspace:",
        f"      host: {host}",
    ]
    if detected_target_vars:
        lines.append("    variables:")
        for name, val in detected_target_vars.items():
            lines.append(f"      {name}: {val}")

    lines += [
        "  # prod:",
        "  #   mode: production",
        "  #   workspace:",
        f"  #     host: {host}",
        "  #     root_path: ${workspace.root_path}/.bundle/${bundle.name}/${bundle.target}",
    ]
    if detected_target_vars:
        lines.append("  #   variables:")
        for name in detected_target_vars:
            lines.append(f"  #     {name}: <TODO>")

    return "\n".join(lines) + "\n"

body = build_databricks_yml()
w.workspace.upload(
    path=f"{bundle_root}/databricks.yml",
    content=body.encode(),
    format=ImportFormat.AUTO,
    overwrite=OVERWRITE,
)
print(f"  wrote {bundle_root}/databricks.yml ({len(body)} bytes)")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `resources/<job>.job.yml`
# MAGIC
# MAGIC Built from the queried job settings: tasks are preserved, but notebook/file paths
# MAGIC are rewritten to `../src/...` and `pipeline_task.pipeline_id` is rewritten to a
# MAGIC bundle ref so it resolves to the deployed pipeline resource.

# COMMAND ----------

def minimal_task(t):
    out = {"task_key": t["task_key"]}
    nb = t.get("notebook_task") or {}
    if nb.get("notebook_path"):
        out["notebook_task"] = {"notebook_path": rewrite_to_src(nb["notebook_path"])}
    elif (t.get("spark_python_task") or {}).get("python_file"):
        out["spark_python_task"] = {"python_file": rewrite_to_src(t["spark_python_task"]["python_file"])}
    elif ((t.get("sql_task") or {}).get("file") or {}).get("path"):
        out["sql_task"] = {"file": {"path": rewrite_to_src(t["sql_task"]["file"]["path"])}}
    elif (t.get("pipeline_task") or {}).get("pipeline_id"):
        out["pipeline_task"] = {"pipeline_id": "${resources.pipelines." + PIPE_RES + "_pipeline.id}"}
    elif (t.get("dashboard_task") or {}).get("dashboard_id"):
        out["dashboard_task"] = {"dashboard_id": "${resources.dashboards." + DASH_RES + "_dashboard.id}"}
    elif (t.get("run_job_task") or {}).get("job_id"):
        out["run_job_task"] = {"job_id": t["run_job_task"]["job_id"]}
    deps = t.get("depends_on") or []
    if deps:
        out["depends_on"] = [{"task_key": d["task_key"]} for d in deps if d.get("task_key")]
    return out

name = job_settings_dict.get("name")
# Strip "[dev <user>] [dev] " prefix from job_name segment in name
_short = w.current_user.me().user_name.split("@")[0].replace(".", "_")
DEV_PREFIX = f"[dev {_short}] [dev] "
name = job_name.replace(DEV_PREFIX, "")

job_yaml_body = {"name": name}

sch = job_settings_dict.get("schedule") or {}
if sch.get("quartz_cron_expression"):
    job_yaml_body["schedule"] = {
        k: sch[k] for k in ("quartz_cron_expression", "timezone_id", "pause_status") if sch.get(k)
    }

def topo_sort_tasks(tasks):
    """Order tasks so dependencies precede dependents (Kahn's). Stable on input order."""
    by_key = {t["task_key"]: t for t in tasks}
    in_deg = {k: 0 for k in by_key}
    children = {k: [] for k in by_key}
    for t in tasks:
        for d in (t.get("depends_on") or []):
            dk = d.get("task_key")
            if dk and dk in by_key:
                in_deg[t["task_key"]] += 1
                children[dk].append(t["task_key"])
    ready = [t["task_key"] for t in tasks if in_deg[t["task_key"]] == 0]
    out = []
    while ready:
        k = ready.pop(0)
        out.append(by_key[k])
        for c in children[k]:
            in_deg[c] -= 1
            if in_deg[c] == 0:
                ready.append(c)
    seen = {t["task_key"] for t in out}
    for t in tasks:
        if t["task_key"] not in seen:
            out.append(t)
    return out

sorted_tasks = topo_sort_tasks(job_settings_dict.get("tasks") or [])
job_yaml_body["tasks"] = [minimal_task(t) for t in sorted_tasks]

envs = job_settings_dict.get("environments") or []
if envs:
    job_yaml_body["environments"] = [
        {
            "environment_key": e.get("environment_key"),
            "spec": {k: v for k, v in (e.get("spec") or {}).items() if k == "environment_version"},
        }
        for e in envs if e.get("environment_key")
    ]

write_yaml(
    f"{RES_ROOT}/{JOB_RES}.job.yml",
    {"resources": {"jobs": {f"{JOB_RES}_job": job_yaml_body}}},
    header_comment=f"# Generated from job {JOB_ID}.\n",
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `resources/<pipeline>.pipeline.yml`

# COMMAND ----------

if referenced_pipelines:
    pipe = referenced_pipelines[0]
    spec = pipe["spec"] or {}

    libs = []
    for lib in (spec.get("libraries") or []):
        if (lib.get("notebook") or {}).get("path"):
            libs.append({"notebook": {"path": rewrite_to_src(lib["notebook"]["path"])}})
        elif (lib.get("file") or {}).get("path"):
            libs.append({"file": {"path": rewrite_to_src(lib["file"]["path"])}})
        elif lib.get("glob"):
            libs.append(lib)

    lib_src_paths = [
        r["dst"][len(SRC_ROOT) + 1:]
        for r in copy_results
        if r["status"] == "ok" and r["src"] in pipe["lib_notebooks"]
    ]
    if len(lib_src_paths) > 1:
        root_rel = commonpath(lib_src_paths)
    elif len(lib_src_paths) == 1:
        root_rel = dirname(lib_src_paths[0])
    else:
        root_rel = ""
    root_path = f"../src/{root_rel}" if root_rel else "../src"

    pipeline_name = spec.get("name")
    # Strip "[dev <user>] [dev] " prefix from job_name segment in pipeline_name
    _short = w.current_user.me().user_name.split("@")[0].replace(".", "_")
    DEV_PREFIX = f"[dev {_short}] [dev] "
    pipeline_name = pipeline_name.replace(DEV_PREFIX, "")

    pipeline_yaml_body = {
        "name": pipeline_name or ("${bundle.name}-" + PIPE_RES.replace("_", "-")),
        "serverless": True,
        "root_path": root_path,
    }
    if spec.get("catalog"):
        pipeline_yaml_body["catalog"] = spec["catalog"]
    schema_val = spec.get("schema") or spec.get("target")
    if schema_val:
        pipeline_yaml_body["schema"] = schema_val
    if spec.get("configuration"):
        pipeline_yaml_body["configuration"] = spec["configuration"]
    pipeline_yaml_body["libraries"] = libs or [{"notebook": {"path": "../src/TODO"}}]
else:
    pipeline_yaml_body = {
        "name": "${bundle.name}-" + PIPE_RES.replace("_", "-"),
        "serverless": True,
        "root_path": "../src",
        "libraries": [{"notebook": {"path": "../src/TODO"}}],
    }

write_yaml(
    f"{RES_ROOT}/{PIPE_RES}.pipeline.yml",
    {"resources": {"pipelines": {f"{PIPE_RES}_pipeline": pipeline_yaml_body}}},
    header_comment="# Generated pipeline resource.\n",
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. `resources/<dashboard>.dashboard.yml`
# MAGIC
# MAGIC Stubbed — fill in `file_path` and `warehouse_id` for any dashboard you want bundled.

# COMMAND ----------

dashboard_yml = {
    "resources": {
        "dashboards": {
            f"{DASH_RES}_dashboard": {
                "display_name": "${bundle.name} " + DASH_RES.replace("_", " ").title(),
                "warehouse_id": "TODO_warehouse_id",
                "file_path": "../src/TODO_dashboard.lvdash.json",
                "dataset_catalog": "TODO_catalog",
                "dataset_schema": "TODO_schema",
                "embed_credentials": True,
            }
        }
    }
}
write_yaml(
    f"{RES_ROOT}/{DASH_RES}.dashboard.yml",
    dashboard_yml,
    header_comment="# Generated dashboard resource. Fill in TODOs before deploying.\n",
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

print(f"Bundle scaffolded at: {bundle_root}")
print("")
print("Next steps:")
print(f"  1. databricks workspace export-dir {bundle_root} ./{bundle_name} --overwrite")
print(f"  2. cd ./{bundle_name}")
print("  3. databricks bundle validate")
print("  4. databricks bundle deploy --target dev")

rows = []
for r in copy_results:
    rows.append((r["kind"], r["src"], r["dst"], r["status"]))
for f in ["databricks.yml", f"resources/{JOB_RES}.job.yml", f"resources/{PIPE_RES}.pipeline.yml", f"resources/{DASH_RES}.dashboard.yml"]:
    rows.append(("yaml", f, f"{bundle_root}/{f}", "ok"))
display(spark.createDataFrame(rows, ["kind", "source", "dest", "status"]))
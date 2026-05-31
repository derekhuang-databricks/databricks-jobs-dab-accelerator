# Databricks notebook source
# MAGIC %md
# MAGIC # Extract job assets from a `dab_inventory.json` manifest
# MAGIC
# MAGIC Reads a manifest produced by `inventory_workspace_assets` and materializes every referenced
# MAGIC notebook, workspace file, and pipeline library back into the workspace, preserving the
# MAGIC structure of each source path under `HOME_PATH`.
# MAGIC
# MAGIC For each asset, the destination is:
# MAGIC
# MAGIC ```
# MAGIC <HOME_PATH>/<source_path stripped of its home prefix>
# MAGIC ```
# MAGIC
# MAGIC If the source came from the same `HOME_PATH`, this restores in place. If `HOME_PATH`
# MAGIC points to a different user/folder, the assets are relocated there with the same relative
# MAGIC structure.

# COMMAND ----------

import base64, json, re
from os.path import dirname
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

w = WorkspaceClient()
print(f"Workspace: {w.config.host}")

# COMMAND ----------

dbutils.widgets.text("manifest_prefix", "migration", "Path to manifest file before job name")
dbutils.widgets.dropdown("overwrite", "true", ["true", "false"], "Overwrite existing files")
dbutils.widgets.text("job_id", "", "Job ID to inventory")

HOME_PATH = f"/Workspace/Users/{w.current_user.me().user_name}"
MANIFEST_PREFIX = dbutils.widgets.get("manifest_prefix")
OVERWRITE = dbutils.widgets.get("overwrite") == "true"
JOB_ID = dbutils.widgets.get("job_id")

print(f"home_path={HOME_PATH}")
print(f"overwrite={OVERWRITE}")

# COMMAND ----------

JOB_ID = int(dbutils.widgets.get("job_id")) if dbutils.widgets.get("job_id") else 0
# Get job name from job ID
job = w.jobs.get(JOB_ID)
job_name = getattr(job.settings, "name", None)

# Strip "[dev <user>] [dev] " prefix from job_name segment in job_name
_short = w.current_user.me().user_name.split("@")[0].replace(".", "_")
DEV_PREFIX = f"[dev {_short}] [dev] "
job_name = job_name.replace(DEV_PREFIX, "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load manifest

# COMMAND ----------

SOURCE_PATH = f"{HOME_PATH}/{MANIFEST_PREFIX}/{job_name}"
exp = w.workspace.export(f"{SOURCE_PATH}/dab_inventory.json", format=ExportFormat.AUTO)
manifest = json.loads(base64.b64decode(exp.content).decode())
print(json.dumps(manifest.get("summary", {}), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Collect all asset records (recurse child jobs + pipeline libs)

# COMMAND ----------

assets = []  # list of (kind, path, language_or_None, source_b64_or_None)

def collect(job):
    for nb in job.get("notebooks", []):
        if nb.get("path") and not nb.get("error"):
            assets.append(("notebook", nb["path"], nb.get("language"), nb.get("source_b64")))
    for f in job.get("files", []):
        if f.get("path") and not f.get("error"):
            assets.append(("file", f["path"], None, f.get("source_b64")))
    for p in job.get("pipelines", []):
        for nb in p.get("library_notebooks", []):
            if nb.get("path") and not nb.get("error"):
                assets.append(("notebook", nb["path"], nb.get("language"), nb.get("source_b64")))
        for f in p.get("library_files", []):
            if f.get("path") and not f.get("error"):
                assets.append(("file", f["path"], None, f.get("source_b64")))
    for c in job.get("child_jobs", []):
        collect(c)

collect(manifest["job"])

seen = set()
unique = []
for a in assets:
    if a[1] in seen:
        continue
    seen.add(a[1])
    unique.append(a)
assets = unique
print(f"Unique assets to extract: {len(assets)}")
for kind, path, lang, _ in assets[:20]:
    print(f"  {kind:8s} {lang or '':6s} {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Path derivation
# MAGIC
# MAGIC For each source path, strip the home prefix it came from, then re-anchor under `HOME_PATH`.
# MAGIC Falls back to stripping any `/Workspace/Users/<email>/` prefix so manifests captured from
# MAGIC another user's workspace land correctly under the current `HOME_PATH`.

# COMMAND ----------

USER_HOME_RE = re.compile(r"^/Workspace/Users/[^/]+/(.+)$")

def derive_dest(src_path: str) -> str:
    if src_path.startswith(HOME_PATH + "/"):
        rel = src_path[len(HOME_PATH):].lstrip("/")
    else:
        m = USER_HOME_RE.match(src_path)
        rel = m.group(1) if m else src_path.lstrip("/")
    return "/".join(rel.split("/")[1::])

# Strip "[dev <user>] [dev] " prefix from job_name segment in SOURCE_PATH
_short = w.current_user.me().user_name.split("@")[0].replace(".", "_")
DEV_PREFIX = f"[dev {_short}] [dev] "
SOURCE_PATH = SOURCE_PATH.replace(DEV_PREFIX, "")

for kind, src_path, lang, _ in assets[:5]:
    target_path = f"{SOURCE_PATH}/{derive_dest(src_path)}"
    print(f"  {src_path}  ->  {target_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extract — fetch source if missing, then write to derived path
# MAGIC
# MAGIC If `source_b64` was embedded in the manifest we use it directly. Otherwise we re-export via the SDK.

# COMMAND ----------

LANG_MAP = {
    "PYTHON": Language.PYTHON,
    "SQL": Language.SQL,
    "SCALA": Language.SCALA,
    "R": Language.R,
}

def ensure_dir(p):
    d = dirname(p)
    if d:
        w.workspace.mkdirs(d)

results = []
for kind, src_path, lang, b64 in assets:
    dst_path = f"{SOURCE_PATH}/{derive_dest(src_path)}"
    try:
        if b64 is None:
            fmt = ExportFormat.SOURCE if kind == "notebook" else ExportFormat.AUTO
            exp = w.workspace.export(src_path, format=fmt)
            b64 = exp.content
        raw = base64.b64decode(b64)

        ensure_dir(dst_path)
        if kind == "notebook":
            lang_enum = LANG_MAP.get((lang or "PYTHON").upper(), Language.PYTHON)
            w.workspace.import_(
                path=dst_path,
                format=ImportFormat.SOURCE,
                language=lang_enum,
                content=b64,
                overwrite=OVERWRITE,
            )
        else:
            w.workspace.upload(
                path=dst_path,
                content=raw,
                format=ImportFormat.AUTO,
                overwrite=OVERWRITE,
            )
        results.append({"src": src_path, "dst": dst_path, "kind": kind, "lang": lang, "bytes": len(raw), "status": "ok"})
    except Exception as e:
        results.append({"src": src_path, "dst": dst_path, "kind": kind, "lang": lang, "bytes": 0, "status": f"error: {e}"})

ok = sum(1 for r in results if r["status"] == "ok")
err = len(results) - ok
print(f"Extracted: {ok} ok, {err} errors")
for r in results:
    if r["status"] != "ok":
        print(f"  ! {r['src']} -> {r['status']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Results

# COMMAND ----------

display(spark.createDataFrame(
    [(r["kind"], r["lang"] or "", r["src"], r["dst"], r["bytes"], r["status"]) for r in results],
    ["kind", "language", "source_path", "dest_path", "bytes", "status"],
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: pull to laptop
# MAGIC
# MAGIC ```
# MAGIC databricks workspace export-dir <HOME_PATH>/<project_subfolder> ./my-dab/src/ --overwrite
# MAGIC ```
# MAGIC
# MAGIC Then create `databricks.yml` referencing notebooks under `src/`.
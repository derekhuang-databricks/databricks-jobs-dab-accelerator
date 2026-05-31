# Databricks notebook source
# MAGIC %md
# MAGIC # Create `.git` scaffolding
# MAGIC
# MAGIC Given a `job_id`, looks up the job name and creates a `.git/` directory at
# MAGIC `<HOME_PATH>/<manifest_prefix>/<job_name>/.git/` containing the minimum files
# MAGIC that `git init` produces, so exporting the folder locally gives you a working
# MAGIC git repository:
# MAGIC
# MAGIC ```
# MAGIC .git/
# MAGIC   HEAD
# MAGIC   config
# MAGIC   description
# MAGIC   info/exclude
# MAGIC   objects/info/   (empty)
# MAGIC   objects/pack/   (empty)
# MAGIC   refs/heads/     (empty)
# MAGIC   refs/tags/      (empty)
# MAGIC ```

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
HOME_PATH = f"/Workspace/Users/{w.current_user.me().user_name}"

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("manifest_prefix", "migration", "Target Prefix")
dbutils.widgets.text("default_branch", "main", "Initial branch name")
dbutils.widgets.dropdown("overwrite", "true", ["true", "false"], "Overwrite existing files")

MANIFEST_PREFIX = dbutils.widgets.get("manifest_prefix").strip("/")
DEFAULT_BRANCH = dbutils.widgets.get("default_branch").strip() or "main"
OVERWRITE = dbutils.widgets.get("overwrite") == "true"

JOB_ID = int(dbutils.widgets.get("job_id")) if dbutils.widgets.get("job_id") else 0
# Get job name from job ID
job = w.jobs.get(JOB_ID)
job_name = getattr(job.settings, "name", '')

# Strip "[dev <user>] [dev] " prefix from job_name segment in SOURCE_PATH
_short = w.current_user.me().user_name.split("@")[0].replace(".", "_")
DEV_PREFIX = f"[dev {_short}] [dev] "
job_name = job_name.replace(DEV_PREFIX, "")

TARGET_ROOT = f"{HOME_PATH}/{MANIFEST_PREFIX}/{job_name}"
GIT_ROOT = f"{TARGET_ROOT}/.git"
print(f"job_id        = {JOB_ID}")
print(f"job_name      = {job_name}")
print(f"target_root   = {TARGET_ROOT}")
print(f"git_root      = {GIT_ROOT}")
print(f"default_branch= {DEFAULT_BRANCH}")

# COMMAND ----------

from databricks.sdk.service.workspace import ImportFormat

print(f"Workspace: {w.config.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `.git/` file contents

# COMMAND ----------

HEAD = f"ref: refs/heads/{DEFAULT_BRANCH}\n"

CONFIG = """\
[core]
\trepositoryformatversion = 0
\tfilemode = true
\tbare = false
\tlogallrefupdates = true
"""

DESCRIPTION = "Unnamed repository; edit this file 'description' to name the repository.\n"

INFO_EXCLUDE = """\
# git ls-files --others --exclude-from=.git/info/exclude
# Lines that start with '#' are comments.
# For a project mostly in C, the following would be a good set of
# exclude patterns (uncomment them if you want to use them):
# *.[oa]
# *~
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write `.git/` tree

# COMMAND ----------

for d in [
    GIT_ROOT,
    f"{GIT_ROOT}/info",
    f"{GIT_ROOT}/objects/info",
    f"{GIT_ROOT}/objects/pack",
    f"{GIT_ROOT}/refs/heads",
    f"{GIT_ROOT}/refs/tags",
]:
    w.workspace.mkdirs(d)

files = [
    (f"{GIT_ROOT}/HEAD", HEAD),
    (f"{GIT_ROOT}/config", CONFIG),
    (f"{GIT_ROOT}/description", DESCRIPTION),
    (f"{GIT_ROOT}/info/exclude", INFO_EXCLUDE),
]

results = []
for path, body in files:
    try:
        w.workspace.upload(
            path=path,
            content=body.encode(),
            format=ImportFormat.AUTO,
            overwrite=OVERWRITE,
        )
        results.append({"path": path, "bytes": len(body), "status": "ok"})
    except Exception as e:
        results.append({"path": path, "bytes": 0, "status": f"error: {e}"})

for r in results:
    print(f"  {r['status']:6s}  {r['bytes']:5d}  {r['path']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Summary

# COMMAND ----------

print(f".git scaffolding written at: {GIT_ROOT}")
print("")
print("Next steps:")
print(f"  1. databricks workspace export-dir {TARGET_ROOT} ./{job_name} --overwrite")
print(f"  2. cd ./{job_name}")
print("  3. git status   # already an initialized repo, ready to add/commit")

display(spark.createDataFrame(
    [(r["path"], r["bytes"], r["status"]) for r in results],
    ["path", "bytes", "status"],
))
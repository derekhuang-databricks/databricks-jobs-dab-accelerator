# Databricks Jobs Declarative Automation Bundle (DAB) Accelerator

Tooling for migrating an existing Databricks job — and everything it transitively
references — into a deployable [Declarative Automation Bundle](https://docs.databricks.com/dev-tools/bundles/index.html)
under a git repository.

Given a `job_id`, this workflow:

1. **Inventories** the job's full lineage (tasks, notebooks, workspace files, pipelines and their library notebooks, child jobs) into a single JSON manifest.
2. **Extracts** every referenced asset back into the workspace under a project folder.
3. **Scaffolds a bundle** — generates `databricks.yml`, `resources/<job>.job.yml`, `resources/<pipeline>.pipeline.yml`, and `resources/<dashboard>.dashboard.yml`, with all asset paths rewritten to `src/...`.
4. **Adds `.git/`** so the folder is a working git repository once exported.

The end result is a bundle directory that can be pulled to a laptop, committed, and deployed with `databricks bundle deploy`.

## Repository layout

```
databricks.yml                # bundle definition for this accelerator
resources/
  migration.job.yml           # the four-task workflow that runs the migration
src/
  1_inventory_workspace_assets.py
  2_extract_from_manifest.py
  3_scaffold_bundle.py
  4_create_git_scaffold.py
```

## Prerequisites

- Databricks CLI v0.230+ configured against the target workspace (`databricks auth login`).
- Permissions to read the source job and its referenced notebooks, files, and pipelines.
- A serverless-capable workspace (the migration job runs the four notebooks; cluster config can be added if needed).

## Deploy

```bash
git clone https://github.com/derekhuang-databricks/databricks-jobs-dab-accelerator.git
cd databricks-jobs-dab-accelerator
databricks bundle deploy --target dev
```

## Run

After deploy, trigger the `Migration Workflow` job either from the UI or via:

```bash
databricks bundle run migration_workflow \
  --params job_id=<SOURCE_JOB_ID>,target_folder_prefix=migration
```

Parameters:

| Name                   | Default      | Purpose                                                          |
| ---------------------- | ------------ | ---------------------------------------------------------------- |
| `job_id`               | _(required)_ | The job to migrate.                                              |
| `target_folder_prefix` | `migration`  | Prefix under `/Workspace/Users/<you>/` where output is written.  |

Output lands at:

```
/Workspace/Users/<you>/<target_folder_prefix>/<job_name>/
  dab_inventory.json
  <bundle_name>/
    databricks.yml
    resources/...
    src/...
  .git/
```

Pull to your laptop with:

```bash
databricks workspace export-dir \
  /Workspace/Users/<you>/<target_folder_prefix>/<job_name>/<bundle_name> \
  ./<bundle_name> --overwrite
```

## Limitations

- Dashboard YAML is emitted as a TODO stub — fill in `file_path`, `warehouse_id`, `dataset_catalog`, and `dataset_schema` before deploying.

## Support

This is a community-built accelerator, not an officially supported Databricks product. Use at your own risk; pull requests are welcome.

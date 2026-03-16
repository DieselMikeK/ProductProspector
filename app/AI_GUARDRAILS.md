# AI Guardrails — ProductProspector

Read this file before starting any autonomous or multi-step task in this project.
These rules exist because AI models have caused real data loss in this project when
left to run without checkpoints.

---

## 1. Time Limits and Self-Auditing

- **Never run a process longer than 10 minutes without pausing to report status.**
- **Self-audit every 5 minutes during any long-running task.** Ask yourself:
  - Am I writing to the right file?
  - Am I about to overwrite data that already has good results?
  - Is this run scoped correctly (right family, right vendors, right flags)?
- If anything looks wrong during a self-audit, **stop and report before continuing.**

---

## 2. Before Running Any Stage 4 Validation (validate_end_to_end_runtime.py)

- **Always use `--merge-existing`** when writing to VendorEndToEndValidationSummary.csv,
  VendorEndToEndValidationDetails.csv, or VendorEndToEndValidation.json.
  Running without this flag will wipe all existing results for every vendor not in
  the current run.
- **Check the existing summary before running.** If a family already has vendors marked
  `validated`, confirm with the user before re-running that family.
- **Never re-run a vendor that already has 3/3 scrape success and validated status**
  unless the user explicitly asks for it.
- **Commit current state before running Stage 4** if there are uncommitted changes to
  discovery files.

---

## 3. Before Modifying VendorResolverProfiles.csv or .json

- These files are the result of days of manual and semi-automated discovery work.
  A bad write here is not recoverable without re-doing the work.
- Always sync CSV and JSON together — never update one without the other.
- Never bulk-update a field across all vendors without showing the user the exact
  change and getting confirmation first.

---

## 4. File Safety Rules

- **Never delete or overwrite a file without explicit user confirmation.**
- **Never run `rm -rf` on anything outside of `build/`, `dist/`, or `__pycache__/`.**
- **Never truncate a CSV or JSON result file** — always append or merge.
- If two processes might write to the same file, run them sequentially, not in parallel.
- Temp files go in `app/required/mappings/discovery/` with a `tmp_` prefix and must
  be cleaned up after use.

---

## 5. Root Directory Rules

See `app/BUNDLING_RULES.md` for full bundling policy. Summary:
- Root must contain only: `ProductProspector.app`, `app/`, and `README.md`.
- No `build/`, `dist/`, `images/`, `_internal/`, `.spec`, or temp files at root.
- Clean root before any bundle or handoff action.

---

## 6. Git Rules

- **Commit before starting any long autonomous task** that writes to discovery or
  validation files. This creates a recovery point.
- **Push after every commit** — local-only commits provide no protection if the
  machine turns off or a file is overwritten.
- Never force push. Never amend pushed commits.
- Commit messages must describe: what was done, current state, and next step.

---

## 7. Scope Discipline

- **Work one family at a time.** Complete a family fully (run → review → fix → re-run)
  before moving to the next.
- **Do not re-run families or vendors that are already validated** unless fixing a
  specific known issue with that vendor.
- When fixing a broken vendor, target only that vendor — do not run the full family
  unless the user asks.
- Do not run background tasks in parallel if both tasks write to the same output files.

---

## 8. When Something Goes Wrong

- **Stop immediately. Do not try to fix it silently.**
- Report exactly what happened, what file was affected, and what the last known
  good state was.
- Do not start a new run to try to "fix" a bad run — this compounds the damage.
- Ask the user how to proceed before doing anything else.

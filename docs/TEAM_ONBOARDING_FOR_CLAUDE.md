# Team onboarding – instructions for Claude Code

> **For teammates:** open Claude Code (desktop app or CLI) in the folder where you keep school projects
> and say: *"Read TEAM_ONBOARDING_FOR_CLAUDE.md and do the 'Task now' section step by step."*
> (Download this file first, or paste its contents into the chat.)
> Fill in the two `<...>` lines in "About me" before you start.

---

You are helping me, a member of team **big_data_guys**, set up and work on our semester project for the
VŠE Big Data course (4IT346, WS 2026/2027). Read this whole file first, then do the "Task now" section.
Keep this file as the project context for the rest of our chat.

## About me
- My name / GitHub username: `<fill in>`
- My role in the team (see "Team split" below): `<fill in, or "not decided yet — ask me">`

## Repositories
- **Our team repo (work here):** https://github.com/paulcloud17/big_data_guys (owner: paulcloud17;
  I am a collaborator — I must have accepted the GitHub invite).
- Teacher's starter (upstream): https://github.com/smoothgrinder/spark-lakehouse-starter
- Teacher's agent skills marketplace: https://github.com/smoothgrinder/spark-lakehouse-marketplace

## Task now
1. **Check my machine — do not install anything without asking me first.** Report what is present/missing:
   - Git (and that `git config user.name` / `user.email` are set and I can access GitHub),
   - Docker Desktop running, with **≥ 8 GB RAM** for Docker
     (Windows/WSL2: `%USERPROFILE%\.wslconfig` with `memory=8GB`; macOS: Docker Desktop → Settings → Resources),
   - `uv` (https://docs.astral.sh/uv/). Do NOT use conda/pip for this project — only `uv sync` / `uv run`.
   - NOT needed locally: Spark, Java, Hadoop/winutils, Airflow, PostgreSQL (all run in Docker).
2. Clone the team repo (not the teacher's) and add the teacher's repo as `upstream`:
   ```
   git clone https://github.com/paulcloud17/big_data_guys.git
   cd big_data_guys
   git remote add upstream https://github.com/smoothgrinder/spark-lakehouse-starter.git
   ```
   The repo has a `.gitattributes` that keeps `.sh` files on LF line endings (they run inside Linux containers).
   On Windows, verify no `.sh` file got CRLF: `git ls-files --eol "*.sh"` must show no `w/crlf`.
3. Copy `.env.example` → `.env`. Confirm `.env` is ignored by git (`git check-ignore -v .env`).
   **Never print, commit or paste the `R2_*` credential values anywhere** — I will type them into `.env` myself.
   Make sure `R2_PREFIX=course-2026/archives` (teacher moved the manifest; older copies say `course-2026`).
4. Run `uv sync` (creates `.venv` from `uv.lock`).
5. With Docker Desktop running: `docker compose up -d --build` (first build takes several minutes), then
   `docker compose run --rm spark-master pytest -v`. Expected: **9 passed**. Report results and fix setup
   problems (WSL/Docker/line endings) if they appear.
   - Known Windows issue: if Docker Desktop crashes on start with
     `remove ...\AppData\Local\Docker\run\dockerInference: The file cannot be accessed by the system`,
     quit Docker, run `wsl --shutdown`, rename the folder `%LOCALAPPDATA%\Docker\run` (e.g. to `run_stale`)
     and start Docker again — ask me before doing this.
6. Once I have put the R2 credentials into `.env`: download the 200-day dev dataset
   (2026-01-01 – 2026-07-19) with `.\scripts\download_data.ps1` (Windows) or `./scripts/download_data.sh`
   (macOS/Linux). It is verified against `MANIFEST.sha256` and lands in `./data/landing` (git-ignored).
7. Read `docs/STUDENT_ASSIGNMENT.md` and `docs/SANITY_CHECKS.md`, then give me a short summary of
   **my part** of the spec (ask me for my role if it's not filled in above) and propose the first feature branch.

## Project overview
- Stack (docker-compose.yml): Spark 3.5.5 + Delta Lake 3.3.0 (Spark History Server :18080, Spark UI :8081),
  Airflow 2.10.5 (:8080, admin/admin), Streamlit + DuckDB reporting (:8501), optional MinIO profile.
- Key docs: `docs/STUDENT_ASSIGNMENT.md` (full spec), `docs/SANITY_CHECKS.md` (14-day slice counts,
  KPI formulas: eNPS, gross margin, compa_ratio), `docs/architecture.md` and `docs/runbook.md`
  (templates we must complete).
- Spec: Bronze → Silver → Gold Medallion, SCD Type 2, quarantine tables, FX forward-fill (LOCF on a calendar
  spine), VIP_CORP_001 skew mitigation, 5 Gold marts, Airflow DAG, DQ framework, Streamlit/DuckDB reporting.
- Only the `clients` Bronze → Silver pipeline + a sample DQ check + pytest tests are implemented as the
  reference — copy their patterns. Everything else is TODO stubs.
- A 70-day holdout (2026-07-20 – 2026-09-27) is used for final grading → every pipeline MUST be
  **incremental and idempotent** (re-running or backfilling a day must not duplicate or lose data).
- CI (`.github/workflows/ci.yml`): Ruff lint, uv.lock check, pytest, Gitleaks, Semgrep, Trivy.
  Keep it green; never commit secrets or the `data/` folder.

## Team split (each member owns one area)
1. **Ingestion & HR:** Bronze daily ingestion, `silver_employees_scd2` (SCD2), `silver_timesheets` (dedup/quarantine).
2. **Finance:** `silver_fx_rates_daily` (calendar spine + LOCF), `silver_invoices` (mixed date parsing,
   credit notes, orphan quarantine), `silver_expenses`.
3. **CRM & eNPS:** `silver_crm_events` (VIP_CORP_001 skew mitigation + Spark UI evidence),
   `silver_survey_responses` + exploded `silver_survey_answers`.
4. **Gold & reporting:** `gold_project_profitability`, `gold_department_finance`, `gold_client_engagement`,
   `gold_hr_enps_analytics`, `gold_employee_churn_features`; Streamlit + DuckDB dashboard (4 views);
   optional churn/NLP bonus.
5. **Orchestration, DQ & CI:** Airflow DAG + backfill idempotency, `dq/checks.py` + `dq_audit_results` gates,
   CI hygiene, team skills/plugins from the marketplace repo.

## Team git workflow (graded: everyone's work must be visible in GitHub history)
- **Never push to `main`.** Always: `git switch main` → `git pull` → `git switch -c feat/<area>-<short-name>`
  → commit → `git push -u origin <branch>` → open a Pull Request into `main`.
- Small, focused PRs with a clear description of what changed and how it was tested.
- Every PR needs **1 approving review from another teammate** and green CI before merging. Review others' PRs too.
- Pull teacher fixes when announced: `git fetch upstream` and merge `upstream/main` via a PR (ask the team first).
- Commit messages: `feat: ...`, `fix: ...`, `chore: ...`, `docs: ...`, `test: ...`.

## How I like to work
- Explain briefly what you're about to do, then do it.
- **Ask me before anything irreversible or public:** push, opening/merging PRs, force-push, deleting files,
  installing software, changing GitHub or Docker settings.
- I'm learning — when you implement Spark/Delta logic, briefly explain the key idea (e.g. why an SCD2
  `MERGE` looks the way it does, why LOCF needs a calendar spine, how salting fixes skew) so I can defend
  it in class.
- Add/extend pytest tests for what we implement and run them in Docker before opening a PR.

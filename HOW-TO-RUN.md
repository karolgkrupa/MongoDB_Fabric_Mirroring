# How to Run the Mirroring App

This guide covers running the MongoDB -> Microsoft Fabric mirroring app with
[uv](https://docs.astral.sh/uv/) for dependency management, including running it
locally and as an auto-starting Windows service.

> For the one-time Fabric MirrorDB (Landing Zone) setup and the Azure App Service
> / Terraform deployment options, see the main [README.md](README.md). This
> document focuses on the local and Windows-service workflows.

## Prerequisites

1. **Install uv.** uv manages the Python version and all dependencies for you,
   which makes future updates a single command.

   - Windows (PowerShell):

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

   - macOS / Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

   The required Python version (3.14) is pinned in `.python-version` and is
   downloaded automatically by uv the first time you run `uv sync` — you do not
   need to install Python yourself.

2. **Create your `.env` file.** Copy `.env_example` to `.env` (in the repo root)
   and fill in the values (`MONGO_CONN_STR`, `MONGO_DB_NAME`, `MONGO_COLLECTION`,
   `LZ_URL`, `APP_ID`, `SECRET`, `TENANT_ID`, batch sizes, etc.). See the
   "Pre-requisites for Step2" section of the [README.md](README.md) for how to
   obtain each value.

## Install dependencies

From the repo root:

```bash
uv sync
```

This creates a `.venv/` virtual environment from the locked, reproducible set of
dependencies in `uv.lock`.

## Run locally

You can run the app in either of two ways:

- Headless (no web server) — the same entry point used by the Windows service:

```bash
uv run python service_runner.py
```

- With the Flask health endpoint (useful for the Azure App Service deployment):

```bash
uv run python app.py
```

Both start a listener and an initial-sync worker per collection and then keep
running, replicating changes from MongoDB Atlas to the Fabric MirrorDB. Logs are
written to the rotating `mirroring.log` file in the repo root and to stdout.

## Run as a Windows service (auto-start on boot)

The app can run as a Windows service via [WinSW](https://github.com/winsw/winsw),
so it starts automatically on reboot, restarts on failure, and writes logs you
can read easily. The service runs `service_runner.py` using the uv-managed
virtual environment interpreter.

### Install

1. Make sure you have completed the prerequisites above and created your `.env`
   file in the repo root.
2. Open **PowerShell as Administrator**, change into the `windows-service`
   folder, and run the installer:

```powershell
cd windows-service
Set-ExecutionPolicy -Scope Process Bypass
.\install.ps1
```

   The installer will:
   - install uv if it is missing,
   - run `uv sync` to build the `.venv`,
   - download the WinSW wrapper (`mongodb-fabric-mirroring.exe`) next to the
     service config, and
   - install and start the **MongoDB to Fabric Mirroring** service, set to start
     automatically on boot.

### Where the logs are

- Service stdout/stderr/wrapper logs (rolling): `windows-service\logs\`
- Application log (rotating): `mirroring.log` in the repo root
- Service lifecycle events: Windows **Event Viewer** -> Windows Logs ->
  Application

### Manage the service

You can use the standard `services.msc` UI, or the WinSW executable directly from
the `windows-service` folder:

```powershell
.\mongodb-fabric-mirroring.exe status   mongodb-fabric-mirroring.xml
.\mongodb-fabric-mirroring.exe stop      mongodb-fabric-mirroring.xml
.\mongodb-fabric-mirroring.exe restart   mongodb-fabric-mirroring.xml
```

### Uninstall

From an elevated PowerShell prompt in the `windows-service` folder:

```powershell
.\uninstall.ps1
```

This stops and removes the service. It leaves your `.venv`, logs, and `.env`
untouched.

> **Note on restarts:** As described in the README's "Best Practices and
> Troubleshooting", restartability is guaranteed once the `_resume_token` file
> exists (i.e. after initial sync completes). If the process is stopped before
> initial sync finishes, follow the README guidance to clear the collection
> folder before restarting.

## Updating dependencies and the Python version

Because the project uses uv, updates are straightforward:

- **Bump all dependencies to the latest allowed versions** and refresh the lock:

```bash
uv lock --upgrade
uv sync
```

- **Add or change a dependency:** edit the `dependencies` list in
  `pyproject.toml`, then run `uv lock` and `uv sync`.

- **Change the Python version:** edit `.python-version` (and `requires-python` in
  `pyproject.toml`), then run `uv sync`.

- **Regenerate `requirements.txt`** (kept for the pip-based CI / Azure App Service
  path) after any dependency change:

```bash
uv export --no-hashes --no-dev --no-emit-project -o requirements.txt
```

After updating the dependencies on a machine running the Windows service, restart
the service to pick up the changes (`.\mongodb-fabric-mirroring.exe restart
mongodb-fabric-mirroring.xml`).

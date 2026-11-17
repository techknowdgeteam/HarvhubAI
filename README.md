# HarvHub AI — desktop manager and Python services

This release adds manager-controlled execution modes and integrated service dashboards.

## Execution controls

- The application header has a global **Execution Type** selector (`Loop` or `Once`) used by **Run All**.
- Each runnable service page has its own **Execution type** selector before **Play**. A per-service selection can override the global default.
- The default loop interval is 5 seconds. The manager passes both the selected mode and interval to the service when starting it.
- Selection changes are saved in `.manager/execution_config.json` and apply on the next start; a running process is not silently restarted.

`MySQL Remote` is an infrastructure listener. When **Run All** is set to `Once`, the manager keeps MySQL Remote in listener mode while the other services complete one pass, then stops the temporary listener if this Run All action started it. Starting MySQL Remote by itself in `Once` mode performs one browser/session readiness attempt and exits.

## Integrated MySQL dashboard

The MySQL Remote page displays live listener state, remote portal session state, allocated/in-use/idle channels, free channel slots, and the bridge's channel-owner list. These figures are queried from the bridge's channel registry. **Show Console** switches to the original diagnostic output; **Hide Console** returns to the dashboard.

## Integrated manual broker login

Manual login requests from `brokerdetails.py` are sent to the manager over a loopback-only framed JSON socket. They are displayed inside the Broker Details page under **Accounts** and **Programme** tabs. Login IDs and passwords have individual Copy actions, passwords are masked on screen, and the page provides terminal activation plus Valid/Invalid credential decisions. No separate broker-login window is created, and pending credentials are held in memory rather than written to a plaintext file.

## Project layout

- `harvcore_manager.py` — desktop manager
- `Build_HarvHubProgram.pyw` / `Build_HarvHubProgram.vbs` / `HarvHubAI.vbs` — Windows bootstrap and build launcher
- `payload/` — Python modules deployed into the installed runtime
- `release_manifest.json` — source manifest

## Build

Run `HarvHubAI.vbs` on Windows. The bootstrap downloads/validates the selected release, then `Build_HarvHubProgram.pyw` builds and installs the manager. The builder expects the `payload/` directory and `payload/requirements-runtime.txt` to be present.

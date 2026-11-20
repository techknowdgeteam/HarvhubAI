HARVHUB AI — SETUP PROGRESS AND MISSING-SOURCE REPAIR
====================================================

FILES IN THIS UPDATE
--------------------
1. harvcore_manager.py
2. Build_HarvHubProgram.pyw
3. HarvHubAI.vbs
4. Build_HarvHubProgram.vbs
5. signals_trading.py
6. harvcore_console_supervisor.py

WHAT THIS UPDATE CHANGES
------------------------
- The manager now reports each setup operation, file preflight, SHA-256 check,
  copy, backup, verification, executable install and shortcut operation.
- The setup watchdog reports the exact active operation and the last known
  details when no verified progress is recorded. It warns after 30 seconds,
  continues reporting, and requests cancellation after 10 minutes without
  verified progress. Child setup processes are terminated when cancellation is
  acknowledged. If a Windows filesystem call itself does not return, the
  diagnostic remains visible and the user is told to restart after copying it.
- Silent child processes report their PID, elapsed time and last console output.
- The builder reports payload staging, hashing, copying, build output and
  installation progress. A silent build process is reported and cancelled if
  it produces no verified progress for 10 minutes.
- The bootstrap reports archive validation/extraction and checks every required
  Python source. If a required source is absent from the downloaded archive, it
  attempts to download that individual file from the repository's raw GitHub
  branch. A failed fallback is reported with the exact file and URL.
- harvcore_console_supervisor.py is included in the required payload, embedded
  in the built application, installed into the Python runtime directory, and
  verified automatically. No manual file-existence check is required.
- The builder's generated requirements fallback is now validated correctly;
  a non-empty requirements file is accepted even when it is shorter than a
  Python source module.
- Signals Trading remains named signals_trading.py, and the manager navigation
  uses the Signals Trading tab/key. Existing saved execution preferences under
  the former key are migrated.

HOW TO APPLY
------------
1. Extract this ZIP into the folder containing the HarvHubAI.vbs launcher.
2. Allow the files with matching names to overwrite the old copies.
3. Keep all six supplied source/launcher files together in that same folder.
4. Start HarvHubAI.vbs. The launcher downloads the latest project, applies the
   local manager/builder/Signals Trading/supervisor overrides, stages the full
   payload, rebuilds HarvHubAI.exe and starts the application.

The ZIP is an update bundle, not the complete HarvHub AI repository. The VBS
bootstrap downloads the remaining project modules from the configured GitHub
repository. If GitHub does not provide a required module, the bootstrap reports
which source failed and the individual fallback download result.

SETUP REPORT LOCATION
---------------------
The manager appends its setup console to:
    <HarvHub AI installation root>\logs\setup.hclog

The default root in the supplied manager is:
    C:\xampp\htdocs\harvhubAI

The setup window also has a COPY SETUP CONSOLE action. When a watchdog warning
or error appears, copy the complete report; it now identifies the exact file,
operation, process ID (when applicable), elapsed time and last known output.

VALIDATION
----------
The supplied Python files passed Python compilation checks. Payload staging was
also tested with all 12 required payload entries, including the fallback
requirements file. The two VBS launchers are identical. Windows execution of
the VBS/PowerShell installer was not available in this environment, so final
installer behavior must be confirmed on the Windows VPS.

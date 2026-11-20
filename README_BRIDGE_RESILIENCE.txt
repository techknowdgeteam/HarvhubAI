HARVHUB AI — BRIDGE RESILIENCE AND CONSOLE SUPERVISION PATCH
===========================================================

Files in this ZIP
-----------------
- programme_training.py
- programmesignals_trading.py
- signals_provision_challenge.py
- mysqlremote.py
- harvcore_manager.py
- harvcore_console_supervisor.py  (new console/process supervisor)
- brokercharts.py and brokerdetails.py (supplied working versions, unchanged)

What caused the reported message
--------------------------------
The supplied logs show that TCP requests reached mysqlremote and received a
well-formed outer JSON reply. The failure was inside the remote query page:
its JavaScript tried to parse an HTML response as JSON and raised
"Unexpected token '<'". The three workers then handled the resulting error
inconsistently: Programme Training and Signals Provision returned exit code 2;
Programme Signals Trading converted some bridge failures into empty results.

The TCP listener being open does not prove the PHP query endpoint or the remote
MySQL operation is healthy. Broker Charts and Broker Details can therefore keep
working while a heavier query or another browser window encounters a transient
host/PHP response failure.

Changes
-------
1. Programme Training's BridgeClient retries safe SELECT/SHOW/DESCRIBE reads,
   renews the browser channel after transient HTML/JSON errors, and continues
   its scan loop after bridge exceptions.
2. Signals Provision Challenge now catches per-scan failures, recreates its
   client, and remains alive in Loop mode instead of exiting with code 2.
3. Programme Signals Trading now has stale-channel renewal and safe read retries,
   checks the actual SELECT query path before job discovery, and keeps its loop
   running. Its MT5 terminal path now matches HarvHub AI's configured root:
   C:\xampp\htdocs\harvhubAI\mt5.
4. mysqlremote marks the known upstream HTML/JSON parse failure as transient in
   its response so clients can recognise it.
5. harvcore_console_supervisor.py streams the service's console output into the
   manager's existing per-program console and restarts the child after an
   unexpected exit in Loop mode, using capped backoff. The manager also has a
   fallback restart if the supervisor itself exits unexpectedly.

Safety behaviour
----------------
- SELECT/SHOW/DESCRIBE/EXPLAIN/WITH reads are safe to retry and will keep checking
  until the bridge/query endpoint recovers.
- A write whose response is lost or malformed is NOT blindly replayed. It may
  already have reached MySQL; replaying an INSERT or order-related write could
  duplicate effects. The worker logs the uncertain outcome, resets its channel,
  and continues its next loop. Existing state checks/upserts remain responsible
  for reconciliation.
- A console line containing a transient DB error does not automatically kill a
  live process. The workers now wait/retry internally. The supervisor restarts
  a worker that actually exits, avoiding interruption of an in-flight write.
- Manual Stop / Stop All still terminates the supervisor and its child process
  tree; manual stops are not auto-restarted.

Deployment
----------
1. Stop all HarvHub services from the manager.
2. Back up the current Python service folder.
3. Extract these files into the same folder that currently contains mysqlremote.py,
   programme_training.py, and the other runnable services (normally the HarvHub
   AI Python deployment directory). Overwrite the matching files.
4. Start MySQL Remote, then run the three repaired programme services in Loop mode.
   Run All also launches them through harvcore_console_supervisor.py.
5. Confirm each console shows a [SUPERVISOR] STARTED line and then the worker's
   own log lines.
6. If you launch a frozen HarvHubAI.exe rather than the Python source manager,
   the executable must be rebuilt/replaced with the updated harvcore_manager.py
   so its supervisor launch integration is included. Copying the .py file alone
   does not change code already frozen inside an executable.

Validation performed in this package
------------------------------------
- All seven supplied Python scripts plus the new supervisor were syntax-compiled.
- Source-level checks confirmed that the manager deploy list includes the new
  supervisor file and uses it for service command construction.
- This environment cannot access the user's Windows Chrome/Selenium session,
  VPS, hosting provider, or live MySQL database, so a live end-to-end SQL test
  was not possible here.

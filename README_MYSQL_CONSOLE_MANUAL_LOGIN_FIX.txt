HARVHUB AI — MYSQL METRICS, CONSOLE RESPONSIVENESS & MANUAL LOGIN FIX

WHAT THIS UPDATE FIXES

1. MySQL metrics event format
   The manager's MySQL status poller sends a dictionary, but the old UI event
   handler treated it as a tuple and evaluated data[0]. That raised KeyError: 0
   every two seconds. The handler now accepts both dictionaries and legacy
   one-item tuples/lists. Manager event errors are no longer misfiled as
   mysqlremote program output.

2. Broker Details Show Console
   The Broker Details dashboard and console were both packed as expanding
   siblings inside a page that has no outer vertical scrollbar. This could
   place the console below the visible area. Show Console now swaps the
   dashboard placeholder for that program's console; HIDE CONSOLE restores the
   manual-login dashboard. Other program pages use the same predictable swap.

3. Console replay performance
   Opening a console with a large retained log previously inserted thousands
   of lines through individual Tk calls. It now groups adjacent lines and
   inserts them in batches. The UI event pump also limits work per Tk tick so
   buttons remain responsive during busy service output.

4. Manual-login request visibility
   Broker Details now retries idempotent manual-login add/remove IPC messages
   briefly if the manager listener is not yet ready. It logs an acknowledgement
   when a request is added to the manager attention list, or an actionable
   UI-IPC error when the manager does not acknowledge it. No passwords are
   included in those diagnostic messages.

FILES INCLUDED

The ZIP contains the desktop manager, all six runnable service scripts,
shared support modules, runtime requirements, and build/launch scripts.
The existing project source files were used as the base; only the manager and
Broker Details sources were changed for this specific issue.

INSTALLATION

1. Back up C:\xampp\htdocs\harvhubAI and the current build/source folder.
2. Extract the ZIP, retaining the included filenames.
3. Replace the matching files in your HarvHub source project with the files
   from the ZIP. The files are placed in the top-level folder named
   HarvHubAI_Manager_Metrics_Console_ManualLogin_Fix.
4. Run the normal HarvHubAI.vbs build/launch script to rebuild and deploy the
   desktop EXE. Updating .py files alone does not update an already-built EXE.
5. Start HarvHub AI, open Broker Details, and check the manual-login dashboard.
   When an account probe detects an unlogged-in or unavailable terminal, the
   service should report [MANUAL][UI] acknowledgement and the dashboard should
   show that account. Use Show Console to switch to the actual Broker Details
   output and Hide Console to return to the dashboard.

INTERPRETING THE LOG PROVIDED

The pasted MySQL Remote output shows that ChromeDriver initialized, the bridge
listened on 127.0.0.1:5599, and bridge_status requests completed successfully.
The reported KeyError came from the desktop manager's metrics event handler,
not from the database listener itself. After this fix, the manager should be
able to update the MySQL metrics dashboard without producing repeated
[MANAGER][EVENT ERROR] lines.

VALIDATION / LIMITATIONS

The Python sources are syntax-compiled and the ZIP is integrity-tested. A live
Windows Tk GUI, the local TCP control listener, real database requests, Chrome,
and MT5 logins cannot be exercised in this Linux build environment; verify
those integrations on the target Windows machine.

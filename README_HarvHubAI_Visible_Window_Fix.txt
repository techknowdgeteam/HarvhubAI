HARVHUB AI — VISIBLE STARTUP / BOOTSTRAP / BUILDER FIX

WHAT THIS UPDATE CHANGES

1. After the Windows elevation prompt is accepted, HarvHub opens a visible
   process-cleanup CMD window. It reports the older HarvHub processes it finds
   and stops, and it displays cleanup errors instead of hiding them.

2. The GitHub bootstrap always starts through a visible CMD host and opens the
   existing HarvHub Windows Forms installer. The PowerShell host is no longer
   started with WindowStyle Hidden. PowerShell startup and parse errors remain
   visible; the CMD window pauses on failure so the output cannot disappear.

3. The builder is launched through the matching python.exe rather than
   pythonw.exe. The normal Tk build window remains the main UI, while a visible
   console exposes startup messages and Python tracebacks. The console pauses
   if the builder exits with an error.

4. Both launcher entry points are identical and include the same visible
   startup path: HarvHubAI.vbs and Build_HarvHubProgram.vbs.

5. The single-instance cleanup also recognises older visible bootstrap/build
   CMD wrappers, so a new click can stop a previous HarvHub setup session.

INSTALLATION

1. Close any existing HarvHub installer/build windows.
2. Replace the existing HarvHubAI.vbs and Build_HarvHubProgram.vbs with the
   two files in this package.
3. Launch HarvHubAI.vbs normally and approve the Windows elevation prompt.
4. After approval, a visible cleanup console should appear first, followed by
   the visible installer window and its diagnostic console. When setup reaches
   the build handoff, the HarvHub build window and its diagnostic console open.

IMPORTANT

This update intentionally uses visible console windows. They are part of the
startup diagnostics and should remain visible. If a stage fails, its console
will pause and show the actual error instead of silently exiting.

VALIDATION

The two VBS files were checked for matching content, balanced Function/Sub
blocks, and the visible startup handoffs. Native Windows Script Host, CMD,
PowerShell WinForms, and Python GUI execution cannot be run in this Linux
build environment, so verify the actual windows on the target Windows VPS.

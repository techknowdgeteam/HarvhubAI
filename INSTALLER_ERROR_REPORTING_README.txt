HarvHub AI installer error-reporting update

Files included (use these exact filenames):
- HarvHubAI.vbs
- Build_HarvHubProgram.vbs

What this update changes:
1. Any non-zero exit from the PowerShell bootstrap is saved and displayed in a persistent CMD window.
2. The same complete report opens in Notepad, where it can be copied with Ctrl+A, Ctrl+C.
3. The visible Windows Forms installer adds a Copy Error Details button when an error occurs.
4. PowerShell failures include the exception message, error category, fully qualified error ID, script line/position, script stack, full ErrorRecord and exception details.
5. Every visible bootstrap progress/log message is also written to a durable log file.
6. Builder startup failures, missing source/Python markers, cleanup failures, bootstrap-script creation failures and PowerShell launch failures use the same copyable report window instead of relying on an uncopyable message box.

Copy-friendly error report location:
%TEMP%\HarvHubAI_last_error.txt

Supporting bootstrap files/logs:
%LOCALAPPDATA%\HarvHubAI\Bootstrap\visible_bootstrap.ps1
%LOCALAPPDATA%\HarvHubAI\Bootstrap\visible_bootstrap.log
%LOCALAPPDATA%\HarvHubAI\Bootstrap\bootstrap_error.txt
%LOCALAPPDATA%\HarvHubAI\Bootstrap\builder_output.log

Install:
Replace both launcher files with the files in this package. Run HarvHubAI.vbs again.

Important:
This update makes the full underlying error visible and copyable. It does not guess at the cause of the existing exit code 1. The new report will expose the exact failing operation and location so that the actual cause can be corrected.

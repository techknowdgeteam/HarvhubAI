HARVHUB AI - POWERHELL BOOTSTRAP SYNTAX FIX

FIX
---
The generated visible_bootstrap.ps1 was missing the Find-CompatiblePython
function's root/versions declarations and outer root loop. The function's
closing brace was consequently parsed as an unexpected '}' (reported around
line 256).

The two launcher VBS files have been corrected by restoring:
  - $roots = @((Join-Path $env:LOCALAPPDATA 'Programs\Python'), $env:ProgramFiles)
  - $versions = @('Python313','Python312','Python311','Python310','Python314')
  - foreach ($base in $roots) { ... }
  - if (!$base) { continue }

No download, timeout, ZIP extraction, process cleanup, Python installation,
or build logic was otherwise changed in this repair.

INSTALLATION
------------
1. Close any currently open HarvHub installer/bootstrap windows.
2. Replace the existing HarvHubAI.vbs with the supplied HarvHubAI.vbs.
3. Replace the existing Build_HarvHubProgram.vbs with the supplied
   Build_HarvHubProgram.vbs.
4. Start HarvHubAI.vbs again.

VALIDATION
----------
- Confirmed all 26 embedded PowerShell chunks can be extracted from the VBS.
- Confirmed the generated PowerShell delimiter structure is balanced.
- Confirmed both VBS launchers are byte-identical.
- The PowerShell parser and native Windows GUI cannot be executed in this
  Linux environment; verify the installer on the target Windows machine.

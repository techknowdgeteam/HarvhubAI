HarvHub AI — PowerShell bootstrap syntax repair

Files included (use these exact filenames):
- HarvHubAI.vbs
- Build_HarvHubProgram.vbs

Root cause
The generated visible_bootstrap.ps1 was missing the final branch, loop closure,
UI refresh, and function closure for Set-UiStage. The next generated chunk began
function Download-ToFile before Set-UiStage had been closed. Windows PowerShell
therefore stopped immediately with MissingEndCurlyBrace at the reported lines
145–153, before GitHub download or installation could begin.

What is fixed
1. Restored the missing Set-UiStage branch and closing braces in the generated
   PowerShell source.
2. Added a mandatory PowerShell Parser preflight in HarvHubAI.vbs. Every launch
   now parses the exact generated visible_bootstrap.ps1 before executing it.
3. If parsing fails, setup does not run. The launcher opens the persistent,
   copyable diagnostic report with the parser's exact error messages and exits
   with a distinct validation code.
4. Kept HarvHubAI.vbs and Build_HarvHubProgram.vbs byte-identical.

Install
1. Extract this ZIP.
2. Replace the existing HarvHubAI.vbs and Build_HarvHubProgram.vbs with the
   files in this package.
3. Run HarvHubAI.vbs.

The generated PowerShell file is recreated on each explicit launcher run at:
%LOCALAPPDATA%\HarvHubAI\Bootstrap\visible_bootstrap.ps1

The launcher's built-in parser check uses the Windows PowerShell parser before
any bootstrap operation executes. The repaired Set-UiStage block was also
checked in the reconstructed generated PowerShell source for balanced block
and delimiter closure in the build environment.

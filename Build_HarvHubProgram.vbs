Option Explicit

Dim fso, sh, shellApp, processEnv, localAppData, installRoot, cacheRoot
Dim psScriptPath, markerPath, pythonMarkerPath, psFile, psExit, projectRoot
Dim pythonwPath, builderScript, adminResult, appRunning, clickedVbs
Dim psProcess, psStdOut, psStdErr, diagnosticText
Dim validatorProcess, validationCommand, validationStdOut, validationStdErr, validationExit, validationOutput
Dim builderLogPath, builderExit, builderCommand, builderLaunchError

Set fso = CreateObject("Scripting.FileSystemObject")
Set sh = CreateObject("WScript.Shell")
Set shellApp = CreateObject("Shell.Application")

localAppData = sh.ExpandEnvironmentStrings("%LOCALAPPDATA%")
installRoot = "C:\xampp\htdocs\harvhubAI"
cacheRoot = fso.BuildPath(localAppData, "HarvHubAI\Bootstrap")
psScriptPath = fso.BuildPath(cacheRoot, "visible_bootstrap.ps1")
markerPath = fso.BuildPath(cacheRoot, "project_root.txt")
pythonMarkerPath = fso.BuildPath(cacheRoot, "pythonw_path.txt")
builderLogPath = fso.BuildPath(cacheRoot, "builder_output.log")
clickedVbs = WScript.ScriptFullName

' Elevate before downloading so the visible installer is not restarted midway.
If Not IsAdministrator() Then
    On Error Resume Next
    shellApp.ShellExecute "wscript.exe", """" & WScript.ScriptFullName & """ /elevated", fso.GetParentFolderName(WScript.ScriptFullName), "runas", 1
    If Err.Number <> 0 Then
        diagnosticText = "Could not request administrator permission." & vbCrLf & _
            "VBScript error " & Err.Number & ": " & Err.Description
        Err.Clear
        Call ShowDiagnosticWindow("HarvHub AI elevation request failed", diagnosticText)
        WScript.Quit 22
    End If
    On Error GoTo 0
    WScript.Quit 0
End If

' Enforce one active HarvHub setup/update session. Older HarvHub manager,
' bootstrap, builder, and service processes are stopped before files are touched.
' Unrelated Windows applications and system processes are never targeted.
On Error Resume Next
Call EnsureFolderTree(cacheRoot)
If Err.Number <> 0 Then
    diagnosticText = "Could not create the bootstrap folder: " & cacheRoot & vbCrLf & _
        "VBScript error " & Err.Number & ": " & Err.Description
    Err.Clear
    Call ShowDiagnosticWindow("HarvHub AI bootstrap folder creation failed", diagnosticText)
    WScript.Quit 14
End If
Call StopExistingHarvHubProcesses()
If Err.Number <> 0 Then
    diagnosticText = "The HarvHub process-cleanup stage failed before it could finish." & vbCrLf & _
        "VBScript error " & Err.Number & ": " & Err.Description
    Err.Clear
    Call ShowDiagnosticWindow("HarvHub AI process cleanup failed", diagnosticText)
    WScript.Quit 15
End If
On Error GoTo 0

' Open the Windows Forms installer visibly, but run its PowerShell host hidden.
' The installer itself remains a normal visible window. There is no CMD window to
' leave behind after the GUI opens. WshShell.Exec captures startup/parser errors.
Set processEnv = sh.Environment("PROCESS")
processEnv("HARVHUBAI_CLICKED_VBS") = clickedVbs

On Error Resume Next
Set psFile = fso.CreateTextFile(psScriptPath, True, False)
If Err.Number <> 0 Then
    diagnosticText = "Could not create the generated PowerShell bootstrap: " & psScriptPath & vbCrLf & _
        "VBScript error " & Err.Number & ": " & Err.Description
    Err.Clear
    Call ShowDiagnosticWindow("HarvHub AI bootstrap script could not be created", diagnosticText)
    WScript.Quit 16
End If
psFile.Write BootstrapPowerShell()
If Err.Number <> 0 Then
    diagnosticText = "Could not write the generated PowerShell bootstrap: " & psScriptPath & vbCrLf & _
        "VBScript error " & Err.Number & ": " & Err.Description
    Err.Clear
    psFile.Close
    Call ShowDiagnosticWindow("HarvHub AI bootstrap script could not be written", diagnosticText)
    WScript.Quit 17
End If
psFile.Close
If Err.Number <> 0 Then
    diagnosticText = "Could not close the generated PowerShell bootstrap file: " & psScriptPath & vbCrLf & _
        "VBScript error " & Err.Number & ": " & Err.Description
    Err.Clear
    Call ShowDiagnosticWindow("HarvHub AI bootstrap script could not be closed", diagnosticText)
    WScript.Quit 18
End If

' Parse the exact generated PowerShell file before running it. This catches
' missing braces, unterminated strings, and other parser errors at the launcher
' boundary, with a copyable diagnostic rather than a partially started setup.
processEnv("HARVHUBAI_BOOTSTRAP_PS_SCRIPT") = psScriptPath
validationCommand = "$p=$env:HARVHUBAI_BOOTSTRAP_PS_SCRIPT; $t=$null; $e=$null; [void][System.Management.Automation.Language.Parser]::ParseFile($p,[ref]$t,[ref]$e); if($e.Count -gt 0){$e | ForEach-Object { $_.ToString() }; exit 1}; Write-Output 'PowerShell syntax validation passed.'"
On Error Resume Next
Set validatorProcess = sh.Exec("powershell.exe -NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -Command """ & validationCommand & """")
If Err.Number <> 0 Then
    diagnosticText = "Could not start the PowerShell syntax validator." & vbCrLf & _
        "Generated script: " & psScriptPath & vbCrLf & _
        "VBScript error " & Err.Number & ": " & Err.Description
    Err.Clear
    Call ShowDiagnosticWindow("HarvHub AI bootstrap syntax validation could not start", diagnosticText)
    WScript.Quit 20
End If
On Error GoTo 0
Do While validatorProcess.Status = 0
    WScript.Sleep 100
Loop
validationExit = validatorProcess.ExitCode
validationStdOut = ""
validationStdErr = ""
On Error Resume Next
validationStdOut = validatorProcess.StdOut.ReadAll
validationStdErr = validatorProcess.StdErr.ReadAll
On Error GoTo 0
validationOutput = Trim(validationStdErr & vbCrLf & validationStdOut)
If validationExit <> 0 Then
    If Len(validationOutput) = 0 Then validationOutput = "PowerShell parser rejected the generated bootstrap without returning details."
    Call ShowDiagnosticWindow("HarvHub AI generated PowerShell contains a syntax error", _
        "The bootstrap was NOT executed because its generated PowerShell failed parser validation." & vbCrLf & _
        "Validator exit code: " & validationExit & vbCrLf & _
        "Generated script: " & psScriptPath & vbCrLf & vbCrLf & validationOutput)
    WScript.Quit 21
End If

Set psProcess = sh.Exec("powershell.exe -NoLogo -NoProfile -STA -WindowStyle Hidden -ExecutionPolicy Bypass -File """ & psScriptPath & """")
If Err.Number <> 0 Then
    diagnosticText = "Could not launch the PowerShell bootstrap." & vbCrLf & _
        "Command: powershell.exe -NoLogo -NoProfile -STA -WindowStyle Hidden -ExecutionPolicy Bypass -File " & psScriptPath & vbCrLf & _
        "VBScript error " & Err.Number & ": " & Err.Description
    Err.Clear
    Call ShowDiagnosticWindow("HarvHub AI PowerShell process could not start", diagnosticText)
    WScript.Quit 19
End If
On Error GoTo 0
Do While psProcess.Status = 0
    WScript.Sleep 250
Loop
psExit = psProcess.ExitCode

psStdOut = ""
psStdErr = ""
On Error Resume Next
psStdOut = psProcess.StdOut.ReadAll
psStdErr = psProcess.StdErr.ReadAll
On Error GoTo 0
If psExit <> 0 Then
    diagnosticText = Trim(psStdErr & vbCrLf & psStdOut)
    If Len(diagnosticText) = 0 Then diagnosticText = ReadAllText(fso.BuildPath(cacheRoot, "bootstrap_error.txt"))
    If Len(Trim(diagnosticText)) = 0 Then diagnosticText = "PowerShell exited without returning captured output. See the bootstrap log and generated script paths below."
    diagnosticText = diagnosticText & vbCrLf & vbCrLf & "----- COMPLETE VISIBLE BOOTSTRAP LOG -----" & vbCrLf & _
        ReadAllText(fso.BuildPath(cacheRoot, "visible_bootstrap.log"))
    Call ShowDiagnosticWindow("HarvHub AI installer failed", "Installer PowerShell exit code: " & psExit & vbCrLf & _
        "PowerShell script: " & psScriptPath & vbCrLf & _
        "Bootstrap error file: " & fso.BuildPath(cacheRoot, "bootstrap_error.txt") & vbCrLf & vbCrLf & diagnosticText)
    WScript.Quit psExit
End If

projectRoot = Trim(ReadAllText(markerPath))
pythonwPath = Trim(ReadAllText(pythonMarkerPath))
If Len(projectRoot) = 0 Or Not fso.FolderExists(projectRoot) Then
    Call ShowDiagnosticWindow("HarvHub AI source validation failed", _
        "The downloaded HarvHub AI source folder could not be found." & vbCrLf & _
        "Expected project marker: " & markerPath & vbCrLf & _
        "Marker contents: " & projectRoot & vbCrLf & _
        "Bootstrap error: " & ReadAllText(fso.BuildPath(cacheRoot, "bootstrap_error.txt")))
    WScript.Quit 10
End If
If Len(pythonwPath) = 0 Or Not fso.FileExists(pythonwPath) Then
    Call ShowDiagnosticWindow("HarvHub AI Python runtime missing", _
        "The Python GUI runtime could not be found after bootstrap." & vbCrLf & _
        "Python marker: " & pythonMarkerPath & vbCrLf & _
        "Marker contents: " & pythonwPath & vbCrLf & _
        "Bootstrap error: " & ReadAllText(fso.BuildPath(cacheRoot, "bootstrap_error.txt")))
    WScript.Quit 11
End If

builderScript = fso.BuildPath(projectRoot, "Build_HarvHubProgram.pyw")
If Not fso.FileExists(builderScript) Then
    Call ShowDiagnosticWindow("HarvHub AI builder source missing", _
        "The downloaded GitHub project is missing Build_HarvHubProgram.pyw." & vbCrLf & _
        "Expected builder: " & builderScript & vbCrLf & _
        "Project root: " & projectRoot)
    WScript.Quit 12
End If

Call EnsureFolderTree(installRoot)
processEnv("HARVHUBAI_SOURCE_ROOT") = projectRoot
processEnv("HARVHUBAI_ROOT") = installRoot
processEnv("HARVHUBAI_BOOTSTRAPPED") = "1"
processEnv("HARVHUBAI_GITHUB_BOOTSTRAPPED") = "1"
processEnv("HARVHUB_BUILDER_LOG") = builderLogPath

' Launch the builder with pythonw.exe directly. It creates its own visible,
' normal Tk window and captures build output in its in-window console/log file.
' No cmd.exe or PowerShell wrapper is created during a normal launch.
builderCommand = Chr(34) & pythonwPath & Chr(34) & " " & Chr(34) & builderScript & Chr(34) & " --install --force-rebuild"
' Run the GUI-subsystem Python process directly. WindowStyle 0 prevents a
' console host from being shown; the Tk builder window is independently visible.
builderLaunchError = ""
On Error Resume Next
builderExit = sh.Run(builderCommand, 0, True)
If Err.Number <> 0 Then
    builderLaunchError = Err.Description
    Err.Clear
    builderExit = 1
End If
On Error GoTo 0
If builderExit <> 0 Then
    diagnosticText = ReadAllTextUtf8(builderLogPath)
    If Len(Trim(diagnosticText)) = 0 Then diagnosticText = builderLaunchError
    If Len(Trim(diagnosticText)) = 0 Then diagnosticText = "The GUI builder exited without writing console output. Check that Python can start the builder and that the project sources are accessible."
    Call ShowDiagnosticWindow("HarvHub AI build failed", _
        "Builder process exit code: " & builderExit & vbCrLf & _
        "Python runtime: " & pythonwPath & vbCrLf & _
        "Builder script: " & builderScript & vbCrLf & _
        "Builder log: " & builderLogPath & vbCrLf & vbCrLf & diagnosticText)
    WScript.Quit builderExit
End If
WScript.Quit 0

Function BootstrapPowerShell()
    BootstrapPowerShell = BootstrapPartA() & vbCrLf & BootstrapPartB()
End Function

Function BootstrapPartA()
    BootstrapPartA = Join(Array( _
        PSChunk01(), _
        PSChunk02(), _
        PSChunk03(), _
        PSChunk04(), _
        PSChunk05(), _
        PSChunk06(), _
        PSChunk07(), _
        PSChunk08(), _
        PSChunk09(), _
        PSChunk10(), _
        PSChunk11(), _
        PSChunk12(), _
        PSChunk13() _
    ), vbCrLf)
End Function

Function BootstrapPartB()
    BootstrapPartB = Join(Array( _
        PSChunk14(), _
        PSChunk15(), _
        PSChunk16(), _
        PSChunk17(), _
        PSChunk18(), _
        PSChunk19(), _
        PSChunk20(), _
        PSChunk21(), _
        PSChunk22(), _
        PSChunk23(), _
        PSChunk24(), _
        PSChunk25(), _
        PSChunk26() _
    ), vbCrLf)
End Function

Function PSChunk01()
    PSChunk01 = Join(Array( _
        "$ErrorActionPreference = 'Stop'", _
        "$ProgressPreference = 'SilentlyContinue'", _
        "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12", _
        "Add-Type -AssemblyName System.Windows.Forms", _
        "Add-Type -AssemblyName System.Drawing", _
        "[System.Windows.Forms.Application]::EnableVisualStyles()", _
        "$script:cacheRoot = Join-Path $env:LOCALAPPDATA 'HarvHubAI\Bootstrap'", _
        "$script:zipPath = Join-Path $script:cacheRoot ('HarvHubAI-' + $PID + '-' + [Guid]::NewGuid().ToString('N') + '.zip')", _
        "$script:extractRoot = Join-Path $script:cacheRoot 'source'", _
        "$script:markerPath = Join-Path $script:cacheRoot 'project_root.txt'", _
        "$script:pythonMarkerPath = Join-Path $script:cacheRoot 'pythonw_path.txt'", _
        "$script:errorPath = Join-Path $script:cacheRoot 'bootstrap_error.txt'", _
        "$script:logPath = Join-Path $script:cacheRoot 'visible_bootstrap.log'", _
        "New-Item -ItemType Directory -Path $script:cacheRoot -Force | Out-Null", _
        "[System.IO.File]::WriteAllText($script:logPath, '', [System.Text.Encoding]::Unicode)", _
        "$form = New-Object System.Windows.Forms.Form" _
    ), vbCrLf)
End Function

Function PSChunk02()
    PSChunk02 = Join(Array( _
        "$form.Text = 'HarvHub AI - Installer'", _
        "$form.StartPosition = 'CenterScreen'", _
        "$form.ClientSize = [System.Drawing.Size]::new(760, 560)", _
        "$form.MinimumSize = [System.Drawing.Size]::new(760, 560)", _
        "$form.FormBorderStyle = 'FixedDialog'", _
        "$form.MaximizeBox = $false", _
        "$form.MinimizeBox = $false", _
        "$form.ControlBox = $false", _
        "$form.ShowInTaskbar = $true", _
        "$form.TopMost = $false", _
        "$form.BackColor = [System.Drawing.Color]::FromArgb(250,247,239)", _
        "$form.ForeColor = [System.Drawing.Color]::FromArgb(48,39,24)", _
        "$form.Font = New-Object System.Drawing.Font('Segoe UI', 9)", _
        "$header = New-Object System.Windows.Forms.Panel", _
        "$header.Location = New-Object System.Drawing.Point(18, 16)" _
    ), vbCrLf)
End Function

Function PSChunk03()
    PSChunk03 = Join(Array( _
        "$header.Size = New-Object System.Drawing.Size(724, 86)", _
        "$header.BackColor = [System.Drawing.Color]::FromArgb(255,253,248)", _
        "$header.BorderStyle = 'FixedSingle'", _
        "$form.Controls.Add($header)", _
        "$logo = New-Object System.Windows.Forms.Label", _
        "$logo.Text = 'H'", _
        "$logo.Location = New-Object System.Drawing.Point(16, 15)", _
        "$logo.Size = New-Object System.Drawing.Size(52, 52)", _
        "$logo.TextAlign = 'MiddleCenter'", _
        "$logo.BackColor = [System.Drawing.Color]::FromArgb(184,135,32)", _
        "$logo.ForeColor = [System.Drawing.Color]::White", _
        "$logo.Font = New-Object System.Drawing.Font('Segoe UI', 21, [System.Drawing.FontStyle]::Bold)", _
        "$header.Controls.Add($logo)", _
        "$title = New-Object System.Windows.Forms.Label" _
    ), vbCrLf)
End Function

Function PSChunk04()
    PSChunk04 = Join(Array( _
        "$title.Text = 'HARVHUB AI'", _
        "$title.Location = New-Object System.Drawing.Point(82, 17)", _
        "$title.Size = New-Object System.Drawing.Size(430, 30)", _
        "$title.Font = New-Object System.Drawing.Font('Segoe UI', 16, [System.Drawing.FontStyle]::Bold)", _
        "$title.ForeColor = [System.Drawing.Color]::FromArgb(48,39,24)", _
        "$header.Controls.Add($title)", _
        "$subtitle = New-Object System.Windows.Forms.Label", _
        "$subtitle.Text = 'AUTOMATIC VPS INSTALLATION'", _
        "$subtitle.Location = New-Object System.Drawing.Point(84, 49)", _
        "$subtitle.Size = New-Object System.Drawing.Size(450, 20)", _
        "$subtitle.Font = New-Object System.Drawing.Font('Segoe UI', 8, [System.Drawing.FontStyle]::Bold)", _
        "$subtitle.ForeColor = [System.Drawing.Color]::FromArgb(130,116,93)", _
        "$header.Controls.Add($subtitle)", _
        "$card = New-Object System.Windows.Forms.Panel" _
    ), vbCrLf)
End Function

Function PSChunk05()
    PSChunk05 = Join(Array( _
        "$card.Location = New-Object System.Drawing.Point(18, 114)", _
        "$card.Size = New-Object System.Drawing.Size(724, 428)", _
        "$card.BackColor = [System.Drawing.Color]::FromArgb(255,253,248)", _
        "$card.BorderStyle = 'FixedSingle'", _
        "$form.Controls.Add($card)", _
        "$heading = New-Object System.Windows.Forms.Label", _
        "$heading.Text = 'Preparing HarvHub AI'", _
        "$heading.Location = New-Object System.Drawing.Point(22, 18)", _
        "$heading.Size = New-Object System.Drawing.Size(650, 32)", _
        "$heading.Font = New-Object System.Drawing.Font('Segoe UI', 17, [System.Drawing.FontStyle]::Bold)", _
        "$card.Controls.Add($heading)", _
        "$description = New-Object System.Windows.Forms.Label", _
        "$description.Text = 'Downloading and validating the latest installer files before Python setup begins.'", _
        "$description.Location = New-Object System.Drawing.Point(24, 56)" _
    ), vbCrLf)
End Function

Function PSChunk06()
    PSChunk06 = Join(Array( _
        "$description.Size = New-Object System.Drawing.Size(660, 28)", _
        "$description.ForeColor = [System.Drawing.Color]::FromArgb(130,116,93)", _
        "$card.Controls.Add($description)", _
        "$script:statusLabel = New-Object System.Windows.Forms.Label", _
        "$script:statusLabel.Text = 'Opening the HarvHub AI installer...'", _
        "$script:statusLabel.Location = New-Object System.Drawing.Point(22, 98)", _
        "$script:statusLabel.Size = New-Object System.Drawing.Size(676, 38)", _
        "$script:statusLabel.BackColor = [System.Drawing.Color]::FromArgb(247,240,223)", _
        "$script:statusLabel.Padding = [System.Windows.Forms.Padding]::new(12, 9, 8, 4)", _
        "$script:statusLabel.Font = New-Object System.Drawing.Font('Segoe UI', 9, [System.Drawing.FontStyle]::Bold)", _
        "$card.Controls.Add($script:statusLabel)", _
        "$script:progress = New-Object System.Windows.Forms.ProgressBar", _
        "$script:progress.Location = New-Object System.Drawing.Point(24, 146)", _
        "$script:progress.Size = New-Object System.Drawing.Size(674, 20)" _
    ), vbCrLf)
End Function

Function PSChunk07()
    PSChunk07 = Join(Array( _
        "$script:progress.Minimum = 0", _
        "$script:progress.Maximum = 100", _
        "$script:progress.Value = 0", _
        "$script:progress.Style = 'Blocks'", _
        "$card.Controls.Add($script:progress)", _
        "$script:stepLabels = @()", _
        "$stepNames = @('Download latest GitHub release', 'Extract and validate HarvHub AI files', 'Prepare compatible 64-bit Python', 'Open the HarvHub AI build window')", _
        "$stepY = 184", _
        "foreach ($stepName in $stepNames) {", _
        "    $step = New-Object System.Windows.Forms.Label", _
        "    $step.Text = 'o  ' + $stepName", _
        "    $step.Location = New-Object System.Drawing.Point(26, $stepY)", _
        "    $step.Size = New-Object System.Drawing.Size(660, 22)", _
        "    $step.ForeColor = [System.Drawing.Color]::FromArgb(130,116,93)" _
    ), vbCrLf)
End Function

Function PSChunk08()
    PSChunk08 = Join(Array( _
        "    $script:stepLabels += $step", _
        "    $card.Controls.Add($step)", _
        "    $stepY += 27", _
        "}", _
        "$script:logBox = New-Object System.Windows.Forms.RichTextBox", _
        "$script:logBox.Location = New-Object System.Drawing.Point(24, 300)", _
        "$script:logBox.Size = New-Object System.Drawing.Size(674, 86)", _
        "$script:logBox.ReadOnly = $true", _
        "$script:logBox.BorderStyle = 'FixedSingle'", _
        "$script:logBox.BackColor = [System.Drawing.Color]::FromArgb(247,240,223)", _
        "$script:logBox.ForeColor = [System.Drawing.Color]::FromArgb(48,39,24)", _
        "$script:logBox.Font = New-Object System.Drawing.Font('Consolas', 8)", _
        "$card.Controls.Add($script:logBox)", _
        "$script:copyButton = New-Object System.Windows.Forms.Button", _
        "$script:copyButton.Text = 'Copy Error Details'", _
        "$script:copyButton.Location = New-Object System.Drawing.Point(440, 390)", _
        "$script:copyButton.Size = New-Object System.Drawing.Size(138, 28)", _
        "$script:copyButton.Visible = $false", _
        "$script:copyButton.Add_Click({ try { [System.Windows.Forms.Clipboard]::SetText($script:logBox.Text); $script:statusLabel.Text = 'Installer details copied to clipboard.' } catch { [System.Windows.Forms.MessageBox]::Show(('Could not copy details: ' + $_.Exception.Message), 'HarvHub AI') } })", _
        "$card.Controls.Add($script:copyButton)", _
        "$script:closeButton = New-Object System.Windows.Forms.Button" _
    ), vbCrLf)
End Function

Function PSChunk09()
    PSChunk09 = Join(Array( _
        "$script:closeButton.Text = 'Close'", _
        "$script:closeButton.Location = New-Object System.Drawing.Point(590, 390)", _
        "$script:closeButton.Size = New-Object System.Drawing.Size(108, 28)", _
        "$script:closeButton.Visible = $false", _
        "$script:closeButton.Add_Click({ $form.Close() })", _
        "$card.Controls.Add($script:closeButton)", _
        "$form.Show()", _
        "$form.Activate()", _
        "$form.BringToFront()", _
        "[System.Windows.Forms.Application]::DoEvents()", _
        "function Pump-Ui {", _
        "    $form.Refresh()", _
        "    [System.Windows.Forms.Application]::DoEvents()", _
        "}", _
        "function Add-UiLog([string]$Message) {", _
        "    $line = '[' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + '] ' + $Message", _
        "    try { [System.IO.File]::AppendAllText($script:logPath, $line + [Environment]::NewLine, [System.Text.Encoding]::Unicode) } catch { }", _
        "    $script:logBox.AppendText($line + [Environment]::NewLine)" _
    ), vbCrLf)
End Function

Function PSChunk10()
    PSChunk10 = Join(Array( _
        "    $script:logBox.SelectionStart = $script:logBox.TextLength", _
        "    $script:logBox.ScrollToCaret()", _
        "    Pump-Ui", _
        "}", _
        "function Set-UiStage([int]$Index, [string]$Message, [int]$Percent) {", _
        "    $script:statusLabel.Text = $Message", _
        "    $script:progress.Style = 'Blocks'", _
        "    $script:progress.Value = [Math]::Max(0, [Math]::Min(100, $Percent))", _
        "    for ($n = 0; $n -lt $script:stepLabels.Count; $n++) {", _
        "        if ($n -lt $Index) {", _
        "            $script:stepLabels[$n].Text = 'OK  ' + $stepNames[$n]", _
        "            $script:stepLabels[$n].ForeColor = [System.Drawing.Color]::FromArgb(36,122,75)", _
        "        } elseif ($n -eq $Index) {", _
        "            $script:stepLabels[$n].Text = '>>  ' + $stepNames[$n]" _
    ), vbCrLf)
End Function

Function PSChunk11()
    PSChunk11 = Join(Array( _
        "            $script:stepLabels[$n].ForeColor = [System.Drawing.Color]::FromArgb(184,135,32)", _
        "        } else {", _
        "            $script:stepLabels[$n].Text = 'o  ' + $stepNames[$n]", _
        "            $script:stepLabels[$n].ForeColor = [System.Drawing.Color]::FromArgb(130,116,93)", _
        "        }", _
        "    }", _
        "    Pump-Ui", _
        "}", _
        "function Download-ToFile([string]$Url, [string]$Destination, [string]$Label, [long]$ResumeOffset = 0) {", _
        "    Add-UiLog(('Downloading ' + $Label + '.'))", _
        "    $request = $null; $response = $null; $inputStream = $null; $outputStream = $null", _
        "    $phase = 'opening a connection to the download server'", _
        "    try {", _
        "        $request = [System.Net.HttpWebRequest]::Create($Url)", _
        "        $request.Method = 'GET'", _
        "        $request.UserAgent = 'HarvHubAI-Installer/3.0'", _
        "        $request.Accept = 'application/zip, application/octet-stream;q=0.9, */*;q=0.8'", _
        "        $request.Timeout = 180000", _
        "        $request.ReadWriteTimeout = 300000", _
        "        $request.KeepAlive = $true", _
        "        $request.ProtocolVersion = [Version]'1.1'", _
        "        $request.ServicePoint.ConnectionLimit = 8", _
        "        $request.ServicePoint.UseNagleAlgorithm = $false", _
        "        $request.AllowAutoRedirect = $true", _
        "        if ($ResumeOffset -gt 0) { $request.AddRange($ResumeOffset); Add-UiLog('Requesting HTTP range resume from byte ' + $ResumeOffset + '.') }", _
        "        $script:progress.Style = 'Blocks'", _
        "        $script:statusLabel.Text = 'Connecting to download server... (180-second response limit)'", _
        "        Pump-Ui" _
    ), vbCrLf)
End Function

Function PSChunk12()
    PSChunk12 = Join(Array( _
        "        $phase = 'waiting for the server response (180-second limit)'", _
        "        $responseTimer = [System.Diagnostics.Stopwatch]::StartNew()", _
        "        $asyncResponse = $request.BeginGetResponse($null, $null)", _
        "        $lastWaitNotice = 0", _
        "        while (!$asyncResponse.IsCompleted) {", _
        "            $waitSeconds = [int]$responseTimer.Elapsed.TotalSeconds", _
        "            if ($waitSeconds -ge 180) { try { $request.Abort() } catch {}; throw ('Timed out after 180 seconds waiting for download response headers. Check internet access, DNS, firewall, proxy settings, and server availability. URL: ' + $Url) }", _
        "            if ($waitSeconds -ge ($lastWaitNotice + 15)) { $lastWaitNotice = $waitSeconds; Add-UiLog('Still waiting for response headers: ' + $waitSeconds + '/180 seconds.'); $script:statusLabel.Text = 'Waiting for download server... ' + $waitSeconds + '/180 seconds' }", _
        "            Pump-Ui", _
        "            Start-Sleep -Milliseconds 100", _
        "        }", _
        "        $response = $request.EndGetResponse($asyncResponse)", _
        "        $appendResponse = $false", _
        "        if ($ResumeOffset -gt 0 -and $response.StatusCode -eq [System.Net.HttpStatusCode]::PartialContent) {", _
        "            $contentRange = [string]$response.Headers['Content-Range']", _
        "            if ($contentRange -notmatch ('^bytes\s+' + [regex]::Escape([string]$ResumeOffset) + '-')) { throw ('The server returned an unexpected Content-Range while resuming at byte ' + $ResumeOffset + ': ' + $contentRange) }", _
        "            $appendResponse = $true", _
        "            Add-UiLog('Server accepted range resume at byte ' + $ResumeOffset + '.')", _
        "        } elseif ($ResumeOffset -gt 0) {", _
        "            Add-UiLog('Server did not honor HTTP range resume (status ' + [int]$response.StatusCode + '). Restarting this route from byte zero safely.')", _
        "            $ResumeOffset = 0", _
        "        }", _
        "        $responseLength = [long]$response.ContentLength", _
        "        $total = if ($responseLength -gt 0) { $responseLength + $ResumeOffset } else { -1 }", _
        "        if ($total -gt 0) { Add-UiLog('Server responded. Total archive size: ' + [Math]::Round($total / 1MB, 2) + ' MB.') } else { Add-UiLog('Server did not provide a download size; progress will be measured using received bytes.') }", _
        "        $phase = 'opening the download response stream'", _
        "        $inputStream = $response.GetResponseStream()", _
        "        if (!$inputStream) { throw 'The download server returned an empty response stream.' }", _
        "        $fileMode = if ($appendResponse) { [System.IO.FileMode]::Append } else { [System.IO.FileMode]::Create }", _
        "        $outputStream = [System.IO.File]::Open($Destination, $fileMode, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)", _
        "        $downloadTimer = [System.Diagnostics.Stopwatch]::StartNew()", _
        "        $speedSampleTimer = [System.Diagnostics.Stopwatch]::StartNew()", _
        "        [long]$received = $ResumeOffset", _
        "        [long]$receivedThisAttempt = 0", _
        "        [long]$speedSampleBytes = 0", _
        "        $lastProgressNotice = 0" _
    ), vbCrLf)
End Function

Function PSChunk13()
    PSChunk13 = Join(Array( _
        "        $phase = 'downloading the archive (20-minute attempt limit; 2-hour route limit; 5-minute idle limit; slow connections are allowed)'", _
        "        $buffer = New-Object byte[] 262144", _
        "        while ($true) {", _
        "            if ($downloadTimer.Elapsed.TotalSeconds -ge 1200) { try { $request.Abort() } catch {}; throw 'This download attempt exceeded the 20-minute limit. Its partial archive is being retained for a resume attempt.' }", _
        "            $readTimer = [System.Diagnostics.Stopwatch]::StartNew()", _
        "            $asyncRead = $inputStream.BeginRead($buffer, 0, $buffer.Length, $null, $null)", _
        "            $lastIdleNotice = 0", _
        "            while (!$asyncRead.IsCompleted) {", _
        "                $idleSeconds = [int]$readTimer.Elapsed.TotalSeconds", _
        "                if ($downloadTimer.Elapsed.TotalSeconds -ge 1200) { try { $request.Abort() } catch {}; throw 'This download attempt exceeded the 20-minute limit. Its partial archive is being retained for a resume attempt.' }", _
        "                if ($idleSeconds -ge 300) { try { $request.Abort() } catch {}; throw ('No archive data arrived for 300 seconds. The connection appears stalled. Partial data received: ' + [Math]::Round($received / 1MB, 2) + ' MB. URL: ' + $Url) }", _
        "                if ($idleSeconds -ge ($lastIdleNotice + 30)) { $lastIdleNotice = $idleSeconds; Add-UiLog('Connected, but no archive data has arrived for ' + $idleSeconds + ' seconds. Partial data received: ' + [Math]::Round($received / 1MB, 2) + ' MB.') }", _
        "                Pump-Ui", _
        "                Start-Sleep -Milliseconds 100", _
        "            }", _
        "            $count = $inputStream.EndRead($asyncRead)", _
        "            if ($count -le 0) { break }", _
        "            $outputStream.Write($buffer, 0, $count)", _
        "            $received += $count", _
        "            $receivedThisAttempt += $count", _
        "            $speedSampleBytes += $count", _
        "            $elapsedSeconds = [Math]::Max(0.1, $downloadTimer.Elapsed.TotalSeconds)", _
        "            $averageKBps = $receivedThisAttempt / $elapsedSeconds / 1KB", _
        "            if ($total -gt 0) {", _
        "                $percent = [int][Math]::Min(100, [Math]::Floor($received * 100.0 / $total))", _
        "                $script:progress.Value = [Math]::Max($script:progress.Value, [Math]::Min(100, $percent))", _
        "                $script:statusLabel.Text = $Label + ' - ' + $percent + '% (' + [Math]::Round($received / 1MB, 2) + ' / ' + [Math]::Round($total / 1MB, 2) + ' MB) | ' + [Math]::Round($averageKBps, 1) + ' KB/s'", _
        "            } else {", _
        "                $estimatedFill = [int][Math]::Min(95, [Math]::Floor(95.0 * $received / ($received + 8MB)))", _
        "                $script:progress.Value = [Math]::Max($script:progress.Value, $estimatedFill)", _
        "                $script:statusLabel.Text = $Label + ' - ' + [Math]::Round($received / 1MB, 2) + ' MB received | ' + [Math]::Round($averageKBps, 1) + ' KB/s'", _
        "            }", _
        "            if ($speedSampleTimer.Elapsed.TotalSeconds -ge 30) {", _
        "                $sampleKBps = $speedSampleBytes / [Math]::Max(1, $speedSampleTimer.Elapsed.TotalSeconds) / 1KB", _
        "                Add-UiLog('Current download speed: ' + [Math]::Round($sampleKBps, 1) + ' KB/s; attempt average: ' + [Math]::Round($averageKBps, 1) + ' KB/s. Slow speeds are permitted; the installer will continue while bytes arrive.')", _
        "                $speedSampleBytes = 0", _
        "                $speedSampleTimer.Restart()", _
        "            }", _
        "            $transferSizeText = ''", _
        "            if ($total -gt 0) { $transferSizeText = ' of ' + [Math]::Round($total / 1MB, 2) + ' MB' }", _
        "            if ([int]$downloadTimer.Elapsed.TotalSeconds -ge ($lastProgressNotice + 15)) { $lastProgressNotice = [int]$downloadTimer.Elapsed.TotalSeconds; Add-UiLog('Archive transfer: ' + [Math]::Round($received / 1MB, 2) + ' MB received' + $transferSizeText + ' at ' + [Math]::Round($averageKBps, 1) + ' KB/s.') }", _
        "            Pump-Ui" _
    ), vbCrLf)
End Function

Function PSChunk14()
    PSChunk14 = Join(Array( _
        "        }", _
        "        $phase = 'finishing and validating the downloaded ZIP file'", _
        "        $outputStream.Flush()", _
        "        $outputStream.Dispose(); $outputStream = $null", _
        "        $inputStream.Dispose(); $inputStream = $null", _
        "        $response.Dispose(); $response = $null", _
        "        if (!(Test-Path -LiteralPath $Destination)) { throw 'The download file was not created.' }", _
        "        $actualSize = (Get-Item -LiteralPath $Destination).Length", _
        "        if ($actualSize -lt 10240) { throw ('The downloaded ZIP is too small or incomplete: ' + $actualSize + ' bytes.') }", _
        "        if ($total -gt 0 -and $received -ne $total) { throw ('The downloaded ZIP is incomplete. Expected ' + $total + ' bytes but the file currently contains ' + $received + ' bytes. The partial file will be resumed.') }", _
        "        $script:progress.Style = 'Blocks'", _
        "        $script:progress.Value = 100", _
        "        Add-UiLog($Label + ' transfer completed: ' + [Math]::Round($actualSize / 1MB, 2) + ' MB. Checking ZIP integrity before accepting it.')" _
    ), vbCrLf)
End Function

Function PSChunk15()
    PSChunk15 = Join(Array( _
        "    } catch {", _
        "        $exception = $_.Exception", _
        "        $detail = $exception.Message", _
        "        $networkDetail = ''", _
        "        if ($exception -is [System.Net.WebException]) { $networkDetail = ' Network status: ' + $exception.Status + '.'; try { if ($exception.Response) { $networkDetail += ' HTTP response: ' + $exception.Response.StatusCode + ' ' + $exception.Response.StatusDescription + '.' } } catch { } }", _
        "        try { if ($outputStream) { $outputStream.Flush(); $outputStream.Dispose(); $outputStream = $null } } catch { }", _
        "        try { if ($inputStream) { $inputStream.Dispose(); $inputStream = $null } } catch { }", _
        "        try { if ($response) { $response.Dispose(); $response = $null } } catch { }", _
        "        try { if ($request) { $request.Abort() } } catch { }", _
        "        $partialSize = 0", _
        "        try { if (Test-Path -LiteralPath $Destination) { $partialSize = (Get-Item -LiteralPath $Destination).Length } } catch { }", _
        "        if ($partialSize -ge 10240) { Add-UiLog('Keeping ' + [Math]::Round($partialSize / 1MB, 2) + ' MB of partial archive data for an HTTP range-resume attempt.') } else { try { Remove-Item -LiteralPath $Destination -Force -ErrorAction SilentlyContinue } catch { } }", _
        "        $failure = $Label + ' download failed while ' + $phase + '. URL: ' + $Url + '. Cause: ' + $detail + $networkDetail", _
        "        Add-UiLog('DOWNLOAD ATTEMPT FAILED: ' + $failure)", _
        "        throw $failure", _
        "    } finally {", _
        "        if ($outputStream) { try { $outputStream.Dispose() } catch { } }", _
        "        if ($inputStream) { try { $inputStream.Dispose() } catch { } }", _
        "        if ($response) { try { $response.Dispose() } catch { } }", _
        "    }", _
        "}", _
        "function Download-ToFileWithRetry([string]$Url, [string]$Destination, [string]$Label, [int]$MaxSeconds = 7200) {", _
        "    $retryTimer = [System.Diagnostics.Stopwatch]::StartNew()", _
        "    $attempt = 0", _
        "    $errors = @()", _
        "    while ($attempt -lt 8 -and $retryTimer.Elapsed.TotalSeconds -lt $MaxSeconds) {", _
        "        $attempt++", _
        "        $resumeOffset = 0", _
        "        if (Test-Path -LiteralPath $Destination) {", _
        "            try { $resumeOffset = [long](Get-Item -LiteralPath $Destination).Length } catch { $resumeOffset = 0 }", _
        "            if ($resumeOffset -lt 10240) { Remove-Item -LiteralPath $Destination -Force -ErrorAction SilentlyContinue; $resumeOffset = 0 }", _
        "        }", _
        "        try {", _
        "            Add-UiLog('Download attempt ' + $attempt + '/8 for ' + $Label + '; elapsed=' + [int]$retryTimer.Elapsed.TotalSeconds + 's.')", _
        "            Download-ToFile $Url $Destination $Label $resumeOffset", _
        "            return", _
        "        } catch {", _
        "            $caught = $_", _
        "            $message = ($caught | Out-String).Trim()", _
        "            $errors += $message", _
        "            if ($message -match 'Content-Range') { Add-UiLog('The server returned an invalid resume range. Discarding this partial file and restarting the download from byte zero.'); Remove-Item -LiteralPath $Destination -Force -ErrorAction SilentlyContinue }", _
        "            $rangeStatus = 0", _
        "            try { if ($caught.Exception.Response) { $rangeStatus = [int]$caught.Exception.Response.StatusCode } } catch { }", _
        "            if ($rangeStatus -eq 416 -and (Test-Path -LiteralPath $Destination) -and (Get-Item -LiteralPath $Destination).Length -ge 10240) { Add-UiLog('The server reports there are no bytes beyond the retained file. Continuing to downstream integrity/size verification.'); return }", _
        "            if ($attempt -lt 8 -and $retryTimer.Elapsed.TotalSeconds -lt $MaxSeconds) {", _
        "                $delay = [Math]::Min(30, 5 * $attempt)", _
        "                Add-UiLog($Label + ' failed: ' + $message + ' Retrying in ' + $delay + ' seconds; usable partial data will be resumed.')", _
        "                for ($wait = 0; $wait -lt $delay; $wait++) { Pump-Ui; Start-Sleep -Seconds 1 }", _
        "            }", _
        "        }", _
        "    }", _
        "    throw ('Download retries/time limit exhausted for ' + $Label + '. ' + ($errors -join ' || '))", _
        "}", _
        "function Test-RepositoryZip([string]$Path) {", _
        "    Add-Type -AssemblyName System.IO.Compression.FileSystem", _
        "    $archive = [System.IO.Compression.ZipFile]::OpenRead($Path)", _
        "    try {", _
        "        $entryCount = $archive.Entries.Count", _
        "        if ($entryCount -lt 5) { throw ('Repository ZIP contains too few entries: ' + $entryCount) }", _
        "        return $entryCount", _
        "    } finally { $archive.Dispose() }", _
        "}", _
        "function Download-GitHubRepository([string]$Destination) {", _
        "    $routes = @(@{ Name = 'GitHub Codeload direct'; Url = 'https://codeload.github.com/techknowdgeteam/HarvhubAI/zip/refs/heads/main' }, @{ Name = 'GitHub archive fallback'; Url = 'https://github.com/techknowdgeteam/HarvhubAI/archive/refs/heads/main.zip' })", _
        "    $routeErrors = @()", _
        "    foreach ($route in $routes) {", _
        "        $routeTimer = [System.Diagnostics.Stopwatch]::StartNew()", _
        "        $attempt = 0", _
        "        while ($attempt -lt 8 -and $routeTimer.Elapsed.TotalSeconds -lt 7200) {", _
        "            $attempt++", _
        "            $resumeOffset = 0", _
        "            if (Test-Path -LiteralPath $Destination) {", _
        "                try { $resumeOffset = [long](Get-Item -LiteralPath $Destination).Length } catch { $resumeOffset = 0 }", _
        "                if ($resumeOffset -lt 10240) { Remove-Item -LiteralPath $Destination -Force -ErrorAction SilentlyContinue; $resumeOffset = 0 }", _
        "            }", _
        "            try {", _
        "                Add-UiLog('Trying download route: ' + $route.Name + ' | attempt ' + $attempt + '/8 | elapsed=' + [int]$routeTimer.Elapsed.TotalSeconds + 's.')", _
        "                if ($resumeOffset -gt 0) { Add-UiLog('Resuming the existing partial archive at byte ' + $resumeOffset + '; no already-received bytes will be intentionally discarded.') }", _
        "                Download-ToFile $route.Url $Destination ($route.Name + ' repository ZIP') $resumeOffset", _
        "                $entryCount = Test-RepositoryZip $Destination", _
        "                Add-UiLog('GitHub repository ZIP passed integrity validation with ' + $entryCount + ' entries.')", _
        "                return", _
        "            } catch {", _
        "                $caught = $_", _
        "                $routeMessage = ($caught | Out-String).Trim()", _
        "                $routeErrors += ($route.Name + ' attempt ' + $attempt + ': ' + $routeMessage)", _
        "                if ($routeMessage -match 'central directory|invalid|not a valid zip|zip archive') { Add-UiLog('The downloaded bytes did not yet form a valid ZIP. Keeping the partial file so the next attempt can request the remaining byte range.') }", _
        "                if ($routeMessage -match 'Content-Range') { Add-UiLog('The server returned an invalid resume range. Restarting this route from byte zero.'); Remove-Item -LiteralPath $Destination -Force -ErrorAction SilentlyContinue }", _
        "                $rangeStatus = 0", _
        "                try { if ($caught.Exception.Response) { $rangeStatus = [int]$caught.Exception.Response.StatusCode } } catch { }", _
        "                if ($rangeStatus -eq 416 -and (Test-Path -LiteralPath $Destination)) {", _
        "                    try { $entryCount = Test-RepositoryZip $Destination; Add-UiLog('The server rejected a resume because the retained file is already complete; ZIP integrity passed with ' + $entryCount + ' entries.'); return } catch { Add-UiLog('Retained partial archive is not complete; restarting this route from byte zero.'); Remove-Item -LiteralPath $Destination -Force -ErrorAction SilentlyContinue }", _
        "                }", _
        "                Add-UiLog($route.Name + ' attempt ' + $attempt + ' failed: ' + $routeMessage)", _
        "                if ($attempt -lt 8 -and $routeTimer.Elapsed.TotalSeconds -lt 7200) {", _
        "                    $delay = [Math]::Min(30, 5 * $attempt)", _
        "                    Add-UiLog('Retrying the same GitHub route in ' + $delay + ' seconds. Any usable partial archive will be resumed; low bandwidth alone is not a failure.')", _
        "                    for ($wait = 0; $wait -lt $delay; $wait++) { Pump-Ui; Start-Sleep -Seconds 1 }", _
        "                } else {", _
        "                    Add-UiLog($route.Name + ' retry/time limit reached after ' + [int]$routeTimer.Elapsed.TotalSeconds + ' seconds.')", _
        "                }", _
        "            }", _
        "        }", _
        "        if (Test-Path -LiteralPath $Destination) { Remove-Item -LiteralPath $Destination -Force -ErrorAction SilentlyContinue }", _
        "        Add-UiLog('Download route exhausted. Trying the next GitHub route.')", _
        "    }", _
        "    throw ('All GitHub download routes failed after resumable retries. ' + ($routeErrors -join ' || '))", _
        "}", _
        "function Find-CompatiblePython {", _
        "    $roots = @((Join-Path $env:LOCALAPPDATA 'Programs\Python'), $env:ProgramFiles)", _
        "    $versions = @('Python313','Python312','Python311','Python310','Python314')", _
        "    foreach ($base in $roots) {", _
        "        if (!$base) { continue }" _
    ), vbCrLf)
End Function

Function PSChunk16()
    PSChunk16 = Join(Array( _
        "        foreach ($version in $versions) {", _
        "            $folder = if ($version -like 'Python*') { Join-Path $base $version } else { $base }", _
        "            $py = Join-Path $folder 'python.exe'", _
        "            $pyw = Join-Path $folder 'pythonw.exe'", _
        "            if (!(Test-Path -LiteralPath $py) -or !(Test-Path -LiteralPath $pyw)) { continue }", _
        "            try {", _
        "                $output = & $py -c 'import sys,struct,tkinter; print(sys.version_info.major,sys.version_info.minor,struct.calcsize(''P'')*8)' 2>$null", _
        "                if ($LASTEXITCODE -eq 0 -and (($output -join ' ').Trim() -match '^3\s+13\s+64$')) {", _
        "                    return $pyw", _
        "                }", _
        "            } catch { }", _
        "        }", _
        "    }", _
        "    return $null" _
    ), vbCrLf)
End Function

Function PSChunk17()
    PSChunk17 = Join(Array( _
        "}", _
        "function Wait-WithUi($Process, [string]$Message) {", _
        "    $script:progress.Style = 'Blocks'", _
        "    $script:statusLabel.Text = $Message", _
        "    $watch = [System.Diagnostics.Stopwatch]::StartNew()", _
        "    $lastReport = -15", _
        "    $lastCpuSeconds = 0.0", _
        "    $lastWorkingSet = 0", _
        "    Add-UiLog('[START] ' + $Message + ' PID=' + $Process.Id + '; maximum wait=1200 seconds.')", _
        "    while (!$Process.HasExited) {", _
        "        Pump-Ui", _
        "        Start-Sleep -Milliseconds 250", _
        "        $Process.Refresh()", _
        "        if ($watch.Elapsed.TotalSeconds -ge ($lastReport + 15)) {", _
        "            $cpuSeconds = 0.0", _
        "            $workingSet = 0", _
        "            try { $cpuSeconds = $Process.TotalProcessorTime.TotalSeconds; $workingSet = $Process.WorkingSet64 } catch { }", _
        "            $cpuDelta = [math]::Round(($cpuSeconds - $lastCpuSeconds), 2)", _
        "            Add-UiLog('[HEARTBEAT] ' + $Message + ' still running; PID=' + $Process.Id + '; elapsed=' + [int]$watch.Elapsed.TotalSeconds + 's; CPU used in last interval=' + $cpuDelta + 's; working set=' + $workingSet + ' bytes. The installer does not expose its internal progress stream.')", _
        "            $lastCpuSeconds = $cpuSeconds", _
        "            $lastWorkingSet = $workingSet", _
        "            $lastReport = [int]$watch.Elapsed.TotalSeconds", _
        "        }", _
        "        if ($watch.Elapsed.TotalSeconds -ge 1200) {", _
        "            Add-UiLog('[TIMEOUT] ' + $Message + ' exceeded 1200 seconds. PID=' + $Process.Id + '; last CPU=' + $lastCpuSeconds + 's; last working set=' + $lastWorkingSet + ' bytes.')", _
        "            try { Stop-Process -Id $Process.Id -Force -ErrorAction Stop } catch { Add-UiLog('[WARNING] Could not terminate installer PID=' + $Process.Id + ': ' + $_.Exception.Message) }", _
        "            throw ($Message + ' timed out after 1200 seconds. Process PID=' + $Process.Id + '. Last CPU=' + $lastCpuSeconds + 's; working set=' + $lastWorkingSet + ' bytes.')", _
        "        }", _
        "    }", _
        "    Add-UiLog('[EXIT] ' + $Message + ' finished with exit code ' + $Process.ExitCode + ' after ' + [int]$watch.Elapsed.TotalSeconds + ' seconds.')", _
        "    $script:progress.Style = 'Blocks'", _
        "    $script:progress.Value = 100", _
        "    if ($Process.ExitCode -ne 0 -and $Process.ExitCode -ne 3010) {", _
        "        throw ($Message + ' failed with exit code ' + $Process.ExitCode)" _
    ), vbCrLf)
End Function

Function PSChunk18()
    PSChunk18 = Join(Array( _
        "    }", _
        "}", _
        "try {", _
        "    Set-UiStage 0 'Downloading the latest HarvHub AI source from GitHub...' 0", _
        "    Remove-Item -LiteralPath $script:markerPath -Force -ErrorAction SilentlyContinue", _
        "    Remove-Item -LiteralPath $script:pythonMarkerPath -Force -ErrorAction SilentlyContinue", _
        "    Remove-Item -LiteralPath $script:errorPath -Force -ErrorAction SilentlyContinue", _
        "    Remove-Item -LiteralPath $script:zipPath -Force -ErrorAction SilentlyContinue", _
        "    if (Test-Path -LiteralPath $script:extractRoot) {", _
        "        Remove-Item -LiteralPath $script:extractRoot -Recurse -Force", _
        "    }", _
        "    New-Item -ItemType Directory -Path $script:extractRoot -Force | Out-Null", _
        "    Download-GitHubRepository $script:zipPath" _
    ), vbCrLf)
End Function

Function PSChunk19()
    PSChunk19 = Join(Array( _
        "    Set-UiStage 1 'Extracting and validating the downloaded HarvHub AI files...' 0", _
        "    Add-Type -AssemblyName System.IO.Compression.FileSystem", _
        "    Add-UiLog('Validating downloaded GitHub archive: ' + $script:zipPath)", _
        "    $archive = [System.IO.Compression.ZipFile]::OpenRead($script:zipPath)", _
        "    try {", _
        "        Add-UiLog('GitHub archive contains ' + $archive.Entries.Count + ' entries; size=' + (Get-Item -LiteralPath $script:zipPath).Length + ' bytes.')", _
        "        if ($archive.Entries.Count -lt 5) { throw 'GitHub archive contains too few files.' }", _
        "    } finally { $archive.Dispose() }", _
        "    Add-UiLog('Extracting GitHub archive into: ' + $script:extractRoot)", _
        "    [System.IO.Compression.ZipFile]::ExtractToDirectory($script:zipPath, $script:extractRoot)", _
        "    Add-UiLog('GitHub archive extraction completed. Locating the project builder and required runtime files.')", _
        "    Remove-Item -LiteralPath $script:zipPath -Force -ErrorAction SilentlyContinue", _
        "    $builderFile = Get-ChildItem -LiteralPath $script:extractRoot -Recurse -File -Filter 'Build_HarvHubProgram.pyw' | Select-Object -First 1", _
        "    if (!$builderFile) { throw 'GitHub repository is missing Build_HarvHubProgram.pyw.' }", _
        "    $project = $builderFile.Directory.FullName", _
        "    $managerFile = Join-Path $project 'harvcore_manager.py'", _
        "    if (!(Test-Path -LiteralPath $managerFile)) { throw 'GitHub repository is missing harvcore_manager.py beside the builder.' }", _
        "    Add-UiLog('Selected project root: ' + $project)", _
        "    Add-UiLog('Manager source found: ' + $managerFile)", _
        "    $payload = Join-Path $project 'payload'", _
        "    New-Item -ItemType Directory -Path $payload -Force | Out-Null" _
    ), vbCrLf)
End Function

Function PSChunk20()
    PSChunk20 = Join(Array( _
        "    $sourceNames = @('mysqlremote.py','brokerdetails.py','brokercharts.py','programme_training.py','signals_trading.py','signals_provision_challenge.py','ip.py','notification_email.py','notification_service.py','harvhub_ipc.py','harvcore_console_supervisor.py')", _
        "    foreach ($name in $sourceNames) {", _
        "        Add-UiLog('Checking and staging required source: ' + $name)", _
        "        $target = Join-Path $payload $name", _
        "        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($name)", _
        "        $ext = [System.IO.Path]::GetExtension($name)", _
        "        $pattern = '^' + [regex]::Escape($baseName) + '(?:\s*\([^)]*\))?$'", _
        "        $rootSource = Get-ChildItem -LiteralPath $project -File | Where-Object {", _
        "            $_.Extension -ieq $ext -and ($_.BaseName -ieq $baseName -or $_.BaseName -match $pattern)", _
        "        } | Sort-Object @{Expression={ if ($_.Name -ieq $name) { 0 } else { 1 } }}, FullName | Select-Object -First 1", _
        "        if ($rootSource) {", _
        "            Copy-Item -LiteralPath $rootSource.FullName -Destination $target -Force", _
        "        } elseif (!(Test-Path -LiteralPath $target)) {", _
        "            $found = Get-ChildItem -LiteralPath $project -Recurse -File | Where-Object {", _
        "                $_.Extension -ieq $ext -and ($_.BaseName -ieq $baseName -or $_.BaseName -match $pattern) -and" _
    ), vbCrLf)
End Function

Function PSChunk21()
    PSChunk21 = Join(Array( _
        "                $_.FullName -notmatch '\\(build|release|dist|\.build-venv|\.git)\\'", _
        "            } | Sort-Object @{Expression={ if ($_.Name -ieq $name) { 0 } else { 1 } }}, FullName | Select-Object -First 1", _
        "            if (!$found) {", _
        "                $rawUrl = 'https://raw.githubusercontent.com/techknowdgeteam/HarvhubAI/main/' + $name", _
        "                Add-UiLog('Required source was not in the archive extraction. Downloading individual GitHub fallback: ' + $name)", _
        "                Download-ToFileWithRetry $rawUrl $target ('GitHub source fallback: ' + $name) 7200", _
        "            } else {", _
        "                Add-UiLog('Found nested source file: ' + $found.FullName + '; copying into payload.')", _
        "                Copy-Item -LiteralPath $found.FullName -Destination $target -Force", _
        "            }", _
        "        }", _
        "        if (!(Test-Path -LiteralPath $target) -or (Get-Item -LiteralPath $target).Length -lt 100) {", _
        "            throw ('Python source is missing or incomplete: ' + $name)", _
        "        }", _
        "        Add-UiLog('Validated source: ' + $name + ' (' + (Get-Item -LiteralPath $target).Length + ' bytes)')", _
        "    }", _
        "    $requirements = Join-Path $payload 'requirements-runtime.txt'", _
        "    if (!(Test-Path -LiteralPath $requirements)) {", _
        "        $requirementsText = @('# HarvHub AI runtime requirements','MetaTrader5','pytz','tzdata','requests','urllib3','certifi','selenium','beautifulsoup4','webdriver-manager') -join [Environment]::NewLine", _
        "        [System.IO.File]::WriteAllText($requirements, $requirementsText, [System.Text.Encoding]::ASCII)", _
        "    }" _
    ), vbCrLf)
End Function

Function PSChunk22()
    PSChunk22 = Join(Array( _
        "    $clickedVbs = $env:HARVHUBAI_CLICKED_VBS", _
        "    if (!$clickedVbs -or !(Test-Path -LiteralPath $clickedVbs)) { throw 'The standalone HarvHubAI.vbs file is not accessible.' }", _
        "    Copy-Item -LiteralPath $clickedVbs -Destination (Join-Path $project 'HarvHubAI.vbs') -Force", _
        "    Copy-Item -LiteralPath $clickedVbs -Destination (Join-Path $project 'Build_HarvHubProgram.vbs') -Force", _
        "    $localProjectFolder = Split-Path -Parent $clickedVbs", _
        "    foreach ($overrideName in @('Build_HarvHubProgram.pyw','harvcore_manager.py','signals_trading.py','harvcore_console_supervisor.py')) {", _
        "        $localOverride = Join-Path $localProjectFolder $overrideName", _
        "        if (Test-Path -LiteralPath $localOverride -PathType Leaf) {", _
        "            Copy-Item -LiteralPath $localOverride -Destination (Join-Path $project $overrideName) -Force", _
        "            Add-UiLog ('Applied local source override: ' + $overrideName)", _
        "        }", _
        "    }", _
        "    foreach ($required in @('Build_HarvHubProgram.pyw','harvcore_manager.py','HarvHubAI.vbs','Build_HarvHubProgram.vbs')) {", _
        "        if (!(Test-Path -LiteralPath (Join-Path $project $required))) { throw ('Required installer file is missing: ' + $required) }", _
        "    }", _
        "    [System.IO.File]::WriteAllText($script:markerPath, $project, [System.Text.Encoding]::Unicode)", _
        "    Add-UiLog('GitHub archive extracted and all required Python source files validated.')", _
        "    Set-UiStage 2 'Checking for a compatible 64-bit Python runtime...' 10", _
        "    $pythonw = Find-CompatiblePython", _
        "    if (!$pythonw) {", _
        "        Add-UiLog('Compatible Python not found. Downloading the verified Python 3.13.17 x64 installer.')", _
        "        $pythonInstaller = Join-Path $script:cacheRoot 'python-3.13.17-amd64.exe'" _
    ), vbCrLf)
End Function

Function PSChunk23()
    PSChunk23 = Join(Array( _
        "        $pythonUrl = 'https://www.python.org/ftp/python/3.13.17/python-3.13.17-amd64.exe'", _
        "        Download-ToFileWithRetry $pythonUrl $pythonInstaller 'Python 3.13.17 x64 installer' 7200", _
        "        $expectedHash = '5da012101ba2e17113a9a1cd901e9ce1441086f2b7ecb5de06ec94163899a190'", _
        "        $actualHash = (Get-FileHash -LiteralPath $pythonInstaller -Algorithm SHA256).Hash.ToLowerInvariant()", _
        "        if ($actualHash -ne $expectedHash) { throw 'Python installer SHA-256 verification failed.' }", _
        "        $signature = Get-AuthenticodeSignature -FilePath $pythonInstaller", _
        "        if ($signature.Status -ne 'Valid') { throw ('Python installer signature is not valid: ' + $signature.Status) }", _
        "        $targetPython = Join-Path $env:LOCALAPPDATA 'Programs\Python\Python313'", _
        "        $arguments = '/quiet InstallAllUsers=0 ""TargetDir=' + $targetPython + '"" Include_launcher=1 Include_tcltk=1 Include_pip=1 Include_test=0 Shortcuts=0 PrependPath=0 AssociateFiles=0'", _
        "        $installerProcess = Start-Process -FilePath $pythonInstaller -ArgumentList $arguments -PassThru", _
        "        Wait-WithUi $installerProcess 'Installing Python 3.13.17 x64...'", _
        "        $pythonw = Join-Path $targetPython 'pythonw.exe'", _
        "        $pythonExe = Join-Path $targetPython 'python.exe'", _
        "        if (!(Test-Path -LiteralPath $pythonw) -or !(Test-Path -LiteralPath $pythonExe)) {" _
    ), vbCrLf)
End Function

Function PSChunk24()
    PSChunk24 = Join(Array( _
        "            throw 'Python installer completed but python.exe and pythonw.exe were not found.'", _
        "        }", _
        "        $testOutput = & $pythonExe -c 'import sys,struct,tkinter; print(sys.version_info.major,sys.version_info.minor,struct.calcsize(''P'')*8)' 2>$null", _
        "        if ($LASTEXITCODE -ne 0 -or (($testOutput -join ' ').Trim() -notmatch '^3\s+13\s+64$')) {", _
        "            throw 'The newly installed Python runtime failed its 64-bit/Tkinter check.'", _
        "        }", _
        "    }", _
        "    $pythonw = [System.IO.Path]::GetFullPath($pythonw)", _
        "    [System.IO.File]::WriteAllText($script:pythonMarkerPath, $pythonw, [System.Text.Encoding]::Unicode)", _
        "    Add-UiLog('Python runtime is ready: ' + $pythonw)", _
        "    Set-UiStage 3 'GitHub source and Python are ready. Opening the build window...' 100", _
        "    Add-UiLog('Handing off to the normal HarvHub AI build window.')", _
        "    Start-Sleep -Milliseconds 700", _
        "    $form.Close()" _
    ), vbCrLf)
End Function

Function PSChunk25()
    PSChunk25 = Join(Array( _
        "    exit 0", _
        "} catch {", _
        "    $errorRecord = $_", _
        "    $message = @(", _
        "        'HARVHUB AI INSTALLER FAILURE',", _
        "        ('Timestamp: ' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff')) ,", _
        "        ('Message: ' + $errorRecord.Exception.Message),", _
        "        ('Category: ' + $errorRecord.CategoryInfo),", _
        "        ('FullyQualifiedErrorId: ' + $errorRecord.FullyQualifiedErrorId),", _
        "        ('ScriptStackTrace: ' + $errorRecord.ScriptStackTrace),", _
        "        'Source position:',", _
        "        ($errorRecord.InvocationInfo.PositionMessage),", _
        "        'Complete PowerShell error record:',", _
        "        ($errorRecord | Format-List * -Force | Out-String),", _
        "        'Exception details:',", _
        "        ($errorRecord.Exception.ToString())", _
        "    ) -join [Environment]::NewLine", _
        "    try { [System.IO.File]::WriteAllText($script:errorPath, $message, [System.Text.Encoding]::Unicode) } catch { }", _
        "    try { Add-UiLog('ERROR: ' + $errorRecord.Exception.Message); $script:logBox.AppendText([Environment]::NewLine + $message + [Environment]::NewLine); $script:logBox.SelectionStart = $script:logBox.TextLength; $script:logBox.ScrollToCaret() } catch { }", _
        "    try { $script:statusLabel.Text = 'Setup stopped. Copy the full error details or read the report file.'; $script:statusLabel.ForeColor = [System.Drawing.Color]::FromArgb(185,55,45) } catch { }", _
        "    try { $script:progress.Style = 'Blocks'; $script:progress.Value = 0; $form.ControlBox = $true } catch { }", _
        "    try { $script:copyButton.Visible = $true; $script:copyButton.Enabled = $true; $script:closeButton.Visible = $true; $script:closeButton.Enabled = $true } catch { }", _
        "    while ($form.Visible) {", _
        "        Pump-Ui" _
    ), vbCrLf)
End Function

Function PSChunk26()
    PSChunk26 = Join(Array( _
        "        Start-Sleep -Milliseconds 100", _
        "    }", _
        "    exit 1", _
        "}" _
    ), vbCrLf)
End Function


Sub StopExistingHarvHubProcesses()
    Dim helperPath, helperFile, helperScript, cleanupExit, cleanupError
    On Error Resume Next
    helperPath = fso.BuildPath(cacheRoot, "cleanup_" & Replace(fso.GetTempName(), ".tmp", "") & ".ps1")

    helperScript = Join(Array( _
        "$ErrorActionPreference = 'Stop'", _
        "$cacheRoot = Join-Path $env:LOCALAPPDATA 'HarvHubAI\Bootstrap'", _
        "$logPath = Join-Path $cacheRoot 'process_cleanup.log'", _
        "New-Item -ItemType Directory -Path $cacheRoot -Force | Out-Null", _
        "function Add-CleanupLog([string]$Message) { Add-Content -LiteralPath $logPath -Value ('[' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + '] ' + $Message) -Encoding UTF8 }", _
        "Write-Host '[HarvHub AI] Scanning for earlier HarvHub setup, manager, builder, and service processes...' -ForegroundColor Cyan", _
        "try {", _
        "    $all = @(Get-CimInstance -ClassName Win32_Process)", _
        "    $helper = $all | Where-Object { [int]$_.ProcessId -eq $PID } | Select-Object -First 1", _
        "    if (!$helper) { throw 'Could not identify the current cleanup process.' }", _
        "    $launcherPid = [int]$helper.ParentProcessId", _
        "    $launcher = $all | Where-Object { [int]$_.ProcessId -eq $launcherPid } | Select-Object -First 1", _
        "    if (!$launcher) { throw 'Could not identify the current HarvHub launcher process.' }", _
        "    $launchTime = [datetime]$launcher.CreationDate", _
        "    $protectedIds = New-Object 'System.Collections.Generic.HashSet[int]'", _
        "    [void]$protectedIds.Add($launcherPid)", _
        "    $launcherParent = $all | Where-Object { [int]$_.ProcessId -eq [int]$launcher.ParentProcessId } | Select-Object -First 1", _
        "    if ($launcherParent -and ([string]$launcherParent.Name -match '^(wscript|cscript)\.exe$')) { [void]$protectedIds.Add([int]$launcherParent.ProcessId) }", _
        "    $processById = @{}", _
        "    foreach ($proc in $all) { $processById[[int]$proc.ProcessId] = $proc }", _
        "    $rootIds = New-Object 'System.Collections.Generic.HashSet[int]'", _
        "    foreach ($proc in $all) {", _
        "        $procId = [int]$proc.ProcessId", _
        "        if ($protectedIds.Contains($procId)) { continue }", _
        "        $name = ([string]$proc.Name).ToLowerInvariant()", _
        "        $cmd = [string]$proc.CommandLine", _
        "        $isHarvHub = $false", _
        "        if ($name -eq 'harvhubai.exe') { $isHarvHub = $true }", _
        "        elseif (($name -eq 'wscript.exe' -or $name -eq 'cscript.exe') -and $cmd -match '(?i)(HarvHubAI|Build_HarvHubProgram)\.vbs') { $isHarvHub = $true }", _
        "        elseif ($name -eq 'powershell.exe' -and $cmd -match '(?i)(visible_bootstrap\.ps1|visible_builder_launch\.ps1|console_first_bootstrap\.ps1|cleanup_[^\\]*\.ps1)') { $isHarvHub = $true }", _
        "        elseif ($name -eq 'cmd.exe' -and $cmd -match '(?i)(HarvHubAI_First_Time_Setup\.cmd|HarvHubAI_Visible_(Installer|Build)\.cmd|cleanup_[^\\]*\.cmd)') { $isHarvHub = $true }", _
        "        elseif (($name -eq 'python.exe' -or $name -eq 'pythonw.exe') -and $cmd -match '(?i)(Build_HarvHubProgram\.pyw|mysqlremote\.py|brokerdetails\.py|brokercharts\.py|programme_training\.py|signals_trading\.py|signals_provision_challenge\.py)') { $isHarvHub = $true }", _
        "        elseif ($name -match '^python-3\..*-amd64\.exe$' -and $cmd -match '(?i)HarvHubAI\\Bootstrap') { $isHarvHub = $true }", _
        "        if (!$isHarvHub) { continue }", _
        "        $older = $false", _
        "        try {", _
        "            $started = [datetime]$proc.CreationDate", _
        "            if ($started -le $launchTime) { $older = $true }", _
        "        } catch { $older = $true }", _
        "        if ($older) { [void]$rootIds.Add($procId) }", _
        "    }", _
        "    $targetIds = New-Object 'System.Collections.Generic.HashSet[int]'", _
        "    foreach ($id in $rootIds) { [void]$targetIds.Add([int]$id) }", _
        "    $changed = $true", _
        "    while ($changed) {", _
        "        $changed = $false", _
        "        foreach ($proc in $all) {", _
        "            $procId = [int]$proc.ProcessId", _
        "            $parentId = [int]$proc.ParentProcessId", _
        "            if (!$protectedIds.Contains($procId) -and !$targetIds.Contains($procId) -and $targetIds.Contains($parentId)) {", _
        "                [void]$targetIds.Add($procId); $changed = $true", _
        "            }", _
        "        }", _
        "    }", _
        "    $targets = @($all | Where-Object { $targetIds.Contains([int]$_.ProcessId) })", _
        "    Write-Host ('[HarvHub AI] Found ' + $targets.Count + ' older HarvHub process(es) to stop.') -ForegroundColor Yellow", _
        "    $depthById = @{}", _
        "    foreach ($proc in $targets) {", _
        "        $depth = 0; $parentId = [int]$proc.ParentProcessId", _
        "        while ($targetIds.Contains($parentId) -and $depth -lt 64) {", _
        "            $depth++", _
        "            if (!$processById.ContainsKey($parentId)) { break }", _
        "            $parentId = [int]$processById[$parentId].ParentProcessId", _
        "        }", _
        "        $depthById[[int]$proc.ProcessId] = $depth", _
        "    }", _
        "    $ordered = @($targets | Sort-Object -Property @{ Expression = { $depthById[[int]$_.ProcessId] }; Descending = $true })", _
        "    foreach ($proc in $ordered) {", _
        "        try {", _
        "            Write-Host ('[HarvHub AI] Stopping ' + $proc.Name + ' (PID ' + $proc.ProcessId + ')...')", _
        "            Stop-Process -Id ([int]$proc.ProcessId) -Force -ErrorAction Stop", _
        "            Add-CleanupLog ('Stopped older HarvHub process: ' + $proc.Name + ' (PID ' + $proc.ProcessId + ')')", _
        "            Write-Host ('[HarvHub AI] Stopped ' + $proc.Name + ' (PID ' + $proc.ProcessId + ').') -ForegroundColor Green", _
        "        } catch {", _
        "            $stillThere = Get-Process -Id ([int]$proc.ProcessId) -ErrorAction SilentlyContinue", _
        "            if ($stillThere) { Add-CleanupLog ('ERROR stopping ' + $proc.Name + ' (PID ' + $proc.ProcessId + '): ' + $_.Exception.Message) }", _
        "        }", _
        "    }", _
        "    $deadline = (Get-Date).AddSeconds(12)", _
        "    do {", _
        "        $remaining = @(Get-CimInstance -ClassName Win32_Process | Where-Object { $targetIds.Contains([int]$_.ProcessId) })", _
        "        if ($remaining.Count -eq 0) { break }", _
        "        Start-Sleep -Milliseconds 250", _
        "    } while ((Get-Date) -lt $deadline)", _
        "    if ($remaining.Count -gt 0) {", _
        "        $details = ($remaining | ForEach-Object { $_.Name + ' (PID ' + $_.ProcessId + ')' }) -join ', '", _
        "        throw ('Could not close all older HarvHub processes: ' + $details)", _
        "    }", _
        "    foreach ($stale in @('project_root.txt','pythonw_path.txt','bootstrap_error.txt')) {", _
        "        $stalePath = Join-Path $cacheRoot $stale", _
        "        if (Test-Path -LiteralPath $stalePath) { Remove-Item -LiteralPath $stalePath -Force -ErrorAction SilentlyContinue }", _
        "    }", _
        "    Get-ChildItem -LiteralPath $cacheRoot -Filter 'HarvHubAI-*.zip' -File -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue", _
        "    Add-CleanupLog ('Cleanup completed. Stopped ' + $targets.Count + ' older HarvHub process(es); current launcher PID ' + $launcherPid + ' is retained.')", _
        "    Write-Host '[HarvHub AI] Previous HarvHub processes are cleared. Current setup is protected.' -ForegroundColor Green", _
        "    exit 0", _
        "} catch {", _
        "    Add-CleanupLog ('FATAL: ' + ($_ | Out-String).Trim())", _
        "    Write-Host ('[HarvHub AI] CLEANUP ERROR: ' + ($_ | Out-String).Trim()) -ForegroundColor Red", _
        "    exit 1", _
        "}" _
    ), vbCrLf)

    Set helperFile = fso.CreateTextFile(helperPath, True, False)
    helperFile.Write helperScript
    helperFile.Close
    If Err.Number <> 0 Then
        diagnosticText = "Could not prepare the HarvHub process-cleanup helper: " & helperPath & vbCrLf & _
            "VBScript error " & Err.Number & ": " & Err.Description
        Err.Clear
        Call ShowDiagnosticWindow("HarvHub AI cleanup helper could not be created", diagnosticText)
        WScript.Quit 24
    End If

    ' Run process cleanup without opening a command window. PowerShell records
    ' its own diagnostic details in process_cleanup.log when an error occurs.
    cleanupError = ""
    Err.Clear
    cleanupExit = sh.Run("powershell.exe -NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File """ & helperPath & """", 0, True)
    If Err.Number <> 0 Then
        cleanupError = Err.Description
        Err.Clear
        cleanupExit = 1
    End If
    If fso.FileExists(helperPath) Then fso.DeleteFile helperPath, True
    On Error GoTo 0

    If cleanupExit <> 0 Then
        diagnosticText = ReadAllText(fso.BuildPath(cacheRoot, "process_cleanup.log"))
        If Len(Trim(diagnosticText)) = 0 Then diagnosticText = cleanupError
        Call ShowDiagnosticWindow("HarvHub AI process cleanup failed", _
            "HarvHub AI could not stop all earlier HarvHub setup, manager, build, or service processes. Setup was stopped to prevent two instances from using the same files." & vbCrLf & vbCrLf & _
            "Cleanup helper: " & helperPath & vbCrLf & _
            "Cleanup log: " & fso.BuildPath(cacheRoot, "process_cleanup.log") & vbCrLf & vbCrLf & _
            ReadAllTextUtf8(fso.BuildPath(cacheRoot, "process_cleanup.log")) & vbCrLf & vbCrLf & diagnosticText)
        WScript.Quit 13
    End If
End Sub

Function IsAdministrator()
    Dim result
    result = sh.Run("powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden -Command ""$p=New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent()); if($p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)){exit 0}else{exit 1}""", 0, True)
    IsAdministrator = (result = 0)
End Function

' Retained for compatibility with older launcher copies; startup now force-cleans older HarvHub processes.
Function IsInstalledHarvHubRunning(ByVal targetPath)
    Dim wmi, processes, proc, executablePath, commandLine, processName, workerNames, workerName
    IsInstalledHarvHubRunning = False
    workerNames = Array("build_harvhubprogram.pyw", "mysqlremote.py", "brokerdetails.py", "brokercharts.py", "programme_training.py", "signals_trading.py", "signals_provision_challenge.py", "harvcore_console_supervisor.py")
    On Error Resume Next
    Set wmi = GetObject("winmgmts:\\.\root\cimv2")
    Set processes = wmi.ExecQuery("SELECT Name,ExecutablePath,CommandLine FROM Win32_Process WHERE Name='HarvHubAI.exe' OR Name='pythonw.exe' OR Name='python.exe'")
    For Each proc In processes
        executablePath = ""
        commandLine = ""
        processName = LCase(CStr(proc.Name))
        Err.Clear
        executablePath = CStr(proc.ExecutablePath)
        commandLine = CStr(proc.CommandLine)
        If Err.Number = 0 Then
            If processName = "harvhubai.exe" Then
                ' Block replacement of any HarvHub AI manager instance, including
                ' the previous C:\xampp\htdocs\harvcore installation path.
                IsInstalledHarvHubRunning = True
                Exit For
            End If
            If processName = "pythonw.exe" Or processName = "python.exe" Then
                For Each workerName In workerNames
                    If InStr(1, LCase(commandLine), CStr(workerName), vbTextCompare) > 0 Then
                        IsInstalledHarvHubRunning = True
                        Exit For
                    End If
                Next
                If IsInstalledHarvHubRunning Then Exit For
            End If
        End If
    Next
    On Error GoTo 0
End Function

Sub ShowDiagnosticWindow(ByVal heading, ByVal detailText)
    Dim reportPath, commandPath, reportText, stream, commandFile, comSpec, launchLine, errText
    On Error Resume Next
    reportPath = fso.BuildPath(sh.ExpandEnvironmentStrings("%TEMP%"), "HarvHubAI_last_error.txt")
    commandPath = fso.BuildPath(sh.ExpandEnvironmentStrings("%TEMP%"), "HarvHubAI_show_error.cmd")
    reportText = "HARVHUB AI - INSTALLER ERROR REPORT" & vbCrLf & _
        "====================================" & vbCrLf & _
        "Time: " & Now & vbCrLf & _
        "Computer: " & sh.ExpandEnvironmentStrings("%COMPUTERNAME%") & vbCrLf & _
        "User: " & sh.ExpandEnvironmentStrings("%USERDOMAIN%\%USERNAME%") & vbCrLf & _
        "Launcher: " & clickedVbs & vbCrLf & _
        "Bootstrap PowerShell: " & psScriptPath & vbCrLf & _
        "Bootstrap log: " & fso.BuildPath(cacheRoot, "visible_bootstrap.log") & vbCrLf & _
        "Report file: " & reportPath & vbCrLf & vbCrLf & _
        CStr(heading) & vbCrLf & String(70, "-") & vbCrLf & CStr(detailText) & vbCrLf

    ' UTF-8 report is easy to open, select and copy in Notepad.
    Err.Clear
    Set stream = CreateObject("ADODB.Stream")
    If Err.Number = 0 Then
        stream.Type = 2
        stream.Charset = "utf-8"
        stream.Open
        stream.WriteText reportText
        stream.SaveToFile reportPath, 2
        stream.Close
    End If
    If Err.Number <> 0 Or Not fso.FileExists(reportPath) Then
        Err.Clear
        Set stream = Nothing
        Set stream = fso.CreateTextFile(reportPath, True, True)
        stream.Write reportText
        stream.Close
    End If

    ' Keep a visible CMD window open with the complete report, even after WSH exits.
    Err.Clear
    Set commandFile = fso.CreateTextFile(commandPath, True, False)
    commandFile.WriteLine "@echo off"
    commandFile.WriteLine "title HarvHub AI - Installer Error Report"
    commandFile.WriteLine "color 0C"
    commandFile.WriteLine "chcp 65001 >nul"
    commandFile.WriteLine "echo HARVHUB AI INSTALLER FAILED"
    commandFile.WriteLine "echo Full diagnostic report follows."
    commandFile.WriteLine "echo."
    commandFile.WriteLine "type ""%TEMP%\HarvHubAI_last_error.txt"""
    commandFile.WriteLine "echo."
    commandFile.WriteLine "echo This report is also saved at:"
    commandFile.WriteLine "echo %TEMP%\HarvHubAI_last_error.txt"
    commandFile.WriteLine "echo."
    commandFile.WriteLine "echo Select text with the mouse and press Enter or right-click to copy."
    commandFile.WriteLine "echo Press any key to close this CMD window."
    commandFile.WriteLine "pause >nul"
    commandFile.Close

    Err.Clear
    comSpec = sh.ExpandEnvironmentStrings("%ComSpec%")
    launchLine = Chr(34) & comSpec & Chr(34) & " /k " & Chr(34) & Chr(34) & commandPath & Chr(34) & Chr(34)
    sh.Run launchLine, 1, False

    ' Also open Notepad so the whole report can be copied with Ctrl+A, Ctrl+C.
    Err.Clear
    sh.Run "notepad.exe " & Chr(34) & reportPath & Chr(34), 1, False
    Err.Clear
    On Error GoTo 0
End Sub

Function ReadAllText(ByVal filePath)
    Dim stream
    ReadAllText = ""
    On Error Resume Next
    If fso.FileExists(filePath) Then
        Set stream = fso.OpenTextFile(filePath, 1, False, -1)
        ReadAllText = stream.ReadAll
        stream.Close
    End If
    On Error GoTo 0
End Function

Function ReadAllTextUtf8(ByVal filePath)
    Dim stream
    ReadAllTextUtf8 = ""
    On Error Resume Next
    If fso.FileExists(filePath) Then
        Set stream = CreateObject("ADODB.Stream")
        stream.Type = 2
        stream.Charset = "utf-8"
        stream.Open
        stream.LoadFromFile filePath
        ReadAllTextUtf8 = stream.ReadText
        stream.Close
    End If
    On Error GoTo 0
End Function

Sub EnsureFolderTree(ByVal folderPath)
    Dim parentPath, folderError
    On Error Resume Next
    If fso.FolderExists(folderPath) Then Exit Sub
    parentPath = fso.GetParentFolderName(folderPath)
    If Len(parentPath) > 0 And Not fso.FolderExists(parentPath) Then EnsureFolderTree parentPath
    If Not fso.FolderExists(folderPath) Then fso.CreateFolder folderPath
    If Err.Number <> 0 Then
        folderError = "Could not create required folder: " & folderPath & vbCrLf & _
            "VBScript error " & Err.Number & ": " & Err.Description
        Err.Clear
        Call ShowDiagnosticWindow("HarvHub AI folder creation failed", folderError)
        WScript.Quit 20
    End If
    On Error GoTo 0
End Sub

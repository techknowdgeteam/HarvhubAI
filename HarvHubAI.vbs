Option Explicit

Dim fso, sh, shellApp, processEnv, localAppData, installRoot, cacheRoot
Dim psScriptPath, markerPath, pythonMarkerPath, psFile, psExit, projectRoot
Dim pythonwPath, pythonExePath, builderScript, adminResult, appRunning, clickedVbs
Dim visibleCmdPath, visibleCmdFile, builderCmdPath, builderCmdFile, builderExit

Set fso = CreateObject("Scripting.FileSystemObject")
Set sh = CreateObject("WScript.Shell")
Set shellApp = CreateObject("Shell.Application")

localAppData = sh.ExpandEnvironmentStrings("%LOCALAPPDATA%")
installRoot = "C:\xampp\htdocs\harvhubAI"
cacheRoot = fso.BuildPath(localAppData, "HarvHubAI\Bootstrap")
psScriptPath = fso.BuildPath(cacheRoot, "visible_bootstrap.ps1")
markerPath = fso.BuildPath(cacheRoot, "project_root.txt")
pythonMarkerPath = fso.BuildPath(cacheRoot, "pythonw_path.txt")
visibleCmdPath = fso.BuildPath(cacheRoot, "HarvHubAI_Visible_Installer.cmd")
builderCmdPath = fso.BuildPath(cacheRoot, "HarvHubAI_Visible_Build.cmd")
clickedVbs = WScript.ScriptFullName

' Elevate before downloading so the visible installer is not restarted midway.
If Not IsAdministrator() Then
    shellApp.ShellExecute "wscript.exe", """" & WScript.ScriptFullName & """ /elevated", fso.GetParentFolderName(WScript.ScriptFullName), "runas", 1
    WScript.Quit 0
End If

' Enforce one active HarvHub setup/update session. Older HarvHub manager,
' bootstrap, builder, and service processes are stopped before files are touched.
' Unrelated Windows applications and system processes are never targeted.
Call EnsureFolderTree(cacheRoot)
Call StopExistingHarvHubProcesses()

' Always open the visible Windows Forms installer, regardless of whether Python
' is already installed. A visible CMD host is kept underneath it so PowerShell
' startup/parse errors are readable and remain on screen instead of disappearing.
Set processEnv = sh.Environment("PROCESS")
processEnv("HARVHUBAI_CLICKED_VBS") = clickedVbs

Set psFile = fso.CreateTextFile(psScriptPath, True, False)
psFile.Write BootstrapPowerShell()
psFile.Close

Set visibleCmdFile = fso.CreateTextFile(visibleCmdPath, True, False)
visibleCmdFile.WriteLine "@echo off"
visibleCmdFile.WriteLine "title HarvHub AI - Visible Installer Bootstrap"
visibleCmdFile.WriteLine "color 0E"
visibleCmdFile.WriteLine "echo."
visibleCmdFile.WriteLine "echo  HARVHUB AI - INSTALLER STARTUP"
visibleCmdFile.WriteLine "echo  This window remains visible for diagnostics while the installer opens."
visibleCmdFile.WriteLine "echo."
visibleCmdFile.WriteLine "powershell.exe -NoLogo -NoProfile -STA -ExecutionPolicy Bypass -File """ & psScriptPath & """"
visibleCmdFile.WriteLine "set ""BOOTSTRAP_EXIT=%ERRORLEVEL%"""
visibleCmdFile.WriteLine "if not ""%BOOTSTRAP_EXIT%""==""0"" ("
visibleCmdFile.WriteLine "  echo."
visibleCmdFile.WriteLine "  echo  ERROR: The HarvHub visual bootstrap exited with code %BOOTSTRAP_EXIT%."
visibleCmdFile.WriteLine "  echo  The PowerShell error output above explains why the installer did not open."
visibleCmdFile.WriteLine "  echo."
visibleCmdFile.WriteLine "  pause"
visibleCmdFile.WriteLine "  exit /b %BOOTSTRAP_EXIT%"
visibleCmdFile.WriteLine ")"
visibleCmdFile.WriteLine "exit /b 0"
visibleCmdFile.Close

psExit = sh.Run("cmd.exe /d /c """ & visibleCmdPath & """", 1, True)
On Error Resume Next
If fso.FileExists(visibleCmdPath) Then fso.DeleteFile visibleCmdPath, True
On Error GoTo 0
If psExit <> 0 Then
    MsgBox "HarvHub AI setup could not complete its visible bootstrap. The diagnostic CMD window displayed the exact error.", vbCritical, "HarvHub AI bootstrap"
    WScript.Quit psExit
End If

projectRoot = Trim(ReadAllText(markerPath))
pythonwPath = Trim(ReadAllText(pythonMarkerPath))
If Len(projectRoot) = 0 Or Not fso.FolderExists(projectRoot) Then
    MsgBox "The downloaded HarvHub AI source folder could not be found. Run HarvHubAI.vbs again.", vbCritical, "HarvHub AI bootstrap"
    WScript.Quit 10
End If
If Len(pythonwPath) = 0 Or Not fso.FileExists(pythonwPath) Then
    MsgBox "The Python GUI runtime could not be found after bootstrap.", vbCritical, "HarvHub AI bootstrap"
    WScript.Quit 11
End If

builderScript = fso.BuildPath(projectRoot, "Build_HarvHubProgram.pyw")
If Not fso.FileExists(builderScript) Then
    MsgBox "The downloaded GitHub project is missing Build_HarvHubProgram.pyw.", vbCritical, "HarvHub AI bootstrap"
    WScript.Quit 12
End If

Call EnsureFolderTree(installRoot)
pythonExePath = fso.BuildPath(fso.GetParentFolderName(pythonwPath), "python.exe")
If Not fso.FileExists(pythonExePath) Then
    MsgBox "The matching python.exe console runtime was not found beside pythonw.exe: " & pythonExePath, vbCritical, "HarvHub AI build startup"
    WScript.Quit 14
End If

processEnv("HARVHUBAI_SOURCE_ROOT") = projectRoot
processEnv("HARVHUBAI_ROOT") = installRoot
processEnv("HARVHUBAI_BOOTSTRAPPED") = "1"
processEnv("HARVHUBAI_GITHUB_BOOTSTRAPPED") = "1"

' Start the builder through python.exe (not pythonw.exe). The Tk build window
' remains the main UI, and this console exposes Python tracebacks/startup errors.
' If the builder exits unexpectedly, the CMD wrapper pauses and keeps the error visible.
Set builderCmdFile = fso.CreateTextFile(builderCmdPath, True, False)
builderCmdFile.WriteLine "@echo off"
builderCmdFile.WriteLine "title HarvHub AI - Build and Installation"
builderCmdFile.WriteLine "color 0E"
builderCmdFile.WriteLine "echo."
builderCmdFile.WriteLine "echo  HARVHUB AI - BUILD AND INSTALLATION"
builderCmdFile.WriteLine "echo  The HarvHub build window should now appear. This console shows diagnostics."
builderCmdFile.WriteLine "echo."
builderCmdFile.WriteLine """" & pythonExePath & """" & " " & """" & builderScript & """" & " --install --force-rebuild"
builderCmdFile.WriteLine "set ""BUILDER_EXIT=%ERRORLEVEL%"""
builderCmdFile.WriteLine "if not ""%BUILDER_EXIT%""==""0"" ("
builderCmdFile.WriteLine "  echo."
builderCmdFile.WriteLine "  echo  ERROR: HarvHub AI builder exited with code %BUILDER_EXIT%."
builderCmdFile.WriteLine "  echo  Read the Python traceback or error above."
builderCmdFile.WriteLine "  pause"
builderCmdFile.WriteLine "  exit /b %BUILDER_EXIT%"
builderCmdFile.WriteLine ")"
builderCmdFile.WriteLine "exit /b 0"
builderCmdFile.Close

builderExit = sh.Run("cmd.exe /d /c """ & builderCmdPath & """", 1, False)
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
        "New-Item -ItemType Directory -Path $script:cacheRoot -Force | Out-Null", _
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
        "[System.Windows.Forms.Application]::DoEvents()", _
        "function Pump-Ui {", _
        "    $form.Refresh()", _
        "    [System.Windows.Forms.Application]::DoEvents()", _
        "}", _
        "function Add-UiLog([string]$Message) {", _
        "    $script:logBox.AppendText(('[' + (Get-Date -Format 'HH:mm:ss') + '] ' + $Message + [Environment]::NewLine))" _
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
        "function Download-ToFile([string]$Url, [string]$Destination, [string]$Label) {", _
        "    Add-UiLog(('Downloading ' + $Label + '.'))", _
        "    $request = $null; $response = $null; $inputStream = $null; $outputStream = $null", _
        "    $phase = 'opening a connection to the download server'", _
        "    try {", _
        "        $request = [System.Net.HttpWebRequest]::Create($Url)", _
        "        $request.Method = 'GET'", _
        "        $request.UserAgent = 'HarvHubAI-Installer'", _
        "        $request.Timeout = 90000", _
        "        $request.ReadWriteTimeout = 90000", _
        "        $request.AllowAutoRedirect = $true", _
        "        $script:progress.Style = 'Marquee'", _
        "        $script:progress.MarqueeAnimationSpeed = 25", _
        "        $script:statusLabel.Text = 'Connecting to GitHub... (90-second response limit)'", _
        "        Pump-Ui" _
    ), vbCrLf)
End Function

Function PSChunk12()
    PSChunk12 = Join(Array( _
        "        $phase = 'waiting for the server response (90-second limit)'", _
        "        $responseTimer = [System.Diagnostics.Stopwatch]::StartNew()", _
        "        $asyncResponse = $request.BeginGetResponse($null, $null)", _
        "        $lastWaitNotice = 0", _
        "        while (!$asyncResponse.IsCompleted) {", _
        "            $waitSeconds = [int]$responseTimer.Elapsed.TotalSeconds", _
        "            if ($waitSeconds -ge 90) { try { $request.Abort() } catch {}; throw ('Timed out after 90 seconds waiting for GitHub response headers. Check internet access, DNS, firewall, proxy settings, and GitHub availability. URL: ' + $Url) }", _
        "            if ($waitSeconds -ge ($lastWaitNotice + 15)) { $lastWaitNotice = $waitSeconds; Add-UiLog('Still waiting for GitHub response headers: ' + $waitSeconds + '/90 seconds.'); $script:statusLabel.Text = 'Waiting for GitHub response... ' + $waitSeconds + '/90 seconds' }", _
        "            Pump-Ui", _
        "            Start-Sleep -Milliseconds 100", _
        "        }", _
        "        $response = $request.EndGetResponse($asyncResponse)", _
        "        $total = [long]$response.ContentLength", _
        "        if ($total -gt 0) { Add-UiLog('GitHub responded. ZIP size: ' + [Math]::Round($total / 1MB, 2) + ' MB.') } else { Add-UiLog('GitHub responded. The server did not provide a ZIP size; received bytes will be monitored.') }", _
        "        $phase = 'opening the GitHub ZIP response stream'", _
        "        $inputStream = $response.GetResponseStream()", _
        "        if (!$inputStream) { throw 'GitHub returned an empty response stream.' }", _
        "        $outputStream = [System.IO.File]::Open($Destination, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)", _
        "        $downloadTimer = [System.Diagnostics.Stopwatch]::StartNew()", _
        "        [long]$received = 0", _
        "        $lastProgressNotice = 0" _
    ), vbCrLf)
End Function

Function PSChunk13()
    PSChunk13 = Join(Array( _
        "        $phase = 'downloading the GitHub ZIP (20-minute total limit; 90-second idle limit)'", _
        "        $buffer = New-Object byte[] 65536", _
        "        while ($true) {", _
        "            if ($downloadTimer.Elapsed.TotalSeconds -ge 1200) { try { $request.Abort() } catch {}; throw 'GitHub ZIP download exceeded the 20-minute overall limit. The connection was stopped.' }", _
        "            $readTimer = [System.Diagnostics.Stopwatch]::StartNew()", _
        "            $asyncRead = $inputStream.BeginRead($buffer, 0, $buffer.Length, $null, $null)", _
        "            $lastIdleNotice = 0", _
        "            while (!$asyncRead.IsCompleted) {", _
        "                $idleSeconds = [int]$readTimer.Elapsed.TotalSeconds", _
        "                if ($downloadTimer.Elapsed.TotalSeconds -ge 1200) { try { $request.Abort() } catch {}; throw 'GitHub ZIP download exceeded the 20-minute overall limit. The connection was stopped.' }", _
        "                if ($idleSeconds -ge 90) { try { $request.Abort() } catch {}; throw ('No ZIP data arrived for 90 seconds. The GitHub download connection appears stalled. Received so far: ' + [Math]::Round($received / 1MB, 2) + ' MB. URL: ' + $Url) }", _
        "                if ($idleSeconds -ge ($lastIdleNotice + 15)) { $lastIdleNotice = $idleSeconds; Add-UiLog('GitHub is connected but no ZIP data has arrived for ' + $idleSeconds + ' seconds. Received: ' + [Math]::Round($received / 1MB, 2) + ' MB.') }", _
        "                Pump-Ui", _
        "                Start-Sleep -Milliseconds 100", _
        "            }", _
        "            $count = $inputStream.EndRead($asyncRead)", _
        "            if ($count -le 0) { break }", _
        "            $outputStream.Write($buffer, 0, $count)", _
        "            $received += $count", _
        "            if ($total -gt 0) { $percent = [int][Math]::Min(100, ($received * 100.0 / $total)); $script:progress.Style = 'Blocks'; $script:progress.Value = $percent; $script:statusLabel.Text = $Label + ' - ' + $percent + '% (' + [Math]::Round($received / 1MB, 1) + ' / ' + [Math]::Round($total / 1MB, 1) + ' MB)' } else { $script:progress.Style = 'Marquee'; $script:statusLabel.Text = $Label + ' - ' + [Math]::Round($received / 1MB, 1) + ' MB received' }", _
        "            $transferSizeText = ''", _
        "            if ($total -gt 0) { $transferSizeText = ' of ' + [Math]::Round($total / 1MB, 2) + ' MB' }", _
        "            if ([int]$downloadTimer.Elapsed.TotalSeconds -ge ($lastProgressNotice + 15)) { $lastProgressNotice = [int]$downloadTimer.Elapsed.TotalSeconds; Add-UiLog('GitHub ZIP transfer: ' + [Math]::Round($received / 1MB, 2) + ' MB received' + $transferSizeText + '.') }", _
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
        "        if (!(Test-Path -LiteralPath $Destination)) { throw 'The GitHub ZIP file was not created.' }", _
        "        $actualSize = (Get-Item -LiteralPath $Destination).Length", _
        "        if ($actualSize -lt 10240) { throw ('The GitHub ZIP is too small or incomplete: ' + $actualSize + ' bytes.') }", _
        "        if ($total -gt 0 -and $received -ne $total) { throw ('The GitHub ZIP is incomplete. Server declared ' + $total + ' bytes but only ' + $received + ' bytes were received.') }", _
        "        $script:progress.Style = 'Blocks'", _
        "        $script:progress.Value = 100", _
        "        Add-UiLog($Label + ' download completed: ' + [Math]::Round($actualSize / 1MB, 2) + ' MB.')" _
    ), vbCrLf)
End Function

Function PSChunk15()
    PSChunk15 = Join(Array( _
        "    } catch {", _
        "        $exception = $_.Exception", _
        "        $detail = $exception.Message", _
        "        $networkDetail = ''", _
        "        if ($exception -is [System.Net.WebException]) { $networkDetail = ' Network status: ' + $exception.Status + '.'; try { if ($exception.Response) { $networkDetail += ' HTTP response: ' + $exception.Response.StatusCode + ' ' + $exception.Response.StatusDescription + '.' } } catch { } }", _
        "        try { if ($outputStream) { $outputStream.Dispose(); $outputStream = $null } } catch { }", _
        "        try { if ($inputStream) { $inputStream.Dispose(); $inputStream = $null } } catch { }", _
        "        try { if ($response) { $response.Dispose(); $response = $null } } catch { }", _
        "        try { if ($request) { $request.Abort() } } catch { }", _
        "        try { Remove-Item -LiteralPath $Destination -Force -ErrorAction SilentlyContinue } catch { }", _
        "        $failure = $Label + ' download failed while ' + $phase + '. URL: ' + $Url + '. Cause: ' + $detail + $networkDetail", _
        "        Add-UiLog('DOWNLOAD FAILED: ' + $failure)", _
        "        throw $failure", _
        "    } finally {", _
        "        if ($outputStream) { try { $outputStream.Dispose() } catch { } }", _
        "        if ($inputStream) { try { $inputStream.Dispose() } catch { } }", _
        "        if ($response) { try { $response.Dispose() } catch { } }", _
        "    }", _
        "}", _
        "function Find-CompatiblePython {" _
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
        "    $script:progress.Style = 'Marquee'", _
        "    $script:progress.MarqueeAnimationSpeed = 25", _
        "    $script:statusLabel.Text = $Message", _
        "    while (!$Process.HasExited) {", _
        "        Pump-Ui", _
        "        Start-Sleep -Milliseconds 120", _
        "        $Process.Refresh()", _
        "    }", _
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
        "    $githubUrl = 'https://github.com/techknowdgeteam/HarvhubAI/archive/refs/heads/main.zip'", _
        "    Download-ToFile $githubUrl $script:zipPath 'GitHub repository ZIP'" _
    ), vbCrLf)
End Function

Function PSChunk19()
    PSChunk19 = Join(Array( _
        "    Set-UiStage 1 'Extracting and validating the downloaded HarvHub AI files...' 0", _
        "    Add-Type -AssemblyName System.IO.Compression.FileSystem", _
        "    $archive = [System.IO.Compression.ZipFile]::OpenRead($script:zipPath)", _
        "    try {", _
        "        if ($archive.Entries.Count -lt 5) { throw 'GitHub archive contains too few files.' }", _
        "    } finally { $archive.Dispose() }", _
        "    [System.IO.Compression.ZipFile]::ExtractToDirectory($script:zipPath, $script:extractRoot)", _
        "    Remove-Item -LiteralPath $script:zipPath -Force -ErrorAction SilentlyContinue", _
        "    $builderFile = Get-ChildItem -LiteralPath $script:extractRoot -Recurse -File -Filter 'Build_HarvHubProgram.pyw' | Select-Object -First 1", _
        "    if (!$builderFile) { throw 'GitHub repository is missing Build_HarvHubProgram.pyw.' }", _
        "    $project = $builderFile.Directory.FullName", _
        "    $managerFile = Join-Path $project 'harvcore_manager.py'", _
        "    if (!(Test-Path -LiteralPath $managerFile)) { throw 'GitHub repository is missing harvcore_manager.py beside the builder.' }", _
        "    $payload = Join-Path $project 'payload'", _
        "    New-Item -ItemType Directory -Path $payload -Force | Out-Null" _
    ), vbCrLf)
End Function

Function PSChunk20()
    PSChunk20 = Join(Array( _
        "    $sourceNames = @('mysqlremote.py','brokerdetails.py','brokercharts.py','programme_training.py','programmesignals_trading.py','signals_provision_challenge.py','ip.py','notification_email.py','notification_service.py','harvhub_ipc.py')", _
        "    foreach ($name in $sourceNames) {", _
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
        "            if (!$found) { throw ('Required Python source is missing: ' + $name) }", _
        "            Copy-Item -LiteralPath $found.FullName -Destination $target -Force", _
        "        }", _
        "        if (!(Test-Path -LiteralPath $target) -or (Get-Item -LiteralPath $target).Length -lt 100) {", _
        "            throw ('Python source is missing or incomplete: ' + $name)", _
        "        }", _
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
        "        Download-ToFile $pythonUrl $pythonInstaller 'Python 3.13.17 x64 installer'", _
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
        "    $message = ($_ | Out-String).Trim()", _
        "    try { [System.IO.File]::WriteAllText($script:errorPath, $message, [System.Text.Encoding]::Unicode) } catch { }", _
        "    Add-UiLog('ERROR: ' + $message)", _
        "    $script:statusLabel.Text = 'Setup stopped. Read the error above, then close this window.'", _
        "    $script:statusLabel.ForeColor = [System.Drawing.Color]::FromArgb(185,55,45)", _
        "    $script:progress.Style = 'Blocks'", _
        "    $script:progress.Value = 0", _
        "    $form.ControlBox = $true", _
        "    $script:closeButton.Visible = $true", _
        "    $script:closeButton.Enabled = $true", _
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
    Dim helperPath, helperFile, helperScript, cleanupExit, cleanupCmdPath, cleanupCmdFile
    helperPath = fso.BuildPath(cacheRoot, "cleanup_" & Replace(fso.GetTempName(), ".tmp", "") & ".ps1")
    cleanupCmdPath = fso.BuildPath(cacheRoot, "cleanup_" & Replace(fso.GetTempName(), ".tmp", "") & ".cmd")

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
        "        elseif ($name -eq 'powershell.exe' -and $cmd -match '(?i)(visible_bootstrap\.ps1|console_first_bootstrap\.ps1|cleanup_[^\\]*\.ps1)') { $isHarvHub = $true }", _
        "        elseif ($name -eq 'cmd.exe' -and $cmd -match '(?i)(HarvHubAI_First_Time_Setup\.cmd|HarvHubAI_Visible_(Installer|Build)\.cmd|cleanup_[^\\]*\.cmd)') { $isHarvHub = $true }", _
        "        elseif (($name -eq 'python.exe' -or $name -eq 'pythonw.exe') -and $cmd -match '(?i)(Build_HarvHubProgram\.pyw|mysqlremote\.py|brokerdetails\.py|brokercharts\.py|programme_training\.py|programmesignals_trading\.py|signals_provision_challenge\.py)') { $isHarvHub = $true }", _
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

    Set cleanupCmdFile = fso.CreateTextFile(cleanupCmdPath, True, False)
    cleanupCmdFile.WriteLine "@echo off"
    cleanupCmdFile.WriteLine "title HarvHub AI - Previous Process Cleanup"
    cleanupCmdFile.WriteLine "color 0E"
    cleanupCmdFile.WriteLine "echo."
    cleanupCmdFile.WriteLine "echo  HARVHUB AI - SINGLE INSTANCE CHECK"
    cleanupCmdFile.WriteLine "echo  Older HarvHub processes are being stopped before the new setup starts."
    cleanupCmdFile.WriteLine "echo."
    cleanupCmdFile.WriteLine "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File """ & helperPath & """"
    cleanupCmdFile.WriteLine "set ""CLEANUP_EXIT=%ERRORLEVEL%"""
    cleanupCmdFile.WriteLine "if not ""%CLEANUP_EXIT%""==""0"" ("
    cleanupCmdFile.WriteLine "  echo."
    cleanupCmdFile.WriteLine "  echo  ERROR: Previous HarvHub processes could not all be stopped."
    cleanupCmdFile.WriteLine "  echo  Cleanup log: """ & fso.BuildPath(cacheRoot, "process_cleanup.log") & """"
    cleanupCmdFile.WriteLine "  type """ & fso.BuildPath(cacheRoot, "process_cleanup.log") & """"
    cleanupCmdFile.WriteLine "  pause"
    cleanupCmdFile.WriteLine "  exit /b %CLEANUP_EXIT%"
    cleanupCmdFile.WriteLine ")"
    cleanupCmdFile.WriteLine "echo."
    cleanupCmdFile.WriteLine "echo  HarvHub single-instance cleanup completed."
    cleanupCmdFile.WriteLine "exit /b 0"
    cleanupCmdFile.Close

    cleanupExit = sh.Run("cmd.exe /d /c """ & cleanupCmdPath & """", 1, True)
    On Error Resume Next
    If fso.FileExists(helperPath) Then fso.DeleteFile helperPath, True
    If fso.FileExists(cleanupCmdPath) Then fso.DeleteFile cleanupCmdPath, True
    On Error GoTo 0

    If cleanupExit <> 0 Then
        MsgBox "HarvHub AI could not stop all earlier HarvHub setup, manager, build, or service processes. Setup has been stopped to prevent two instances from using the same files." & vbCrLf & vbCrLf & "Review: " & fso.BuildPath(cacheRoot, "process_cleanup.log"), vbCritical, "HarvHub AI process cleanup"
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
    workerNames = Array("build_harvhubprogram.pyw", "mysqlremote.py", "brokerdetails.py", "brokercharts.py", "programme_training.py", "programmesignals_trading.py", "signals_provision_challenge.py")
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

Sub EnsureFolderTree(ByVal folderPath)
    Dim parentPath
    If fso.FolderExists(folderPath) Then Exit Sub
    parentPath = fso.GetParentFolderName(folderPath)
    If Len(parentPath) > 0 And Not fso.FolderExists(parentPath) Then EnsureFolderTree parentPath
    If Not fso.FolderExists(folderPath) Then fso.CreateFolder folderPath
End Sub

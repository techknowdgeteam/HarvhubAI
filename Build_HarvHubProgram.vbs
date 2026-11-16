Option Explicit

Dim fso, sh, shellApp, processEnv, localAppData, installRoot, cacheRoot
Dim psScriptPath, markerPath, pythonMarkerPath, psFile, psExit, projectRoot
Dim pythonwPath, builderScript, adminResult, appRunning, clickedVbs
Dim consolePSPath, consoleCmdPath, consoleFile, cmdFile, consoleExit, pythonAvailable

Set fso = CreateObject("Scripting.FileSystemObject")
Set sh = CreateObject("WScript.Shell")
Set shellApp = CreateObject("Shell.Application")

localAppData = sh.ExpandEnvironmentStrings("%LOCALAPPDATA%")
installRoot = "C:\xampp\htdocs\harvhubAI"
cacheRoot = fso.BuildPath(localAppData, "HarvHubAI\Bootstrap")
psScriptPath = fso.BuildPath(cacheRoot, "visible_bootstrap.ps1")
markerPath = fso.BuildPath(cacheRoot, "project_root.txt")
pythonMarkerPath = fso.BuildPath(cacheRoot, "pythonw_path.txt")
consolePSPath = fso.BuildPath(cacheRoot, "console_first_bootstrap.ps1")
consoleCmdPath = fso.BuildPath(cacheRoot, "HarvHubAI_First_Time_Setup.cmd")
clickedVbs = WScript.ScriptFullName

' Elevate before downloading so the visible installer is not restarted midway.
If Not IsAdministrator() Then
    shellApp.ShellExecute "wscript.exe", """" & WScript.ScriptFullName & """ /elevated", fso.GetParentFolderName(WScript.ScriptFullName), "runas", 1
    WScript.Quit 0
End If

appRunning = IsInstalledHarvHubRunning(fso.BuildPath(installRoot, "HarvHubAI.exe"))
If appRunning Then
    MsgBox "HarvHub AI or one of its services is currently running. Close HarvHub AI normally before updating. No running service has been terminated.", vbInformation, "HarvHub AI update"
    WScript.Quit 3
End If

Call EnsureFolderTree(cacheRoot)
Set processEnv = sh.Environment("PROCESS")
processEnv("HARVHUBAI_CLICKED_VBS") = clickedVbs

' The bootstrap is independent of Python. Use the visible GUI bootstrap when a
' compatible Python/Tk runtime already exists. On a fresh VPS, use a visible CMD
' window so no Python-powered UI is required before Python has been installed.
pythonAvailable = HasCompatiblePython()
If Not pythonAvailable Then
    Set consoleFile = fso.CreateTextFile(consolePSPath, True, False)
    consoleFile.Write ConsoleBootstrapPowerShell()
    consoleFile.Close

    Set cmdFile = fso.CreateTextFile(consoleCmdPath, True, False)
    cmdFile.WriteLine "@echo off"
    cmdFile.WriteLine "title HarvHub AI - First-Time Setup"
    cmdFile.WriteLine "color 0E"
    cmdFile.WriteLine "echo."
    cmdFile.WriteLine "echo  HARVHUB AI - FIRST-TIME VPS BOOTSTRAP"
    cmdFile.WriteLine "echo  Downloading GitHub files and preparing Python before opening the desktop installer."
    cmdFile.WriteLine "echo."
    cmdFile.WriteLine "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File """ & consolePSPath & """"
    cmdFile.WriteLine "set BOOTSTRAP_EXIT=%ERRORLEVEL%"
    cmdFile.WriteLine "if not ""%BOOTSTRAP_EXIT%""==""0"" (echo. & echo HarvHub AI bootstrap failed with exit code %BOOTSTRAP_EXIT%. & echo Review the error above. & pause & exit /b %BOOTSTRAP_EXIT%)"
    cmdFile.WriteLine "echo."
    cmdFile.WriteLine "echo Python and the GitHub source are verified. Returning to HarvHub AI..."
    cmdFile.Close

    consoleExit = sh.Run("cmd.exe /d /c """ & consoleCmdPath & """", 1, True)
    If consoleExit <> 0 Then WScript.Quit consoleExit
Else
    Set psFile = fso.CreateTextFile(psScriptPath, True, False)
    psFile.Write BootstrapPowerShell()
    psFile.Close
    ' Only the PowerShell console is hidden; the WinForms HarvHub AI installer is visible.
    psExit = sh.Run("powershell.exe -NoLogo -NoProfile -STA -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & psScriptPath & """", 0, True)
    If psExit <> 0 Then WScript.Quit psExit
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
processEnv("HARVHUBAI_SOURCE_ROOT") = projectRoot
processEnv("HARVHUBAI_ROOT") = installRoot
processEnv("HARVHUBAI_BOOTSTRAPPED") = "1"
processEnv("HARVHUBAI_GITHUB_BOOTSTRAPPED") = "1"
sh.Run """" & pythonwPath & """ """ & builderScript & """ --install --force-rebuild", 0, False
WScript.Quit 0

Function HasCompatiblePython()
    Dim command, result
    command = "powershell.exe -NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -Command ""$ErrorActionPreference='SilentlyContinue'; $bases=@((Join-Path $env:LOCALAPPDATA 'Programs\Python'),$env:ProgramFiles); $versions=@('Python313','Python312','Python311','Python310','Python314'); $candidates=@(); foreach($base in $bases){ if(!$base){continue}; foreach($version in $versions){$candidates += (Join-Path (Join-Path $base $version) 'python.exe')} }; $cmd=Get-Command python.exe -ErrorAction SilentlyContinue; if($cmd){$candidates += $cmd.Source}; foreach($py in ($candidates | Select-Object -Unique)){if(Test-Path -LiteralPath $py){$out=& $py -c 'import sys,struct,tkinter; print(sys.version_info.major,sys.version_info.minor,struct.calcsize(chr(80))*8)' 2>$null; if($LASTEXITCODE -eq 0 -and (($out -join ' ').Trim() -match '^3\s+13\s+64$')){exit 0}}}; exit 1"""
    result = sh.Run(command, 0, True)
    HasCompatiblePython = (result = 0)
End Function

Function ConsoleBootstrapPowerShell()
    ConsoleBootstrapPowerShell = Join(Array( _
        "$ErrorActionPreference = 'Stop'", _
        "$ProgressPreference = 'SilentlyContinue'", _
        "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12", _
        "$cacheRoot = Join-Path $env:LOCALAPPDATA 'HarvHubAI\Bootstrap'", _
        "$extractRoot = Join-Path $cacheRoot 'source'", _
        "$zipPath = Join-Path $cacheRoot 'HarvHubAI-latest.zip'", _
        "$markerPath = Join-Path $cacheRoot 'project_root.txt'", _
        "$pythonMarkerPath = Join-Path $cacheRoot 'pythonw_path.txt'", _
        "$clickedVbs = $env:HARVHUBAI_CLICKED_VBS", _
        "New-Item -ItemType Directory -Path $cacheRoot -Force | Out-Null", _
        "function Write-Step([string]$Message) { Write-Host ('[' + (Get-Date -Format 'HH:mm:ss') + '] ' + $Message) -ForegroundColor Yellow }", _
        "function Download-ToFile([string]$Url,[string]$Destination,[string]$Label) {", _
        "  Write-Step ('Downloading ' + $Label + '...')", _
        "  $request = [System.Net.HttpWebRequest]::Create($Url); $request.Method='GET'; $request.UserAgent='HarvHubAI-Installer'; $request.Timeout=120000; $request.ReadWriteTimeout=120000; $request.AllowAutoRedirect=$true", _
        "  $response=$request.GetResponse(); $inputStream=$response.GetResponseStream(); $outputStream=[System.IO.File]::Create($Destination); $total=[long]$response.ContentLength; [long]$done=0; $buffer=New-Object byte[] 65536", _
        "  try { while(($count=$inputStream.Read($buffer,0,$buffer.Length)) -gt 0) { $outputStream.Write($buffer,0,$count); $done += $count; if($total -gt 0){$pct=[int][Math]::Min(100,($done*100.0/$total)); Write-Progress -Activity $Label -Status ($pct.ToString() + '%') -PercentComplete $pct } else { Write-Progress -Activity $Label -Status ('Downloaded ' + [Math]::Round($done/1MB,1) + ' MB') } } } finally { $outputStream.Dispose(); $inputStream.Dispose(); $response.Dispose() }", _
        "  if(!(Test-Path -LiteralPath $Destination) -or (Get-Item -LiteralPath $Destination).Length -lt 10240){throw ($Label + ' download is empty or incomplete.')}; Write-Progress -Activity $Label -Completed", _
        "}", _
        "function Find-CompatiblePython {", _
        "  $bases=@((Join-Path $env:LOCALAPPDATA 'Programs\Python'),$env:ProgramFiles); $versions=@('Python313','Python312','Python311','Python310','Python314'); $candidates=@()", _
        "  foreach($base in $bases){if(!$base){continue}; foreach($version in $versions){$candidates += (Join-Path (Join-Path $base $version) 'python.exe')}}", _
        "  $cmd=Get-Command python.exe -ErrorAction SilentlyContinue; if($cmd){$candidates += $cmd.Source}", _
        "  foreach($py in ($candidates | Select-Object -Unique)){if(Test-Path -LiteralPath $py){try{$out=& $py -c 'import sys,struct,tkinter; print(sys.version_info.major,sys.version_info.minor,struct.calcsize(chr(80))*8)' 2>$null; if($LASTEXITCODE -eq 0 -and (($out -join ' ').Trim() -match '^3\s+13\s+64$')){return $py}}catch{}}}; return $null", _
        "}", _
        "try {", _
        "  Write-Host ''; Write-Host 'HARVHUB AI - FIRST-TIME VPS SETUP' -ForegroundColor Cyan; Write-Host 'This CMD stage does not require Python.' -ForegroundColor Gray; Write-Host ''", _
        "  Remove-Item -LiteralPath $markerPath,$pythonMarkerPath -Force -ErrorAction SilentlyContinue; Remove-Item -LiteralPath $zipPath -Force -ErrorAction SilentlyContinue", _
        "  if(Test-Path -LiteralPath $extractRoot){Remove-Item -LiteralPath $extractRoot -Recurse -Force}; New-Item -ItemType Directory -Path $extractRoot -Force | Out-Null", _
        "  Write-Step 'Step 1 of 4: Downloading the latest GitHub repository ZIP.'", _
        "  $githubUrl='https://api.github.com/repos/techknowdgeteam/HarvhubAI/zipball'; Download-ToFile $githubUrl $zipPath 'HarvHub AI GitHub ZIP'", _
        "  Write-Step 'Step 2 of 4: Extracting and validating the repository.'", _
        "  Add-Type -AssemblyName System.IO.Compression.FileSystem; $archive=[System.IO.Compression.ZipFile]::OpenRead($zipPath); try{if($archive.Entries.Count -lt 5){throw 'The GitHub ZIP contains too few files.'}}finally{$archive.Dispose()}; [System.IO.Compression.ZipFile]::ExtractToDirectory($zipPath,$extractRoot)", _
        "  $builder=Get-ChildItem -LiteralPath $extractRoot -Recurse -File -Filter 'Build_HarvHubProgram.pyw' | Select-Object -First 1; if(!$builder){throw 'GitHub repository is missing Build_HarvHubProgram.pyw.'}; $project=$builder.Directory.FullName", _
        "  $manager=Join-Path $project 'harvcore_manager.py'; if(!(Test-Path -LiteralPath $manager)){throw 'GitHub repository is missing harvcore_manager.py.'}; $payload=Join-Path $project 'payload'; New-Item -ItemType Directory -Path $payload -Force | Out-Null", _
        "  $sourceNames=@('mysqlremote.py','brokerdetails.py','brokercharts.py','programme_training.py','programmesignals_trading.py','signals_provision_challenge.py','ip.py','notification_email.py','notification_service.py')", _
        "  foreach($name in $sourceNames) {", _
        "    $target = Join-Path $payload $name", _
        "    $base = [System.IO.Path]::GetFileNameWithoutExtension($name)", _
        "    $ext = [System.IO.Path]::GetExtension($name)", _
        "    $pattern = '^' + [regex]::Escape($base) + '(?:\s*\([^)]*\))?$'", _
        "    $rootSource = Get-ChildItem -LiteralPath $project -File | Where-Object { $_.Extension -ieq $ext -and ($_.BaseName -ieq $base -or $_.BaseName -match $pattern) } | Sort-Object @{Expression={if($_.Name -ieq $name){0}else{1}}},FullName | Select-Object -First 1", _
        "    if ($rootSource) { Copy-Item -LiteralPath $rootSource.FullName -Destination $target -Force }", _
        "    elseif (!(Test-Path -LiteralPath $target)) {", _
        "      $found = Get-ChildItem -LiteralPath $project -Recurse -File | Where-Object { $_.Extension -ieq $ext -and ($_.BaseName -ieq $base -or $_.BaseName -match $pattern) -and $_.FullName -notmatch '\\\\(build|release|dist|\\.build-venv|\\.git)\\\\' } | Sort-Object @{Expression={if($_.Name -ieq $name){0}else{1}}},FullName | Select-Object -First 1", _
        "      if (!$found) { throw ('Required Python source is missing: ' + $name) }", _
        "      Copy-Item -LiteralPath $found.FullName -Destination $target -Force", _
        "    }", _
        "    if (!(Test-Path -LiteralPath $target) -or (Get-Item -LiteralPath $target).Length -lt 100) { throw ('Python source is incomplete: ' + $name) }", _
        "  }", _
        "  $requirements=Join-Path $payload 'requirements-runtime.txt'; if(!(Test-Path -LiteralPath $requirements)){$requiredText=@('# HarvHub AI runtime requirements','MetaTrader5','pytz','tzdata','requests','urllib3','selenium','beautifulsoup4','webdriver-manager') -join [Environment]::NewLine; [IO.File]::WriteAllText($requirements,$requiredText,[Text.Encoding]::ASCII)}", _
        "  if(!$clickedVbs -or !(Test-Path -LiteralPath $clickedVbs)){throw 'The standalone HarvHubAI.vbs file is no longer accessible.'}; Copy-Item -LiteralPath $clickedVbs -Destination (Join-Path $project 'HarvHubAI.vbs') -Force; Copy-Item -LiteralPath $clickedVbs -Destination (Join-Path $project 'Build_HarvHubProgram.vbs') -Force", _
        "  foreach($required in @('Build_HarvHubProgram.pyw','harvcore_manager.py','HarvHubAI.vbs','Build_HarvHubProgram.vbs')){if(!(Test-Path -LiteralPath (Join-Path $project $required))){throw ('Required installer file is missing: '+$required)}}", _
        "  [IO.File]::WriteAllText($markerPath,$project,[Text.Encoding]::Unicode); Write-Step 'GitHub archive extracted and all nine Python source files validated.'", _
        "  Write-Step 'Step 3 of 4: Checking for a compatible 64-bit Python runtime with Tkinter.'", _
        "  $python=Find-CompatiblePython", _
        "  if (!$python) {", _
        "    Write-Step 'Python not found. Downloading verified Python 3.13.17 x64.'", _
        "    $pythonInstaller = Join-Path $cacheRoot 'python-3.13.17-amd64.exe'", _
        "    $pythonUrl = 'https://www.python.org/ftp/python/3.13.17/python-3.13.17-amd64.exe'", _
        "    Download-ToFile $pythonUrl $pythonInstaller 'Python 3.13.17 x64'", _
        "    $expected = '5da012101ba2e17113a9a1cd901e9ce1441086f2b7ecb5de06ec94163899a190'", _
        "    $actual = (Get-FileHash -LiteralPath $pythonInstaller -Algorithm SHA256).Hash.ToLowerInvariant()", _
        "    if ($actual -ne $expected) { throw 'Python installer SHA-256 verification failed.' }", _
        "    $sig = Get-AuthenticodeSignature -FilePath $pythonInstaller", _
        "    if ($sig.Status -ne 'Valid' -or $sig.SignerCertificate.Subject -notmatch 'Python Software Foundation') { throw ('Python installer signature verification failed: ' + $sig.Status + ' ' + $sig.SignerCertificate.Subject) }", _
        "    $targetPython = Join-Path $env:LOCALAPPDATA 'Programs\Python\Python313'", _
        "    $pythonArgs = '/quiet InstallAllUsers=0 Include_launcher=1 Include_tcltk=1 Include_pip=1 Include_test=0 Shortcuts=0 PrependPath=0 AssociateFiles=0 TargetDir=""' + $targetPython + '""'", _
        "    $installerProcess = Start-Process -FilePath $pythonInstaller -ArgumentList $pythonArgs -PassThru -Wait", _
        "    if ($installerProcess.ExitCode -notin @(0,3010)) { throw ('Python installer failed with exit code ' + $installerProcess.ExitCode) }", _
        "    $python = Join-Path $targetPython 'python.exe'", _
        "  }", _
        "  if(!(Test-Path -LiteralPath $python)){throw ('Python executable was not found after setup: '+$python)}; $test=& $python -c 'import sys,struct,tkinter; print(sys.version_info.major,sys.version_info.minor,struct.calcsize(chr(80))*8)' 2>$null; if($LASTEXITCODE -ne 0 -or (($test -join ' ').Trim() -notmatch '^3\s+13\s+64$')){throw 'Python failed the 64-bit and Tkinter runtime check.'}", _
        "  $pythonw=Join-Path (Split-Path -Parent $python) 'pythonw.exe'; if(!(Test-Path -LiteralPath $pythonw)){throw 'pythonw.exe is missing; Tk desktop support is unavailable.'}; [IO.File]::WriteAllText($pythonMarkerPath,$pythonw,[Text.Encoding]::Unicode)", _
        "  Write-Step ('Python runtime verified: '+$python); Write-Step 'Step 4 of 4: GitHub source and Python are ready.'; Write-Host ''; Write-Host 'Bootstrap complete. HarvHub AI desktop build window will now open.' -ForegroundColor Green", _
        "  exit 0", _
        "} catch { Write-Host ''; Write-Host ('HARVHUB AI BOOTSTRAP FAILED: '+($_ | Out-String).Trim()) -ForegroundColor Red; exit 1 }" _
    ), vbCrLf)
End Function

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
        "$script:zipPath = Join-Path $script:cacheRoot 'HarvHubAI-latest.zip'", _
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
        "    Add-UiLog('Downloading ' + $Label + '.')", _
        "    $request = [System.Net.HttpWebRequest]::Create($Url)", _
        "    $request.Method = 'GET'", _
        "    $request.UserAgent = 'HarvHubAI-Installer'", _
        "    $request.Timeout = 120000" _
    ), vbCrLf)
End Function

Function PSChunk12()
    PSChunk12 = Join(Array( _
        "    $request.ReadWriteTimeout = 120000", _
        "    $request.AllowAutoRedirect = $true", _
        "    $asyncResponse = $request.BeginGetResponse($null, $null)", _
        "    while (!$asyncResponse.IsCompleted) {", _
        "        Pump-Ui", _
        "        Start-Sleep -Milliseconds 100", _
        "    }", _
        "    $response = $request.EndGetResponse($asyncResponse)", _
        "    $inputStream = $null", _
        "    $outputStream = $null", _
        "    try {", _
        "        $total = [long]$response.ContentLength", _
        "        $inputStream = $response.GetResponseStream()", _
        "        $outputStream = [System.IO.File]::Create($Destination)" _
    ), vbCrLf)
End Function

Function PSChunk13()
    PSChunk13 = Join(Array( _
        "        $buffer = New-Object byte[] 65536", _
        "        [long]$received = 0", _
        "        while ($true) {", _
        "            $asyncRead = $inputStream.BeginRead($buffer, 0, $buffer.Length, $null, $null)", _
        "            while (!$asyncRead.IsCompleted) {", _
        "                Pump-Ui", _
        "                Start-Sleep -Milliseconds 50", _
        "            }", _
        "            $count = $inputStream.EndRead($asyncRead)", _
        "            if ($count -le 0) { break }", _
        "            $outputStream.Write($buffer, 0, $count)", _
        "            $received += $count", _
        "            if ($total -gt 0) {", _
        "                $percent = [int][Math]::Min(100, ($received * 100.0 / $total))" _
    ), vbCrLf)
End Function

Function PSChunk14()
    PSChunk14 = Join(Array( _
        "                $script:progress.Style = 'Blocks'", _
        "                $script:progress.Value = $percent", _
        "                $script:statusLabel.Text = $Label + ' - ' + $percent + '%'", _
        "            } else {", _
        "                $script:progress.Style = 'Marquee'", _
        "                $script:progress.MarqueeAnimationSpeed = 25", _
        "                $script:statusLabel.Text = $Label + ' - downloading'", _
        "            }", _
        "            Pump-Ui", _
        "        }", _
        "        $outputStream.Flush()", _
        "    } finally {", _
        "        if ($outputStream) { $outputStream.Dispose() }", _
        "        if ($inputStream) { $inputStream.Dispose() }" _
    ), vbCrLf)
End Function

Function PSChunk15()
    PSChunk15 = Join(Array( _
        "        $response.Dispose()", _
        "    }", _
        "    if (!(Test-Path -LiteralPath $Destination) -or (Get-Item -LiteralPath $Destination).Length -lt 10240) {", _
        "        throw ($Label + ' download is empty or incomplete.')", _
        "    }", _
        "    $script:progress.Style = 'Blocks'", _
        "    $script:progress.Value = 100", _
        "    Add-UiLog($Label + ' download completed.')", _
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
        "    $githubUrl = 'https://api.github.com/repos/techknowdgeteam/HarvhubAI/zipball'", _
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
        "    $sourceNames = @('mysqlremote.py','brokerdetails.py','brokercharts.py','programme_training.py','programmesignals_trading.py','signals_provision_challenge.py','ip.py','notification_email.py','notification_service.py')", _
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
        "        $requirementsText = @('# HarvHub AI runtime requirements','MetaTrader5','pytz','tzdata','requests','urllib3','selenium','beautifulsoup4','webdriver-manager') -join [Environment]::NewLine", _
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

Function IsAdministrator()
    Dim result
    result = sh.Run("powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden -Command ""$p=New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent()); if($p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)){exit 0}else{exit 1}""", 0, True)
    IsAdministrator = (result = 0)
End Function

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

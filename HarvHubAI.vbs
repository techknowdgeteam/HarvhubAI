Option Explicit

Dim fso, sh, shellApp, processEnv
Dim base, clickedBase, latestProjectRoot, latestProjectStamp, downloadsPath
Dim installRoot, installedExe, builderScript
Dim pf, pf86, la, pythonwCandidates, pythonwPath
Dim i, candidate, adminResult, argsElevated, startArgs
Dim appRunning, projectReady

Set fso = CreateObject("Scripting.FileSystemObject")
Set sh = CreateObject("WScript.Shell")
Set shellApp = CreateObject("Shell.Application")

clickedBase = fso.GetParentFolderName(WScript.ScriptFullName)
base = clickedBase
installRoot = "C:\xampp\htdocs\harvcore"
installedExe = fso.BuildPath(installRoot, "HarvHubAI.exe")

pf = sh.ExpandEnvironmentStrings("%ProgramFiles%")
pf86 = sh.ExpandEnvironmentStrings("%ProgramFiles(x86)%")
la = sh.ExpandEnvironmentStrings("%LOCALAPPDATA%")

' Elevate once so the EXE and HarvCore sources can be replaced safely.
argsElevated = False
If WScript.Arguments.Count > 0 Then
    If LCase(CStr(WScript.Arguments(0))) = "/elevated" Then argsElevated = True
End If
If Not argsElevated Then
    adminResult = sh.Run("powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden -Command ""$p=New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent()); if($p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)){exit 0}else{exit 1}""", 0, True)
    If adminResult <> 0 Then
        shellApp.ShellExecute "wscript.exe", """" & WScript.ScriptFullName & """ /elevated", clickedBase, "runas", 1
        WScript.Quit 0
    End If
End If

' Prefer the newest complete HarvHub project in Downloads, not necessarily the
' folder that contains the VBS being clicked.
downloadsPath = fso.BuildPath(sh.ExpandEnvironmentStrings("%USERPROFILE%"), "Downloads")
latestProjectRoot = ""
latestProjectStamp = 0
If fso.FolderExists(downloadsPath) Then
    Call ScanLatestProjects(fso.GetFolder(downloadsPath), 0, 0)
End If
If latestProjectRoot <> "" Then base = latestProjectRoot

builderScript = fso.BuildPath(base, "Build_HarvHubProgram.pyw")
projectReady = fso.FileExists(fso.BuildPath(base, "harvcore_manager.py")) _
    And fso.FileExists(builderScript) _
    And fso.FileExists(fso.BuildPath(base, "payload\requirements-runtime.txt")) _
    And fso.FileExists(fso.BuildPath(base, "payload\brokerdetails.py")) _
    And fso.FileExists(fso.BuildPath(base, "HarvHubAI.vbs"))
If Not projectReady Then
    MsgBox "A complete HarvHub AI project could not be found in Downloads or beside this launcher." & vbCrLf & vbCrLf & _
           "Extract the latest complete HarvHub AI ZIP, then click HarvHubAI.vbs again.", _
           vbCritical, "HarvHub AI"
    WScript.Quit 2
End If

' Do not replace an EXE or start a duplicate bootstrap while the manager is open.
appRunning = IsInstalledHarvHubRunning(installedExe)
If appRunning Then
    MsgBox "HarvHub AI is currently open." & vbCrLf & vbCrLf & _
           "Close HarvHub AI normally, then click this launcher again. The latest project in Downloads will rebuild the application and update the HarvCore files.", _
           vbInformation, "HarvHub AI update"
    WScript.Quit 3
End If

If Not fso.FolderExists(installRoot) Then Call EnsureFolderTree(installRoot)

' The GUI builder needs a 64-bit Python 3.10–3.14 interpreter on this computer.
pythonwCandidates = Array( _
    pf & "\Python314\pythonw.exe", _
    pf & "\Python313\pythonw.exe", _
    pf & "\Python312\pythonw.exe", _
    pf & "\Python311\pythonw.exe", _
    pf & "\Python310\pythonw.exe", _
    pf86 & "\Python314\pythonw.exe", _
    pf86 & "\Python313\pythonw.exe", _
    la & "\Programs\Python\Python314\pythonw.exe", _
    la & "\Programs\Python\Python313\pythonw.exe", _
    la & "\Programs\Python\Python312\pythonw.exe", _
    la & "\Programs\Python\Python311\pythonw.exe", _
    la & "\Programs\Python\Python310\pythonw.exe" _
)
pythonwPath = ""
For i = 0 To UBound(pythonwCandidates)
    candidate = CStr(pythonwCandidates(i))
    If fso.FileExists(candidate) Then
        pythonwPath = candidate
        Exit For
    End If
Next
If pythonwPath = "" Then
    If fso.FileExists(installedExe) Then
        shellApp.ShellExecute installedExe, "--repair", installRoot, "runas", 1
        MsgBox "The installed HarvHub AI app was opened in Setup / Repair." & vbCrLf & vbCrLf & _
               "To rebuild from the latest Downloads project, install a 64-bit Python 3.10–3.14 build runtime and click this launcher again.", _
               vbExclamation, "HarvHub AI"
        WScript.Quit 4
    End If
    MsgBox "HarvHub AI requires a 64-bit Python 3.10–3.14 build runtime." & vbCrLf & _
           "Install Python 3.12 or 3.13 x64 with Tcl/Tk, then click this launcher again.", _
           vbExclamation, "HarvHub AI"
    WScript.Quit 4
End If

' Always build from the selected latest project, replace the installed EXE,
' update C:\xampp\htdocs\harvcore\python, refresh the desktop launcher, then
' transition the same GUI window into HarvHub AI. No CMD or build button.
Set processEnv = sh.Environment("PROCESS")
processEnv("HARVHUB_SOURCE_ROOT") = base
processEnv("HARVCORE_ROOT") = installRoot
startArgs = """" & pythonwPath & """ """ & builderScript & """ --install --force-rebuild"
sh.Run startArgs, 0, False
WScript.Quit 0


Sub ScanLatestProjects(ByVal folder, ByVal depth, ByVal inheritedStamp)
    Dim child, folderName, branchStamp
    If depth > 7 Then Exit Sub
    On Error Resume Next
    folderName = LCase(CStr(folder.Name))
    branchStamp = inheritedStamp
    If InStr(folderName, "harvhub") > 0 Then
        If CDbl(folder.DateLastModified) > branchStamp Then branchStamp = CDbl(folder.DateLastModified)
        Call EvaluateProjectFolder(folder, branchStamp)
    End If
    For Each child In folder.SubFolders
        If Not IsExcludedFolder(CStr(child.Name)) Then
            Call ScanLatestProjects(child, depth + 1, branchStamp)
        End If
    Next
    On Error GoTo 0
End Sub


Sub EvaluateProjectFolder(ByVal folder, ByVal inheritedStamp)
    Dim payloadPath, sourceStamp, filePath, payloadFolder, item
    On Error Resume Next
    If Not fso.FileExists(fso.BuildPath(folder.Path, "harvcore_manager.py")) Then Exit Sub
    If Not fso.FileExists(fso.BuildPath(folder.Path, "Build_HarvHubProgram.pyw")) Then Exit Sub
    If Not fso.FileExists(fso.BuildPath(folder.Path, "HarvHubAI.vbs")) Then Exit Sub
    payloadPath = fso.BuildPath(folder.Path, "payload")
    If Not fso.FolderExists(payloadPath) Then Exit Sub
    If Not fso.FileExists(fso.BuildPath(payloadPath, "requirements-runtime.txt")) Then Exit Sub
    If Not fso.FileExists(fso.BuildPath(payloadPath, "brokerdetails.py")) Then Exit Sub

    sourceStamp = inheritedStamp
    If CDbl(folder.DateLastModified) > sourceStamp Then sourceStamp = CDbl(folder.DateLastModified)
    For Each filePath In Array( _
        fso.BuildPath(folder.Path, "harvcore_manager.py"), _
        fso.BuildPath(folder.Path, "Build_HarvHubProgram.pyw"), _
        fso.BuildPath(folder.Path, "HarvHubAI.vbs"), _
        fso.BuildPath(folder.Path, "README.md") _
    )
        If fso.FileExists(CStr(filePath)) Then
            If CDbl(fso.GetFile(CStr(filePath)).DateLastModified) > sourceStamp Then
                sourceStamp = CDbl(fso.GetFile(CStr(filePath)).DateLastModified)
            End If
        End If
    Next
    Set payloadFolder = fso.GetFolder(payloadPath)
    For Each item In payloadFolder.Files
        If CDbl(item.DateLastModified) > sourceStamp Then sourceStamp = CDbl(item.DateLastModified)
    Next
    If sourceStamp > latestProjectStamp Then
        latestProjectStamp = sourceStamp
        latestProjectRoot = folder.Path
    End If
    On Error GoTo 0
End Sub


Function IsExcludedFolder(ByVal name)
    Dim lower
    lower = LCase(name)
    IsExcludedFolder = (lower = ".build-venv" Or lower = "build" Or lower = "release" _
        Or lower = "dist" Or lower = ".git" Or lower = "__pycache__" _
        Or lower = "node_modules" Or lower = ".venv" Or lower = "venv" _
        Or lower = "site-packages" Or lower = "cache")
End Function


Function IsInstalledHarvHubRunning(ByVal targetPath)
    Dim wmi, processes, proc, executablePath, commandLine, processName
    IsInstalledHarvHubRunning = False
    On Error Resume Next
    Set wmi = GetObject("winmgmts:\\.\root\cimv2")
    Set processes = wmi.ExecQuery("SELECT Name,ExecutablePath,CommandLine FROM Win32_Process WHERE Name='HarvHubAI.exe' OR Name='pythonw.exe' OR Name='python.exe'")
    For Each proc In processes
        executablePath = ""
        commandLine = ""
        processName = ""
        Err.Clear
        executablePath = CStr(proc.ExecutablePath)
        commandLine = CStr(proc.CommandLine)
        processName = LCase(CStr(proc.Name))
        If Err.Number = 0 Then
            If (processName = "harvhubai.exe" And LCase(executablePath) = LCase(targetPath)) _
                Or ((processName = "pythonw.exe" Or processName = "python.exe") _
                    And InStr(1, LCase(commandLine), "build_harvhubprogram.pyw", vbTextCompare) > 0) Then
                IsInstalledHarvHubRunning = True
                Exit For
            End If
        End If
    Next
    On Error GoTo 0
End Function


Sub EnsureFolderTree(ByVal folderPath)
    Dim parentPath
    If fso.FolderExists(folderPath) Then Exit Sub
    parentPath = fso.GetParentFolderName(folderPath)
    If Len(parentPath) > 0 And Not fso.FolderExists(parentPath) Then EnsureFolderTree parentPath
    If Not fso.FolderExists(folderPath) Then fso.CreateFolder folderPath
End Sub

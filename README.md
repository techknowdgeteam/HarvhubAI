# HarvHub AI - GitHub-first Windows VPS installer

## The only file needed on a new VPS

Copy **HarvHubAI.vbs** to the new Windows VPS and double-click it. Do not copy the Python files first.

The standalone VBS launcher performs this order:

1. Downloads the latest ZIP from `https://api.github.com/repos/techknowdgeteam/HarvhubAI/zipball`.
2. Extracts the archive under `%LOCALAPPDATA%\HarvHubAI\Bootstrap\source`.
3. Finds the builder and manager, then normalizes flat Python uploads into the `payload` directory required by the project.
4. Validates all nine Python source modules and the runtime requirements file.
5. Requests administrator permission only after the GitHub source has been downloaded and validated.
6. Reuses compatible 64-bit Python or installs verified Python 3.13.17 x64.
7. Runs the downloaded builder, which creates HarvHubAI.exe, installs the sources, creates shortcuts, and opens the manager.

The launcher is ASCII-only VBScript without a UTF-8 BOM, to avoid Windows Script Host's "Invalid character" parser error. `Build_HarvHubProgram.vbs` is an identical compatibility alias.

## Installation paths

- Application root: `C:\xampp\htdocs\harvhubAI\`
- Deployed Python programs: `C:\xampp\htdocs\harvhubAI\python\`
- MT5 template: `C:\xampp\htdocs\harvhubAI\mt5\MetaTrader 5\`
- Logs: `C:\xampp\htdocs\harvhubAI\logs\`
- Manager source/backups: `C:\xampp\htdocs\harvhubAI\.manager\`

The Python source files in this release use the `harvhubAI` installation folder, not the former `harvcore` folder.

## GitHub repository requirements

Repository: `https://github.com/techknowdgeteam/HarvhubAI`

The repository must be public for the unauthenticated ZIP download. The root (or a project folder below it) must contain:

- `Build_HarvHubProgram.pyw`
- `harvcore_manager.py`
- Python files: `mysqlremote.py`, `brokerdetails.py`, `brokercharts.py`, `programme_training.py`, `signals_trading.py`, `signals_provision_challenge.py`, `ip.py`, `notification_email.py`, `notification_service.py`

The Python files may be at the repository root or under `payload`. When both exist, root-level files take priority and refresh `payload`, so flat GitHub uploads replace older nested copies. The bootstrap also recognizes duplicate names like `brokercharts(3).py`. If the requirements file is missing, the bootstrap creates the default runtime dependency list. It replaces the downloaded VBS files with the exact standalone launcher that was clicked.

## Installed app versus explicit updater

- After setup, the Desktop and Start Menu **HarvHub AI** shortcuts target `C:\xampp\htdocs\harvhubAI\HarvHubAI.exe` directly. The installed EXE checks the existing Python 3.13 runtime, Python modules, Chrome, MT5 template, and deployed service files. When those dependencies are present, it opens the manager workspace immediately and does not download GitHub sources or reinstall dependencies.
- If a required dependency is genuinely missing, the manager enters its repair/setup flow and installs only what is missing.
- Double-clicking **HarvHubAI.vbs** explicitly starts the GitHub download, validation, update/build, and setup path. `Build_HarvHubProgram.vbs` remains a compatibility alias for that explicit updater.
- Updating should be run with the manager and services closed, because the VBS updater replaces installed application files.
## Security

Use a public repository only for source code that contains no passwords, API keys, private certificates, database credentials, or other secrets. Rotate any credential that was ever committed publicly. The Python 3.13.17 x64 installer is downloaded from python.org and checked against its published SHA-256 digest and Authenticode signature before installation.

## Validation scope

Python source syntax, ZIP integrity, project normalization, bootstrap ordering, and ASCII-only VBS encoding are validated in the build environment. Actual Windows Script Host, UAC, PowerShell extraction, Python installation, and VPS execution must still be verified on Windows.


## Visible single-file VPS bootstrap

Copy only `HarvHubAI.vbs` to a fresh VPS and run it. After the Windows elevation prompt, a visible **HarvHub AI - Installer** window remains open while it downloads the GitHub repository ZIP, extracts and validates every required Python file, and checks or installs the 64-bit Python build runtime. The PowerShell console itself is hidden, but the WinForms installer is visible; downloading and Python preparation are not silent/background-only stages. When that stage succeeds, the installer window hands off to the normal HarvHub AI builder progress window and then the manager.

The launcher downloads `https://api.github.com/repos/techknowdgeteam/HarvhubAI/zipball`. The repository must be public for this unauthenticated bootstrap.

The installed root is `C:\xampp\htdocs\harvhubAI`. The Python module filename `harvcore_manager.py` is intentionally retained because it is imported by the builder; it is a file/module name, not an installation folder.


## First-run fallback without Python

The standalone `HarvHubAI.vbs` checks for a compatible 64-bit Python/Tkinter runtime. If one is already available, it uses the visible HarvHub AI WinForms bootstrap window. If no compatible runtime exists, it opens a visible CMD window and runs the Python-free PowerShell bootstrap from that console. The CMD stage downloads and extracts the latest GitHub repository, validates the nine Python files, installs/verifies Python 3.13.17 x64, and then hands off to the normal Python build window. The initial download/install stage does not depend on Python.

## MetaTrader 5 setup recovery

HarvHub AI launches the signed official MetaQuotes installer using its documented `/auto` and `/path` switches. It verifies the actual `mt5\MetaTrader 5\terminal64.exe` output instead of relying only on the installer exit code. If the automatic install does not create that file, the official interactive installer opens and asks the user to choose the exact HarvHub AI MT5 template folder.


## Normal launch without an elevation prompt

The desktop EXE is built without PyInstaller's `--uac-admin` option, so it uses
the normal current-user execution level. The VBS launcher and manager do not
automatically relaunch themselves with `runas`. Opening HarvHub AI therefore
runs in the signed-in Windows user's security context and does not request
administrator permission merely to display the application window.

Actual setup/repair operations still check for administrator privileges before
installing system-wide components or changing protected locations. If Windows
denies access during those operations, run the explicit setup/repair operation
with administrator rights. UAC cannot be silently approved by an unsigned app.


## Applying this source update

1. Replace these four project-root files in the HarvHub AI GitHub repository:
   `HarvHubAI.vbs`, `Build_HarvHubProgram.vbs`,
   `Build_HarvHubProgram.pyw`, and `harvcore_manager.py`.
2. Also replace the same files in your local HarvHub source/project folder.
3. Run the updated `HarvHubAI.vbs` to download the updated project and rebuild
   the Windows desktop executable.
4. The newly built `release\HarvHubAI.exe` is the executable that must be
   installed/used by the Desktop and Start Menu shortcuts.

The currently supplied `HarvHubAI.exe` was built with the old
`--uac-admin` setting. It will continue to request elevation until it is
replaced by a newly built executable. This source ZIP intentionally does not
include that old EXE.

This patch addresses the UAC dialog that says an app from an **unknown
publisher** wants to make changes to the device. It does not suppress a separate
SmartScreen warning such as **Windows protected your PC**; that warning is a
different Windows reputation/signing mechanism.

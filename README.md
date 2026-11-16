# HarvHub AI — global-header and updater fix

HarvHub AI is a Windows desktop application for preparing the HarvCore Python
runtime and MetaTrader 5 installation, then controlling HarvCore programs from
one responsive white-and-gold window.

## What to click

1. Extract this complete project ZIP into Downloads.
2. Double-click **`HarvHubAI.vbs`**. `Build_HarvHubProgram.vbs` is an alias.
3. Approve the Windows administrator prompt.
4. Do not open CMD and do not click a separate build button.

The launcher scans the current Windows user's **Downloads** folder for complete
project folders whose folder name contains `HarvHub`. It selects the newest
matching project using the folder and source-file modified dates. That selected
project takes priority over the folder from which the VBS was clicked.

Every launcher click starts the GUI builder in forced-refresh mode. The builder:

- Builds `HarvHubAI.exe` from the selected newest Downloads project.
- Backs up and replaces `C:\xampp\htdocs\harvcore\HarvHubAI.exe`.
- Updates changed program/support files under `C:\xampp\htdocs\harvcore\python\`.
- Saves the selected complete project under
  `C:\xampp\htdocs\harvcore\.manager\source\` as a fallback.
- Refreshes Desktop and Start Menu shortcuts. These shortcuts run the installed
  VBS updater, so each click checks Downloads again and applies the newest project.
- Transitions the same GUI window from build progress into HarvHub AI. There is
  no CMD window, separate build button, or build-success modal.

If HarvHub AI is already running, close it normally before using an external
launcher. The launcher checks for both the installed EXE process and the Python
GUI-bootstrap process; it does not kill active HarvCore workers or overwrite an
EXE that Windows is using.

## Global application header

- The **HarvHub AI** application title appears once, in the top global header.
  The duplicate title is removed from the home panel and navigation brand.
- **Run All**, **Stop All**, and **Update** are placed at the right edge of that
  same global header. The old Ready indicator is removed.
- **Stop All** is hidden when no HarvCore program is running. It appears as soon
  as one or more managed programs are running and disappears when all stop.
- **Update** replaces the old Setup / Repair header action. It requires all
  managed HarvCore programs to be stopped, asks before closing the manager, then
  waits for the current manager process to exit before starting the installed
  `HarvHubAI.vbs` updater. The updater finds the newest complete HarvHub project
  in Downloads, rebuilds the application, and updates installed program files.
  Existing files are backed up by the updater.
- Global actions are not repeated on individual program pages.
- Each service page retains only its own Play / Stop / Show Console / Copy Console
  Result / Clear controls.
- Consoles are hidden by default. Show Console reveals full output; Hide Console
  collapses it. Copy Console Result copies the original output.
- Setup runs in the same window and provides **Back to Home**. Leaving an active
  setup requests a safe stop of the current installer/download step.
- Landscape uses left-edge navigation; narrow/portrait uses responsive navigation
  above the content. The palette is fixed white/cream and gold; there is no theme
  toggle.

Navigation entries:

1. MySQL Remote
2. Broker Details
3. Broker Charts
4. Programme Training
5. Programme Signals Trading
6. Signals Provision Challenge
7. Computer IP

`ip.py`, `notification_email.py`, and `notification_service.py` are silently
installed support files, not separate program pages.

## Installation destinations

- Desktop application: `C:\xampp\htdocs\harvcore\HarvHubAI.exe`
- HarvCore program files: `C:\xampp\htdocs\harvcore\python\`
- MT5 template: `C:\xampp\htdocs\harvcore\mt5\MetaTrader 5\`
- Latest project fallback: `C:\xampp\htdocs\harvcore\.manager\source\`
- Backups and deployment manifest: `C:\xampp\htdocs\harvcore\.manager\`

The installer updates the nine supplied Python program/support files and the
runtime requirements file. Changed existing files are backed up before atomic
replacement. Source hashes are recorded in `.manager\source_manifest.json`.

## Builder computer requirements

The build computer needs a 64-bit Python 3.10–3.14 installation with Tkinter/Tcl-Tk.
The customer-facing HarvHub AI runtime installs its pinned Python 3.13.17 x64
runtime, required modules, Chrome (when missing), and MetaTrader 5 (when missing).

## Security and diagnostics

- Full console and setup reports are hidden unless explicitly shown/copied.
- Saved diagnostic records use Windows DPAPI for the current Windows user.
- Clipboard copies intentionally contain original plaintext diagnostic output.
- Python files are importable `.py` sources; they are not cryptographically encrypted.
- The application and updater require Windows administrator privileges to install
  system software and write under `C:\xampp\htdocs\harvcore`.

## Validation status

Python syntax checks passed. The Tk UI smoke test confirmed one visible global title,
seven navigation items, global-only Run All / Stop All / Update controls, conditional
Stop All visibility, and responsive layout at wide and narrow sizes. The Update
handoff was tested with a mocked Windows Script Host and verified to wait for the
current manager PID to exit before starting the installed VBS.

The VBS launcher, Windows Script Host/WMI behavior, administrator elevation,
installer execution, and actual Windows shortcut/update behavior have not been run
on a Windows computer in this environment. Test the package on a disposable Windows
VPS before production rollout.

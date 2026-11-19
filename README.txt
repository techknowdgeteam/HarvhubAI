HARVHUB AI — STARTUP / FOCUS / ERROR-CONSOLE FIXES
==================================================

FILES
-----
HarvHubAI.vbs
    GitHub/bootstrap downloader and first-time installer launcher.

Build_HarvHubProgram.vbs
    Same launcher under the compatibility filename expected by the project.

Build_HarvHubProgram.pyw
    No-console GUI builder. The build window stays on top, writes a persistent
    builder log, and shows/copies its full error output inside the window.

harvcore_manager.py
    Main HarvHub AI window and setup/repair manager. Starts maximized and
    always-on-top; setup output and full tracebacks are displayed in a
    copyable Setup Console.

INSTALL / USE
-------------
1. Extract all four files into the SAME folder.
2. Run HarvHubAI.vbs.
3. The downloader fetches the project as usual. Before the builder starts, it
   overlays the two supplied local fixes (Build_HarvHubProgram.pyw and
   harvcore_manager.py) onto the downloaded project.
4. The builder rebuilds HarvHubAI.exe from those updated sources and deploys it
   to the normal HarvHub AI installation directory.

BEHAVIOUR
---------
- The download installer, builder/setup window, and HarvHub AI window are
  configured as topmost foreground windows.
- The main HarvHub AI window opens maximized.
- The normal single-instance cleanup no longer creates or displays a CMD file
  window. The builder is launched through pythonw.exe, without a console host.
- Setup/build/service output is captured by the application's own consoles.
  Setup/build failures include their traceback and can be copied from the UI.
- CMD is reserved for the final fallback only, when the application cannot
  create its own startup-error window. Ordinary startup does not open CMD.

IMPORTANT
---------
The previously supplied HarvHubAI.exe binary is not included or patched here.
The supplied builder rebuilds a fresh HarvHubAI.exe on the Windows computer
when HarvHubAI.vbs is run. I validated Python syntax and launcher consistency
in this environment; the Windows GUI/foreground behaviour still needs to be
confirmed on the target Windows machine.

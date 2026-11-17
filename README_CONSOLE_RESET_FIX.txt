HARVHUB AI - COMPLETE CONSOLE + RESET FIX

This package contains the complete desktop manager source and all six runnable
service scripts plus their support modules and build/launch scripts.

WHAT CHANGED
1. Every runnable service is launched as its own child process. Its stdout and
   stderr are captured and routed only to that program's own Show Console view.
2. The manager uses Python's unbuffered mode and UTF-8 output so progress lines
   are forwarded as soon as the service writes them.
3. The manager keeps each program's own output buffer and protected log file.
   Opening Show Console replays that program's own captured output.
4. Service startup/cycle banners are explicitly flushed in Broker Details,
   Broker Charts and Programme Signals Trading. The other services already emit
   their own cycle/request progress, and MySQL Remote reports incoming requests.
5. The output watchdog reports process health when a service is alive but has
   emitted no output. It labels this as a health notice, not fabricated work.
6. Reset waits for Run All to finish cancelling before taking the final process
   snapshot, verifies that managed services are stopped, then performs orphan
   service and HarvHub-owned MT5 cleanup before returning to Home.
7. Reset no longer reports success if a managed service remains alive.

INSTALL
- Back up the existing HarvHub AI project folder.
- Extract this ZIP into the project root, preserving the included filenames.
- Replace the matching source files in the project with these versions.
- Run HarvHubAI.vbs / the normal project build launcher to rebuild and deploy
  HarvHubAI.exe. Updating Python sources alone does not update an existing EXE.

IMPORTANT
- MySQL Remote is a request-driven database listener. While idle, its console
  should say it is listening/waiting; it prints detailed activity when other
  programs send database requests. It cannot show SQL activity when no client
  is making requests.
- A service with no eligible database records will report that result and wait
  for its next configured scan. This is real idle/empty-work status, not a
  console capture failure.
- The package was syntax-compiled in this environment. The Windows GUI, Windows
  process-tree termination, live database, Chrome and MT5 integrations must be
  verified on the target Windows machine.

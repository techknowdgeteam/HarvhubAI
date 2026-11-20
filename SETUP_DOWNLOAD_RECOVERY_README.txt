HARVHUB AI — GITHUB DOWNLOAD RECOVERY PATCH
===========================================

Files in this patch
-------------------
HarvHubAI.vbs
Build_HarvHubProgram.vbs

Both VBS launchers contain the same embedded PowerShell bootstrap and are kept
byte-identical. Replace both files in the project/source folder with the
versions in this ZIP.

What was fixed
--------------
1. Removed the 24 KB/s Codeload speed cutoff. Slow transfer speed by itself no
   longer aborts a download while bytes are arriving.
2. Increased the response-header wait allowance to 180 seconds.
3. Increased the no-data idle allowance to 5 minutes. A connection that keeps
   delivering bytes slowly is allowed to continue.
4. Increased the per-attempt archive transfer limit to 20 minutes and the
   per-route retry window to 2 hours.
5. Preserves useful partial archive data after an interrupted transfer.
6. Retries the same GitHub route up to eight times and requests the remaining
   byte range using HTTP Range when the server supports it.
7. If the server does not honor Range, the downloader safely restarts that
   attempt from byte zero rather than appending a second full archive.
8. Validates the repository ZIP before accepting it. If a range request returns
   HTTP 416, the retained archive is accepted only when ZIP integrity validates.
9. Applies retry/resume behavior to individual raw GitHub source downloads and
   the Python installer download as well.
10. Every failed attempt reports the URL, current phase, network error, bytes
    already received, and whether a partial file is being retained.

How to install
--------------
1. Extract this ZIP.
2. Replace BOTH HarvHubAI.vbs and Build_HarvHubProgram.vbs in your source folder.
3. Run HarvHubAI.vbs again.

No Python modules, MT5 files, Chrome files, application settings, or service
logic are changed by this patch. The GitHub download itself still requires the
VPS to be able to reach GitHub. If GitHub is unreachable or the connection
remains idle for five minutes, the launcher reports the specific failure.

Validation performed here
-------------------------
- Confirmed all 26 embedded PowerShell chunk functions exist exactly once.
- Confirmed no 24 KB/s abort logic remains.
- Confirmed the two VBS files are byte-identical.
- Confirmed ZIP package integrity.

Windows Script Host / PowerShell execution must still be confirmed on the VPS.

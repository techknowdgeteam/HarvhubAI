HARVHUB PYTHON EMAIL RELAY PATCH
================================

CONTENTS
--------
1. notification_email.py
   Modified Python sender. Python workers now send email requests to the
   hosted PHP relay instead of calling Brevo directly.

2. notification_email_relay.php
   New authenticated endpoint. It verifies a short-lived HMAC request and
   calls the existing PHP sendNotificationEmail() function.

WHAT DOES NOT NEED CHANGING
---------------------------
- notification_service.py
- mysqlremote.py
- programme_training.py
- brokerdetails.py
- brokercharts.py
- programmesignals_trading.py
- signals_provision_challenge.py
- other workers that already call notification_service.py

INSTALLATION
------------
A. HOSTING SERVER
1. Upload notification_email_relay.php to the same directory as:
      mysqlconnection.php
      notification_email.php
2. Do NOT replace notification_email.php. The relay calls its existing
   sendNotificationEmail($recipient, $title, $message, $meta) function.
3. Confirm that https://harvhub.wuaze.com/notification_email_relay.php is
   reachable. A browser GET should return HTTP 405 / method_not_allowed;
   that is expected because the endpoint accepts POST only.

B. WINDOWS / PYTHON APPLICATION
1. Back up the existing notification_email.py.
2. Replace it with the notification_email.py in this ZIP in the directory
   from which the HarvHub workers import their notification modules.
   In the supplied test output, this was:
      C:\xampp\htdocs\harvhubAI\notification_email.py
   If your deployed app imports from a python subfolder instead, replace the
   module in that actual import location too; check the printed module path.
3. Restart the HarvHub Python workers so they load the new module.

RELAY URL
---------
The default endpoint is:
  https://harvhub.wuaze.com/notification_email_relay.php

If the PHP relay is hosted at another HTTPS hostname/path, set the Windows
machine environment variable HARVHUB_NOTIFICATION_RELAY_URL to the exact URL,
then restart the HarvHub application. Example PowerShell:
  [Environment]::SetEnvironmentVariable(
    'HARVHUB_NOTIFICATION_RELAY_URL',
    'https://YOUR-HOST/notification_email_relay.php',
    'User'
  )

SECURITY / REQUEST SIGNING
--------------------------
- The PC does not send the raw Brevo API key to the relay.
- Python reads the current API key using the existing DB bridge and derives
  an HMAC signing key from it.
- PHP reads that same current key from server_account and verifies the HMAC.
- Signatures expire after 180 seconds.
- The relay only accepts POST JSON and limits request bodies to 32 KB.
- HTTPS is required by the Python adapter.

TEST
----
1. Keep mysqlremote.py running in LOOP mode so the local DB bridge is active.
2. Run the existing email diagnostic/test script.
3. Expected success log from Python:
     [HarvHub Notification Email] PHP RELAY ACCEPTED EMAIL ...
4. The PHP server error log should contain the existing PHP mailer's
   BREVO SUCCESS log. If delivery fails, the Python output reports the relay
   HTTP status and generic relay error; the server error log records the
   detailed Brevo response.

IMPORTANT
---------
This changes the Brevo API request origin from the local PC to the hosted
PHP server. It does not make the PC's HTTP request disappear: the PC still
connects to the relay, but Brevo sees the hosting server as the sender of the
SMTP API request.

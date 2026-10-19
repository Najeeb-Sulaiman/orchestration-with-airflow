# Stage 2: Schedule via Cron
The engineer puts the steps on a small server's crontab.

## Cron Schedule
```text
30 * * * *   /python/path /absolute/path/to/script/ >> /absolute/path/to/logfile.log 2>&1
35 * * * *   /python/path /absolute/path/to/script/ >> /absolute/path/to/logfile.log 2>&1
40 * * * *   /python/path /absolute/path/to/script/ >> /absolute/path/to/logfile.log 2>&1
```

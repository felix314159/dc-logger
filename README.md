# dc-logger
Download + real-time sync entire Discord server content into structured SQL database, designed for LLM consumption with human-readable real-time logs.

## Disclaimer

This project is an independent, unofficial tool and is not affiliated with, endorsed by, or sponsored by Discord. It is intended to operate in accordance with Discord’s Developer Terms, Developer Policy, and applicable documentation, but compliance depends on your specific deployment and use.

Use of this software is entirely at your own risk. The authors and contributors make no warranties (express or implied) and accept no liability for any direct or indirect loss, damage, or consequence arising from use of the code, including but not limited to data exposure, incomplete or inaccurate logs, service interruptions, account enforcement actions (including suspension or bans), or any other operational, legal, or security outcome.

## Warning

Entirely vibe-coded by Codex 5.3 and 5.5 Thinking, I did not write a single line of code. It seems to work well tho, lgtm LFG

## Logging Multiple Server

You can add the same bot to multiple servers and this tool will be able
to log all servers at once. It will create a separate db file for each
logged server, and the real-time logs will show the servername for each
event if you are logging more than one server at the same time.

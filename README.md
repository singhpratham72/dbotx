# dbotx

Bulk-download tracks from a Telegram music bot ([@DeezLoad2bot](https://t.me/DeezLoad2bot)) using [Telethon](https://docs.telethon.dev). Feed it a list of Spotify/Deezer links (or track names) and it downloads them in parallel, safely and resumably.

## Features

- **Parallel downloads** – several tracks in flight at once (default 6, max 10), with replies routed to the right track.
- **Resumable** – progress is saved to `downloaded_log.json` after every track; finished tracks are skipped on restart and failed ones are retried.
- **No duplicates** – links are normalized, existing files are never re-downloaded, and only one instance can run at a time.
- **Safe writes** – files download to `.part` and are renamed only after a size check, so interrupted runs never leave broken MP3s.
- **Rate-limit aware** – randomized send throttle, and a `FloodWaitError` pauses all workers for the required time.
- **Handles bot quirks** – clicks the first inline-keyboard result for text searches, logs queue/status messages, and moves on after errors or a timeout.
- **Clean names** – files are saved as `Song Name - Artist.mp3`.

## Setup

Requires Python 3.10+ and Telegram API credentials from [my.telegram.org](https://my.telegram.org) → *API development tools*.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env        # then set TELEGRAM_API_HASH
python main.py --login      # one-time phone + code login (creates session_downloader.session)
```

`TELEGRAM_API_ID` can also be set in `.env`; it defaults to the value in `main.py`.

## Usage

Put one track per line in `songs.txt` (Spotify/Deezer link or `Artist - Title`). Blank lines are ignored, and a trailing `# Title - Artist` comment helps match bot replies:

```text
https://open.spotify.com/track/1YiqwPFyrzYMVJM7JF5AH9  # Balam Pichkari Remix - Vishal Dadlani
Daft Punk - Get Lucky
```

```bash
python main.py                  # run everything in songs.txt
python main.py --limit 3        # quick test with the first 3 entries
python main.py my_list.txt -c 8 # custom list, 8 parallel workers
```

Downloads land in `./downloads/`.

| Option | Default | Description |
| --- | --- | --- |
| `input` | `songs.txt` | Text file with one track per line |
| `-c, --concurrency` | `6` | Tracks in flight at once (1–10) |
| `--send-gap MIN MAX` | `2 4` | Random delay (s) between requests to the bot |
| `--timeout` | `90` | Seconds to wait for the bot's reply per track |
| `--skip-failed` | off | Don't retry tracks that failed earlier |
| `--limit N` | all | Only process the first N entries |
| `--login` | – | Authenticate, save the session, and exit |

Re-running the script is always safe: it picks up where it left off.

## Notes

- If the bot doesn't respond at first, open its chat once, press `/start`, and join any channel it requires.
- Direct Spotify/Deezer track links work best; text searches rely on the bot's result menu.
- Keep `.env` and `*.session` private; both are git-ignored.
- Use this only for content you have the right to download.

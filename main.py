#!/usr/bin/env python3
"""Download a list of tracks from a Telegram bot (default: @DeezLoad2bot) with Telethon.

Usage:
    python main.py [songs.txt] [-c 6] [--timeout 90] [--skip-failed] [--send-gap 2 4]

Several tracks are requested and downloaded in parallel (-c, default 6).

Completed tracks are always skipped on re-run; previously failed tracks are retried
unless --skip-failed is given.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import mimetypes
import os
import random
import re
import sys
from contextlib import contextmanager
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

import fcntl

from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from telethon import TelegramClient, events, types
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.custom import Message

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
DEFAULT_API_ID = 35737285
BOT_USERNAME = "@DeezLoad2bot"
SESSION_NAME = "session_downloader"
DOWNLOAD_DIR = Path("./downloads")
LOG_FILE = Path("./downloaded_log.json")
DEFAULT_INPUT = Path("./songs.txt")

PER_SONG_TIMEOUT = 90  # seconds to wait for the bot per attempt
DEFAULT_CONCURRENCY = 6  # tracks in flight at once
SEND_GAP = (2.0, 4.0)  # randomized global delay between any two requests sent to the bot
DOWNLOAD_TIMEOUT = 300  # seconds before a stalled file download is abandoned
MAX_CLICKS_PER_TRACK = 3  # safety cap on inline-button clicks
MAX_FLOOD_RETRIES = 3

AUDIO_EXTENSIONS = {".mp3", ".flac", ".m4a", ".ogg", ".opus", ".wav", ".aac"}
MIME_EXT_FALLBACK = {
    "audio/mpeg": ".mp3",
    "audio/mp3": ".mp3",
    "audio/flac": ".flac",
    "audio/x-flac": ".flac",
    "audio/mp4": ".m4a",
    "audio/x-m4a": ".m4a",
    "audio/ogg": ".ogg",
    "audio/wav": ".wav",
}

ERROR_RE = re.compile(
    r"not found|no results?|couldn'?t find|could not find|nothing found|"
    r"unavailable|not available|invalid|unsupported|failed|error|"
    r"something went wrong|sorry|unable to|can'?t (?:find|download)",
    re.IGNORECASE,
)

console = Console()
log = logging.getLogger("dl")


# --------------------------------------------------------------------------- #
# Results
# --------------------------------------------------------------------------- #
class TrackFailed(Exception):
    """Raised when the bot reports an error / the track cannot be fetched."""


class TrackTimeout(TrackFailed):
    """Raised when the per-song timeout expires."""


@dataclass
class Outcome:
    ok: bool
    detail: str  # file path on success, reason on failure


# --------------------------------------------------------------------------- #
# State tracking (downloaded_log.json)
# --------------------------------------------------------------------------- #
class StateLog:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.completed: dict[str, dict] = {}
        self.failed: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.completed = data.get("completed", {})
            self.failed = data.get("failed", {})
        except (json.JSONDecodeError, OSError) as exc:
            backup = self.path.with_suffix(".json.bak")
            log.warning("Could not read %s (%s); backing up to %s", self.path, exc, backup)
            self.path.replace(backup)

    def _save(self) -> None:
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps({"completed": self.completed, "failed": self.failed}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        tmp.replace(self.path)  # atomic: a Ctrl+C can't leave a half-written file

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def mark_done(self, entry: str, file: str) -> None:
        self.completed[entry] = {"file": file, "at": self._now()}
        self.failed.pop(entry, None)
        self._save()

    def mark_failed(self, entry: str, reason: str) -> None:
        self.failed[entry] = {"reason": reason, "at": self._now()}
        self._save()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def normalize_key(key: str) -> str:
    """Strip tracking params from Spotify/Deezer links so one track has exactly one key."""
    if re.match(r"https?://(open\.spotify\.com|(www\.)?deezer\.com)/", key):
        key = key.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    return key


@dataclass(frozen=True)
class Entry:
    key: str  # what is sent to the bot (link or track name); also the log key
    hint: str = ""  # optional "Title - Artists" from a trailing "# comment", used to match replies


def load_entries(path: Path) -> list[Entry]:
    if not path.exists():
        sys.exit(f"Input file not found: {path}")
    seen: set[str] = set()
    entries: list[Entry] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        key, _, hint = re.sub(r"^\s+", "", raw).partition(" #")  # trailing "  # comment" = hint
        key = normalize_key(key.strip())
        if not key or key.startswith("#") or key in seen:
            continue
        seen.add(key)
        entries.append(Entry(key, hint.strip()))
    return entries


@contextmanager
def single_instance(lock_path: Path = Path(".downloader.lock")):
    """Refuse to run twice at once (two instances would duplicate downloads)."""
    fh = open(lock_path, "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("Another instance is already running (lock held on .downloader.lock).")
    try:
        yield
    finally:
        fcntl.flock(fh, fcntl.LOCK_UN)
        fh.close()


def sanitize(name: str, max_len: int = 150) -> str:
    name = re.sub(r'[<>:"/\\|?*_\x00-\x1f]', "", name)  # drop unsafe chars/quotes/underscores entirely
    name = re.sub(r"\s+", " ", name).replace("( ", "(").replace(" )", ")")
    return name.strip(" .")[:max_len] or "track"


def is_audio(msg: Message) -> bool:
    doc = msg.document
    if doc is None:
        return False
    if msg.audio is not None:
        return True
    mime = (doc.mime_type or "").lower()
    if mime.startswith("audio/"):
        return True
    for attr in doc.attributes:
        if isinstance(attr, types.DocumentAttributeFilename):
            return Path(attr.file_name).suffix.lower() in AUDIO_EXTENSIONS
    return False


def build_filename(msg: Message, entry: str) -> str:
    doc = msg.document
    title = performer = file_name = None
    for attr in doc.attributes:
        if isinstance(attr, types.DocumentAttributeAudio):
            title, performer = attr.title, attr.performer
        elif isinstance(attr, types.DocumentAttributeFilename):
            file_name = attr.file_name

    ext = Path(file_name).suffix.lower() if file_name else ""
    if ext not in AUDIO_EXTENSIONS:
        mime = (doc.mime_type or "").lower()
        ext = MIME_EXT_FALLBACK.get(mime) or mimetypes.guess_extension(mime) or ".mp3"

    if performer:  # keep names short: first artist only, drop the bot's truncation ellipsis
        performer = performer.split(",")[0].replace("…", "").strip()
    if title and performer:
        stem = f"{title} - {performer}"  # artist always last
    elif title:
        stem = title
    elif file_name:
        stem = Path(file_name).stem
    else:
        stem = entry if not entry.startswith("http") else f"track_{msg.id}"
    return sanitize(stem) + ext


def unique_path(directory: Path, filename: str) -> Path:
    path = directory / filename
    stem, suffix, n = path.stem, path.suffix, 1
    while path.exists():
        path = directory / f"{stem} ({n}){suffix}"
        n += 1
    return path


def clickable_button(msg: Message) -> tuple[int, int] | None:
    """Return (row, col) of the first button that can be 'pressed' (callback / plain)."""
    for i, row in enumerate(msg.buttons or []):
        for j, btn in enumerate(row):
            if isinstance(btn.button, (types.KeyboardButtonCallback, types.KeyboardButton)):
                return i, j
    return None


def short(text: str, n: int = 120) -> str:
    text = " ".join(text.split())
    return text if len(text) <= n else text[: n - 1] + "…"


# --------------------------------------------------------------------------- #
# Bot conversation (concurrent)
#
# The bot answers requests strictly in the order they were sent (verified: for N
# requests it emits N "🎧 Track: ..." info messages, then N audio files, in FIFO
# order) and never uses reply-to threading. So replies are routed to jobs by:
#   info message  -> oldest job still waiting for its info (or best title-hint match)
#   audio file    -> the job whose info title best matches the audio's title tag
#   error text    -> oldest job still waiting for any response
# --------------------------------------------------------------------------- #
INFO_RE = re.compile(r"Track:\s*(.+)", re.IGNORECASE)


def _norm(s: str) -> str:
    return re.sub(r"[\W_]+", "", s.lower())


def similarity(a: str, b: str) -> float:
    a, b = _norm(a), _norm(b)
    if not a or not b:
        return 0.0
    if a in b or b in a:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def audio_title(msg: Message) -> str:
    for attr in msg.document.attributes:
        if isinstance(attr, types.DocumentAttributeAudio) and attr.title:
            return attr.title
    return ""


@dataclass(eq=False)
class Job:
    entry: Entry
    future: asyncio.Future
    state: str = "sent"  # sent -> clicked? -> info -> (resolved & removed)
    info_title: str = ""
    sent_id: int = 0
    clicks: int = 0

    @property
    def label(self) -> str:
        return short(self.entry.hint or self.entry.key, 38)


class BotPool:
    def __init__(
        self,
        client: TelegramClient,
        bot,
        out_dir: Path,
        timeout: float,
        send_gap: tuple[float, float],
    ) -> None:
        self.client, self.bot, self.out_dir = client, bot, out_dir
        self.timeout, self.send_gap = timeout, send_gap
        self.jobs: list[Job] = []  # in-flight requests, oldest first

        self._send_lock = asyncio.Lock()  # serializes sends => job order == send order
        self._last_send = 0.0
        self._gate = asyncio.Event()  # cleared while a FloodWait is being served
        self._gate.set()
        self._pause_until = 0.0

        self._reserved: set[Path] = set()  # target files currently being downloaded
        self._seen_audio: set[int] = set()
        self._seen_info: set[int] = set()
        self._seen_menu: set[int] = set()
        self._last_text: dict[int, str] = {}
        self._builders = [
            events.NewMessage(from_users=bot, incoming=True),
            events.MessageEdited(from_users=bot, incoming=True),
        ]

    # -- wiring -------------------------------------------------------------
    def attach(self) -> None:
        for b in self._builders:
            self.client.add_event_handler(self._route, b)

    def detach(self) -> None:
        for b in self._builders:
            self.client.remove_event_handler(self._route, b)

    # -- flood control ------------------------------------------------------
    async def _pause(self, seconds: float) -> None:
        """Global pause: every worker blocks at the gate until the FloodWait is over."""
        loop = asyncio.get_running_loop()
        self._pause_until = max(self._pause_until, loop.time() + seconds)
        self._gate.clear()
        log.warning("FloodWait: pausing ALL workers for %.0fs", seconds)
        while (remaining := self._pause_until - loop.time()) > 0:
            await asyncio.sleep(remaining)
        self._gate.set()

    async def _call(self, op):
        """Run an awaitable-factory, honoring/serving FloodWaitError (with the shared gate)."""
        for _ in range(MAX_FLOOD_RETRIES):
            await self._gate.wait()
            try:
                return await op()
            except FloodWaitError as exc:
                await self._pause(exc.seconds + 1)
        raise TrackFailed("Gave up after repeated FloodWait errors")

    # -- job bookkeeping ----------------------------------------------------
    def _drop(self, job: Job) -> None:
        if job in self.jobs:
            self.jobs.remove(job)

    def _resolve(self, job: Job, msg: Message) -> None:
        self._drop(job)
        if not job.future.done():
            job.future.set_result(msg)

    def _reject(self, job: Job, reason: str) -> None:
        self._drop(job)
        if not job.future.done():
            job.future.set_exception(TrackFailed(reason))

    def _waiting(self, msg: Message, states: tuple[str, ...]) -> list[Job]:
        return [j for j in self.jobs if j.state in states and j.sent_id < msg.id]

    # -- incoming message router -------------------------------------------
    async def _route(self, event) -> None:
        msg: Message = event.message
        text = (msg.raw_text or "").strip()

        # 1) audio file -> job whose info title matches the audio title tag
        if is_audio(msg):
            if msg.id in self._seen_audio:
                return
            self._seen_audio.add(msg.id)
            job = self._match_audio(msg)
            if job is None:
                log.warning("Audio from bot matched no pending request; ignored (%s)", short(audio_title(msg), 50))
            else:
                self._resolve(job, msg)
            return

        # 2) inline keyboard (search results / pickers) -> click the first option
        pos = clickable_button(msg)
        if pos:
            if msg.id in self._seen_menu:
                return
            cands = [j for j in self._waiting(msg, ("sent", "clicked")) if j.clicks < MAX_CLICKS_PER_TRACK]
            if not cands:
                return
            self._seen_menu.add(msg.id)
            job = cands[0]
            job.state, job.clicks = "clicked", job.clicks + 1
            label = msg.buttons[pos[0]][pos[1]].text
            log.info("%s: menu -> clicking %r", job.label, short(label, 50))
            try:
                await self._call(lambda: msg.click(*pos))
            except asyncio.TimeoutError:
                pass  # bot never answered the callback query; its reply arrives as a normal message
            except TrackFailed as exc:
                self._reject(job, str(exc))
            except RPCError as exc:
                log.debug("Button click RPC error (ignored): %s", exc)
            return

        if not text:
            return

        # 3) "🎧 Track: <title>" info message -> assign to a job
        m = INFO_RE.search(text)
        if m:
            if msg.id in self._seen_info:
                return
            self._seen_info.add(msg.id)
            cands = self._waiting(msg, ("sent", "clicked"))
            if not cands:
                return
            title = m.group(1).strip()
            best = max(cands, key=lambda j: similarity(title, j.entry.hint) if j.entry.hint else 0.0)
            if not best.entry.hint or similarity(title, best.entry.hint) < 0.6:
                best = cands[0]  # no usable hint: trust FIFO
            best.state, best.info_title = "info", title
            log.info("%s: bot found %r", best.label, short(title, 60))
            return

        # 4) any other text: error -> fail oldest waiting job; otherwise just a status line
        if self._last_text.get(msg.id) == text:
            return
        self._last_text[msg.id] = text
        if ERROR_RE.search(text):
            cands = self._waiting(msg, ("sent", "clicked"))
            if cands:
                log.error("%s: bot error: %s", cands[0].label, short(text, 120))
                self._reject(cands[0], short(text, 200))
                return
        log.info("Bot: %s", short(text))

    def _match_audio(self, msg: Message) -> Job | None:
        title = audio_title(msg)
        with_info = self._waiting(msg, ("info",))
        if with_info:
            best = max(with_info, key=lambda j: similarity(title, j.info_title))
            return best if similarity(title, best.info_title) >= 0.5 else with_info[0]
        no_info = self._waiting(msg, ("sent", "clicked"))  # bot sent audio without an info message
        return no_info[0] if no_info else None

    # -- public API ---------------------------------------------------------
    async def fetch(self, entry: Entry) -> Outcome:
        """Request one track and download it. Safe to call from many workers at once."""
        try:
            job = await self._submit(entry)
            try:
                msg = await asyncio.wait_for(job.future, self.timeout)
            except asyncio.TimeoutError:
                self._drop(job)
                raise TrackFailed(f"Timed out after {self.timeout:.0f}s waiting for the bot") from None
            return await self._download(job, msg)
        except TrackFailed as exc:
            return Outcome(False, str(exc))
        except asyncio.TimeoutError:
            return Outcome(False, f"Download stalled for more than {DOWNLOAD_TIMEOUT:.0f}s")
        except RPCError as exc:
            return Outcome(False, f"Telegram RPC error: {type(exc).__name__}: {exc}")

    async def _submit(self, entry: Entry) -> Job:
        loop = asyncio.get_running_loop()
        async with self._send_lock:
            # Global send throttle: one request every send_gap seconds (randomized),
            # regardless of how many workers are active.
            wait = self._last_send + random.uniform(*self.send_gap) - loop.time()
            if wait > 0:
                await asyncio.sleep(wait)
            job = Job(entry=entry, future=loop.create_future())
            self.jobs.append(job)  # register BEFORE sending so a fast reply can't be missed
            try:
                sent = await self._call(lambda: self.client.send_message(self.bot, entry.key))
            except BaseException:
                self._drop(job)
                raise
            job.sent_id = sent.id
            self._last_send = loop.time()
        return job

    async def _download(self, job: Job, msg: Message) -> Outcome:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        target = self.out_dir / build_filename(msg, job.entry.key)

        # No duplicates: if this file already exists on disk or is being downloaded by
        # another worker right now, don't fetch it again (and never create "name (1).mp3").
        if target.exists() or target in self._reserved:
            log.info("%s: already have %s, skipping duplicate", job.label, target.name)
            return Outcome(True, str(target))
        self._reserved.add(target)

        log.info("%s: downloading %s", job.label, target.name)
        # Temp name + rename on success: an interrupted run never leaves a truncated .mp3.
        part = target.with_name(target.name + ".part")
        try:
            path = await self._call(
                lambda: asyncio.wait_for(self.client.download_media(msg, file=str(part)), DOWNLOAD_TIMEOUT)
            )
            if not path:
                raise TrackFailed("download_media returned nothing")
            expected = getattr(msg.document, "size", None)
            if expected and part.stat().st_size != expected:
                raise TrackFailed(f"Incomplete download ({part.stat().st_size}/{expected} bytes)")
            part.replace(target)
        except BaseException:
            part.unlink(missing_ok=True)
            raise
        finally:
            self._reserved.discard(target)
        return Outcome(True, str(target))


# --------------------------------------------------------------------------- #
# Main flow
# --------------------------------------------------------------------------- #
async def run(args: argparse.Namespace) -> int:
    load_dotenv()
    api_hash = os.getenv("TELEGRAM_API_HASH")
    if not api_hash:
        console.print("[red]TELEGRAM_API_HASH is missing. Copy .env.example to .env and fill it in.[/red]")
        return 2
    api_id = int(os.getenv("TELEGRAM_API_ID", DEFAULT_API_ID))

    if args.login:
        async with TelegramClient(SESSION_NAME, api_id, api_hash) as client:
            me = await client.get_me()
            console.print(f"[green]Logged in as {me.first_name} (@{me.username}). Session saved.[/green]")
        return 0

    entries = load_entries(Path(args.input))
    if args.limit:
        entries = entries[: args.limit]
    if not entries:
        console.print("[yellow]No entries in input file.[/yellow]")
        return 0

    state = StateLog(LOG_FILE)
    for stale in DOWNLOAD_DIR.glob("*.part"):  # safe: we hold the single-instance lock
        stale.unlink(missing_ok=True)

    def is_done(e: Entry) -> bool:
        rec = state.completed.get(e.key)
        return bool(rec) and Path(rec["file"]).exists()

    todo = deque(
        e for e in entries if not is_done(e) and not (args.skip_failed and e.key in state.failed)
    )
    already = len(entries) - len(todo)
    workers = max(1, min(args.concurrency, len(todo))) if todo else 0
    log.info(
        "%d entries total, %d already handled, %d to process with %d parallel workers",
        len(entries), already, len(todo), workers,
    )

    ok = fail = active = 0
    send_gap = (min(args.send_gap), max(args.send_gap))

    async with TelegramClient(SESSION_NAME, api_id, api_hash) as client:
        bot = await client.get_entity(BOT_USERNAME)
        pool = BotPool(client, bot, DOWNLOAD_DIR, args.timeout, send_gap)
        pool.attach()

        progress = Progress(
            TextColumn("[bold blue]{task.description}", justify="left"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        )

        def refresh() -> None:
            progress.update(task, description=f"{active} active | {ok} ok | {fail} failed")

        async def worker() -> None:
            nonlocal ok, fail, active
            while todo:
                entry = todo.popleft()
                active += 1
                refresh()
                outcome = await pool.fetch(entry)
                active -= 1
                if outcome.ok:
                    ok += 1
                    state.mark_done(entry.key, outcome.detail)
                    log.info("OK   %s -> %s", short(entry.hint or entry.key, 50), outcome.detail)
                else:
                    fail += 1
                    state.mark_failed(entry.key, outcome.detail)
                    log.error("FAIL %s: %s", short(entry.hint or entry.key, 50), outcome.detail)
                progress.advance(task)
                refresh()

        try:
            with progress:
                task = progress.add_task("Starting", total=len(entries), completed=already)
                await asyncio.gather(*(worker() for _ in range(workers)))
        finally:
            pool.detach()

    console.print(f"\n[bold]Done.[/bold] downloaded={ok} failed={fail} skipped={already}  log={LOG_FILE}")
    return 0 if fail == 0 else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", nargs="?", default=str(DEFAULT_INPUT), help="text file, one track per line")
    parser.add_argument("-c", "--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                        help=f"tracks in flight at once, 1-10 (default {DEFAULT_CONCURRENCY})")
    parser.add_argument("--send-gap", type=float, nargs=2, metavar=("MIN", "MAX"), default=list(SEND_GAP),
                        help="random delay range (s) between any two requests sent to the bot")
    parser.add_argument("--timeout", type=float, default=PER_SONG_TIMEOUT, help="per-song bot-reply timeout in seconds")
    parser.add_argument("--skip-failed", action="store_true", help="don't retry tracks that failed earlier")
    parser.add_argument("--limit", type=int, default=0, help="only process the first N entries (for testing)")
    parser.add_argument("--login", action="store_true", help="just authenticate and save the session, then exit")
    args = parser.parse_args()
    if not 1 <= args.concurrency <= 10:
        parser.error("--concurrency must be between 1 and 10")

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, show_path=False, markup=False)],
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)

    try:
        with single_instance():
            sys.exit(asyncio.run(run(args)))
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted. Progress is saved; re-run to resume.[/yellow]")
        sys.exit(130)


if __name__ == "__main__":
    main()

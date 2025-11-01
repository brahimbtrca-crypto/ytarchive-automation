#!/usr/bin/env python3
"""
yt_auto_download.py
- Runs a single recording job for one or more YouTube live URLs (concurrent).
- Uses ytarchive (preferred) falling back to yt-dlp if needed.
- After a successful recording, uploads to Google Drive via rclone.
- Keeps only `KEEP_LAST_N` recent recordings locally.
- Logging to stdout and a recording log file.
"""

import os
import sys
import asyncio
import subprocess
import json
import shlex
from pathlib import Path
from datetime import datetime
from dateutil import parser as dateparser
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configuration (can be changed or passed via env vars)
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "./recordings"))
RCLONE_REMOTE = os.environ.get("RCLONE_REMOTE", "gdrive:yt_backups")  # rclone remote:path
KEEP_LAST_N = int(os.environ.get("KEEP_LAST_N", "5"))
URLS_FILE = os.environ.get("URLS_FILE", "urls.txt")
YTARCHIVE_CMD = os.environ.get("YTARCHIVE_CMD", "ytarchive")
YTDLP_CMD = os.environ.get("YTDLP_CMD", "yt-dlp")
LOGFILE = OUTPUT_DIR / "recording.log"
MAX_RUNTIME_SECONDS = int(os.environ.get("MAX_RUNTIME_SECONDS", str(6*3600)))  # 6 hours default

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def log(msg):
    t = datetime.utcnow().isoformat()
    line = f"{t} UTC | {msg}"
    print(line, flush=True)
    with open(LOGFILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

async def run_process(cmd, cwd=None, stdout_log=None, stderr_log=None, timeout=None):
    log(f"Starting: {cmd}")
    proc = await asyncio.create_subprocess_shell(cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd)
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise
    out = stdout.decode(errors="ignore") if stdout else ""
    err = stderr.decode(errors="ignore") if stderr else ""
    if stdout_log:
        async with aiofiles.open(stdout_log, "a") as f:
            await f.write(out)
    if stderr_log:
        async with aiofiles.open(stderr_log, "a") as f:
            await f.write(err)
    return proc.returncode, out, err

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=2, max=30))
def rclone_copy_with_retry(local_path: str, remote_path: str):
    # raises subprocess.CalledProcessError on persistent error
    cmd = f"rclone copyto --progress {shlex.quote(local_path)} {shlex.quote(remote_path)}"
    log(f"Uploading {local_path} -> {remote_path}")
    r = subprocess.run(cmd, shell=True, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if r.returncode != 0:
        log(f"rclone failed: {r.stderr.strip()[:800]}")
        raise subprocess.CalledProcessError(r.returncode, cmd, output=r.stdout, stderr=r.stderr)
    log(f"Upload OK: {local_path}")
    return True

def rclone_delete_local(path):
    try:
        os.remove(path)
        log(f"Deleted local file: {path}")
    except Exception as e:
        log(f"Failed to delete local file {path}: {e}")

def cleanup_keep_last(output_dir: Path, keep_n: int):
    files = sorted([p for p in output_dir.iterdir() if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)
    for f in files[keep_n:]:
        try:
            f.unlink()
            log(f"Cleanup removed old file: {f}")
        except Exception as e:
            log(f"Cleanup failed for {f}: {e}")

async def record_with_ytarchive(url: str, outdir: Path, max_runtime: int):
    # ytarchive typical CLI: ytarchive <url> best -o <outdir>/<title> ...
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = url.replace("https://", "").replace("/", "_").replace(":", "_")
    outpref = outdir / f"{safe_name}_{timestamp}.mkv"
    # Some ytarchive versions produce multiple files; we instruct output to a single filename using --output
    cmd = f"{YTARCHIVE_CMD} --no-frag-files -q --output {shlex.quote(str(outpref))} {shlex.quote(url)}"
    # If ytarchive is missing or fails, we will raise to let fallback try
    log(f"Attempting ytarchive for {url}")
    proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=max_runtime)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        log(f"ytarchive timed out after {max_runtime} seconds for {url}")
        raise
    rc = proc.returncode
    log(f"ytarchive exit {rc} for {url}")
    if rc != 0:
        raise RuntimeError(f"ytarchive failed: {stderr.decode('utf-8', errors='ignore')[:1000]}")
    # find created file (best-effort)
    created = sorted(outdir.glob(f"{safe_name}_*.mkv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not created:
        raise RuntimeError("ytarchive didn't produce expected file")
    return str(created[0])

async def record_with_ytdlp(url: str, outdir: Path, max_runtime: int):
    # yt-dlp can be used to record live fragments; prefer single-file via ffmpeg postprocess, but this is best-effort
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = url.replace("https://", "").replace("/", "_").replace(":", "_")
    outfile = outdir / f"{safe_name}_{timestamp}.mp4"
    cmd = f"{YTDLP_CMD} -f best -o {shlex.quote(str(outfile))} --hls-use-mpegts --no-part --continue {shlex.quote(url)}"
    log(f"Attempting yt-dlp for {url}")
    proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=max_runtime)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        log(f"yt-dlp timed out after {max_runtime} seconds for {url}")
        raise
    rc = proc.returncode
    log(f"yt-dlp exit {rc} for {url}")
    if rc != 0:
        raise RuntimeError(f"yt-dlp failed: {stderr.decode('utf-8', errors='ignore')[:1000]}")
    if not outfile.exists():
        raise RuntimeError("yt-dlp didn't produce expected file")
    return str(outfile)

async def do_one(url: str):
    # main flow for a single URL
    try:
        # start recording
        try:
            recorded = await record_with_ytarchive(url, OUTPUT_DIR, MAX_RUNTIME_SECONDS)
        except Exception as e:
            log(f"ytarchive failed for {url}: {e}. Falling back to yt-dlp.")
            recorded = await record_with_ytdlp(url, OUTPUT_DIR, MAX_RUNTIME_SECONDS)
        log(f"Recorded file: {recorded}")
        # upload to rclone remote: path with same filename
        fname = os.path.basename(recorded)
        remote_path = f"{RCLONE_REMOTE.rstrip('/')}/{fname}"
        # Do upload with retries
        rclone_copy_with_retry(recorded, remote_path)
        # optionally verify (skip expensive checksum for speed); using rclone's size or md5sum would be possible
        # remove local
        rclone_delete_local(recorded)
        # cleanup extra files
        cleanup_keep_last(OUTPUT_DIR, KEEP_LAST_N)
        # write status
        status = {
            "url": url,
            "uploaded": True,
            "remote_path": remote_path,
            "time_utc": datetime.utcnow().isoformat()
        }
        with open(OUTPUT_DIR / f"status_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
            json.dump(status, f)
        log(f"Job complete for {url}")
    except Exception as e:
        log(f"Job failed for {url}: {e}")
        # write a failed status
        status = {"url": url, "uploaded": False, "error": str(e), "time_utc": datetime.utcnow().isoformat()}
        with open(OUTPUT_DIR / f"status_fail_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
            json.dump(status, f)
        raise

async def main():
    # read urls
    if len(sys.argv) > 1:
        urls = sys.argv[1:]
    else:
        if not Path(URLS_FILE).exists():
            log(f"No URLs specified and {URLS_FILE} missing.")
            sys.exit(1)
        urls = [line.strip() for line in open(URLS_FILE, "r", encoding="utf-8") if line.strip() and not line.startswith("#")]
    log(f"Starting run for {len(urls)} URL(s). Max runtime per job: {MAX_RUNTIME_SECONDS}s")
    tasks = [do_one(u) for u in urls]
    # run concurrently (each do_one records then uploads)
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    import aiofiles  # dynamic import to fail early if missing
    asyncio.run(main())

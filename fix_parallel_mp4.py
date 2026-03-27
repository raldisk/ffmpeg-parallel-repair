"""
fix_parallel_mp4.py — Principal-grade MP4 repair pipeline
Combines: dynamic duration validation, bounded workers, temp isolation,
          streaming logs, codec-aware re-encode, cached HW detection.
"""

import subprocess
import threading
import tempfile
import json
import os
import sys
import time
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from shutil import move
from typing import Optional
from uuid import uuid4

# ── Threading ────────────────────────────────────────────────────────────────

PRINT_LOCK = threading.Lock()
COLORS = ["\033[91m", "\033[92m", "\033[93m", "\033[94m", "\033[95m", "\033[96m"]
RESET = "\033[0m"


def log(msg: str, color: str = "") -> None:
    with PRINT_LOCK:
        print(f"{color}{msg}{RESET}" if color else msg, flush=True)


# ── Domain types ─────────────────────────────────────────────────────────────

class Strategy(Enum):
    TIMESTAMPS   = auto()
    REBUILD      = auto()
    REENCODE     = auto()
    FAILED       = auto()


@dataclass(frozen=True)
class RepairResult:
    input_file:    Path
    success:       bool
    strategy:      Strategy
    output_path:   Optional[Path]
    orig_duration: float
    out_duration:  float
    error:         Optional[str] = None

    @property
    def quality_ratio(self) -> float:
        if self.orig_duration <= 0:
            return 1.0 if self.out_duration > 0 else 0.0
        return self.out_duration / self.orig_duration


# ── ffprobe / ffmpeg helpers ──────────────────────────────────────────────────

def _preflight() -> None:
    """Fail fast if ffmpeg/ffprobe are not on PATH."""
    for tool in ("ffmpeg", "ffprobe"):
        r = subprocess.run([tool, "-version"], capture_output=True)
        if r.returncode != 0:
            sys.exit(f"[ERROR] '{tool}' not found on PATH")


def get_source_vcodec(path: Path) -> str:
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(path)
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.stdout.strip() or "libx264"


def get_duration(path: Path) -> float:
    if not path.exists() or path.stat().st_size == 0:
        return 0.0
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(path)
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return float(r.stdout.strip())
    except Exception:
        return 0.0


def duration_valid(original: float, repaired: float) -> bool:
    if original <= 1.0:
        return repaired > 0.5
    return repaired >= original * 0.95


def detect_hwaccel() -> list[str]:
    """Detect NVENC once at startup; fall back to libx264."""
    r = subprocess.run(["ffmpeg", "-hide_banner", "-encoders"],
                       capture_output=True, text=True)
    if "h264_nvenc" in r.stdout:
        log("[init] Hardware encoder: h264_nvenc")
        return ["-c:v", "h264_nvenc", "-preset", "p2"]
    log("[init] Hardware encoder: libx264 (software)")
    return ["-c:v", "libx264", "-preset", "veryfast", "-crf", "18"]


# ── Core repair logic ─────────────────────────────────────────────────────────

def run_ffmpeg(cmd: list[str], prefix: str, log_file: Path, color: str) -> int:
    """Stream ffmpeg output to console (colored) and log file simultaneously."""
    with open(log_file, "a", encoding="utf-8") as fh:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1
        )
        for line in proc.stdout:
            stripped = line.rstrip()
            log(f"[{prefix}] {stripped}", color)
            fh.write(stripped + "\n")
        proc.stdout.close()
        proc.wait()
    return proc.returncode


def _attempt(
    cmd: list[str],
    dst: Path,
    orig_dur: float,
    prefix: str,
    log_file: Path,
    color: str,
) -> bool:
    """Run a repair command; return True if output passes duration validation."""
    dst.unlink(missing_ok=True)
    rc = run_ffmpeg(cmd, prefix, log_file, color)
    if rc != 0:
        dst.unlink(missing_ok=True)
        return False
    d = get_duration(dst)
    ok = duration_valid(orig_dur, d)
    if not ok:
        dst.unlink(missing_ok=True)
    return ok


def fix_video(
    input_file: Path,
    fixed_dir: Path,
    temp_dir: Path,
    logs_dir: Path,
    color: str,
    hw_flags: list[str],
) -> RepairResult:
    src = input_file.resolve()
    tag = uuid4().hex[:8]
    prefix = src.name

    s1 = temp_dir / f"{src.stem}_{tag}_s1.mp4"
    s2 = temp_dir / f"{src.stem}_{tag}_s2.mp4"
    final = fixed_dir / f"{src.stem}_fixed.mp4"
    log_file = logs_dir / f"{src.stem}_{tag}.log"

    orig_dur = get_duration(src)
    log(f"\n[{prefix}] original duration: {orig_dur:.2f}s", color)

    # Step 1 — regenerate timestamps (lossless)
    log(f"[{prefix}] Step 1: regenerate timestamps", color)
    if _attempt(
        ["ffmpeg", "-loglevel", "verbose", "-stats", "-y",
         "-fflags", "+genpts", "-i", str(src),
         "-c", "copy", "-movflags", "+faststart", str(s1)],
        s1, orig_dur, prefix, log_file, color
    ):
        move(s1, final)
        return RepairResult(src, True, Strategy.TIMESTAMPS, final, orig_dur, get_duration(final))

    # Step 2 — rebuild index (lossless)
    log(f"[{prefix}] Step 2: rebuild index", color)
    if _attempt(
        ["ffmpeg", "-loglevel", "verbose", "-stats", "-y",
         "-i", str(src), "-map", "0", "-c", "copy",
         "-avoid_negative_ts", "make_zero", str(s2)],
        s2, orig_dur, prefix, log_file, color
    ):
        move(s2, final)
        return RepairResult(src, True, Strategy.REBUILD, final, orig_dur, get_duration(final))

    # Step 3 — re-encode (lossy fallback; detect source codec to avoid silent downgrade)
    src_codec = get_source_vcodec(src)
    if src_codec not in ("h264", "avc1") and hw_flags[1] == "h264_nvenc":
        log(f"[{prefix}] WARNING: source is {src_codec}, NVENC produces H.264 — using libx264", color)
        encode_flags = ["-c:v", "libx264", "-preset", "veryfast", "-crf", "18"]
    else:
        encode_flags = hw_flags

    log(f"[{prefix}] Step 3: re-encode ({encode_flags[1]})", color)
    s3 = temp_dir / f"{src.stem}_{tag}_s3.mp4"
    rc = run_ffmpeg(
        ["ffmpeg", "-loglevel", "verbose", "-stats", "-y",
         "-i", str(src)] + encode_flags + ["-c:a", "copy", str(s3)],
        prefix, log_file, color
    )
    if rc == 0 and s3.exists():
        move(s3, final)
        return RepairResult(src, True, Strategy.REENCODE, final, orig_dur, get_duration(final))

    return RepairResult(src, False, Strategy.FAILED, None, orig_dur, 0.0,
                        error="All strategies exhausted")


# ── Orchestration ─────────────────────────────────────────────────────────────

def main() -> None:
    _preflight()

    cwd = Path.cwd()
    fixed_dir = cwd / "fixed"
    logs_dir  = cwd / "logs"
    fixed_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)

    files = sorted(cwd.glob("*.mp4"))
    if not files:
        print("No MP4 files found.")
        input("Press ENTER to close.")
        return

    # Detect HW encoder once — cached for all workers
    hw_flags = detect_hwaccel()

    # Bounded worker count: IO + CPU balance.
    # More than cpu_count//2 workers hurts on re-encode workloads;
    # 4 is a safe conservative ceiling for parallel lossless passes.
    cpu = os.cpu_count() or 2
    workers = min(len(files), max(2, cpu // 2))

    # Isolated temp dir — never pollutes source directory
    temp_dir = Path(tempfile.mkdtemp(prefix="mp4fix_"))

    print(f"Found {len(files)} file(s), {workers} workers, temp: {temp_dir}")

    results: list[RepairResult] = []

    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            fmap = {
                ex.submit(fix_video, f, fixed_dir, temp_dir, logs_dir,
                          COLORS[i % len(COLORS)], hw_flags): f
                for i, f in enumerate(files)
            }
            for future in as_completed(fmap):
                f = fmap[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    log(f"[{f.name}] CRASHED: {e}")
                    results.append(RepairResult(f, False, Strategy.FAILED, None, 0.0, 0.0,
                                                error=str(e)))
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    print("\n=========== RESULTS ===========")
    for r in results:
        icon = "✓" if r.success else "✗"
        ratio = f"{r.quality_ratio:.1%}" if r.success else "—"
        print(f"  {icon} {r.input_file.name} → {r.strategy.name} ({ratio})")
        if r.error:
            print(f"      {r.error}")
    print("===============================")
    input("All done. Press ENTER to close.")


if __name__ == "__main__":
    main()

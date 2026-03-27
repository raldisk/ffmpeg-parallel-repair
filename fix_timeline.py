"""
verify_fixed.py — Post-repair integrity verifier for MP4 files.

Runs after fix_parallel_mp4.py. Performs four checks that belong here
and not in the repair pipeline:

  1. Container readability   — ffprobe can parse the file at all
  2. Stream presence         — at minimum one video stream exists
  3. Full decode pass        — ffmpeg -f null decodes every frame
  4. Duration fidelity       — compares against originals when available

Verdict tiers
  PASS  clean decode, duration intact, video stream present
  WARN  decoded successfully but with error lines in stderr, or
        duration ratio slightly off, or audio stream absent
  FAIL  ffprobe can't read it, no video stream, decode non-zero,
        or file is missing / zero-byte

Usage
  python verify_fixed.py                          # fixed/ vs ./
  python verify_fixed.py --fixed fixed/ --source ./
  python verify_fixed.py --strict                 # WARN treated as FAIL
  python verify_fixed.py --report my_report.txt

Exit code: 0 = all PASS, 1 = any WARN or FAIL present.
"""

import argparse
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional

import subprocess


# ── ANSI ──────────────────────────────────────────────────────────────────────

RESET  = "\033[0m"
RED    = "\033[91m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"

PRINT_LOCK = threading.Lock()


def cprint(msg: str, color: str = "") -> None:
    with PRINT_LOCK:
        print(f"{color}{msg}{RESET}" if color else msg, flush=True)


# ── Domain types ──────────────────────────────────────────────────────────────

class Verdict(Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"

    @property
    def color(self) -> str:
        return {Verdict.PASS: GREEN, Verdict.WARN: YELLOW, Verdict.FAIL: RED}[self]

    @property
    def symbol(self) -> str:
        return {Verdict.PASS: "✓", Verdict.WARN: "⚠", Verdict.FAIL: "✗"}[self]


@dataclass(frozen=True)
class StreamInfo:
    has_video:   bool
    has_audio:   bool
    video_codec: str
    audio_codec: str
    width:       int
    height:      int


# How many decode error/warning lines to show inline before truncating.
DECODE_LINES_PREVIEW = 5


@dataclass
class VerifyResult:
    path:            Path
    verdict:         Verdict
    duration:        float               # duration of fixed file; 0 if unreadable
    source_duration: float               # 0.0 if no source dir provided
    stream_info:     Optional[StreamInfo]
    error_lines:     list[str]           # raw stderr lines containing "error"
    warning_lines:   list[str]           # raw stderr lines containing "warning"
    elapsed:         float               # seconds for the decode test
    issues:          list[str] = field(default_factory=list)

    # ── Computed ──────────────────────────────────────────────────────────────

    @property
    def decode_errors(self) -> int:
        return len(self.error_lines)

    @property
    def decode_warnings(self) -> int:
        return len(self.warning_lines)

    @property
    def quality_ratio(self) -> float:
        if self.source_duration <= 0:
            return 1.0
        if self.duration <= 0:
            return 0.0
        return self.duration / self.source_duration


# ── ffprobe / ffmpeg helpers ───────────────────────────────────────────────────

def _preflight() -> None:
    for tool in ("ffmpeg", "ffprobe"):
        r = subprocess.run([tool, "-version"], capture_output=True)
        if r.returncode != 0:
            sys.exit(f"[ERROR] '{tool}' not found on PATH — install ffmpeg and retry.")


def _probe_duration(path: Path) -> float:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return float(r.stdout.strip())
    except Exception:
        return 0.0


def _probe_streams(path: Path) -> Optional[StreamInfo]:
    """
    Returns StreamInfo parsed from ffprobe JSON, or None if ffprobe fails.
    Uses ffprobe -show_streams to get codec names and video dimensions.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-show_streams",
        "-of", "json",
        str(path),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        data = json.loads(r.stdout)
        streams = data.get("streams", [])
    except Exception:
        return None

    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    audio = next((s for s in streams if s.get("codec_type") == "audio"), None)

    return StreamInfo(
        has_video   = video is not None,
        has_audio   = audio is not None,
        video_codec = video.get("codec_name", "unknown") if video else "—",
        audio_codec = audio.get("codec_name", "unknown") if audio else "—",
        width       = int(video.get("width",  0)) if video else 0,
        height      = int(video.get("height", 0)) if video else 0,
    )


def _decode_test(path: Path) -> tuple[int, list[str], list[str]]:
    """
    Run a full decode pass via `ffmpeg -v error -i file -f null -`.
    Returns (returncode, error_lines, warning_lines).

    -v error suppresses informational output so anything written to stderr
    is a genuine decode problem. Lines are bucketed by first keyword match:
    "error" → error_lines, "warning" → warning_lines. Callers get the raw
    lines, not just counts, so they can surface exactly what ffmpeg said.

    returncode != 0 is an automatic FAIL regardless of line content.
    """
    cmd = ["ffmpeg", "-v", "error", "-i", str(path), "-f", "null", "-"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        return -1, ["ffmpeg decode timed out (>600s)"], []

    error_lines:   list[str] = []
    warning_lines: list[str] = []

    for line in r.stderr.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        ll = stripped.lower()
        if "error" in ll:
            error_lines.append(stripped)
        elif "warning" in ll:
            warning_lines.append(stripped)

    return r.returncode, error_lines, warning_lines


# ── Core verification ─────────────────────────────────────────────────────────

def verify_file(
    path: Path,
    source_dir: Optional[Path],
) -> VerifyResult:
    issues: list[str] = []

    # ── Guard: file must exist and be non-empty ───────────────────────────────
    if not path.exists():
        return VerifyResult(path, Verdict.FAIL, 0.0, 0.0, None, [], [], 0.0,
                            issues=["File does not exist"])

    if path.stat().st_size == 0:
        return VerifyResult(path, Verdict.FAIL, 0.0, 0.0, None, [], [], 0.0,
                            issues=["File is zero bytes"])

    # ── Source duration lookup ────────────────────────────────────────────────
    # Canonical original filename: strip the _fixed suffix added by the repair
    # script. Works for the default naming convention; silently skips if the
    # source can't be found (no issue raised — source dir is optional).
    source_duration = 0.0
    if source_dir is not None:
        stem = path.stem
        if stem.endswith("_fixed"):
            original_stem = stem[: -len("_fixed")]
        else:
            original_stem = stem

        source_candidate = source_dir / f"{original_stem}.mp4"
        if source_candidate.exists():
            source_duration = _probe_duration(source_candidate)

    # ── Stream presence via ffprobe ───────────────────────────────────────────
    stream_info = _probe_streams(path)

    if stream_info is None:
        return VerifyResult(path, Verdict.FAIL, 0.0, source_duration, None, [], [], 0.0,
                            issues=["ffprobe failed — container unreadable"])

    if not stream_info.has_video:
        issues.append("No video stream found")

    if not stream_info.has_audio:
        issues.append("No audio stream found (WARN only — may be intentional)")

    # ── Duration ─────────────────────────────────────────────────────────────
    duration = _probe_duration(path)

    if duration <= 0:
        issues.append("Duration reported as zero or negative")

    if source_duration > 0:
        ratio = duration / source_duration if duration > 0 else 0.0
        if ratio < 0.90:
            issues.append(f"Duration loss severe: {ratio:.1%} of original")
        elif ratio < 0.95:
            issues.append(f"Duration slightly short: {ratio:.1%} of original")

    # ── Full decode pass ──────────────────────────────────────────────────────
    t0 = time.perf_counter()
    rc, error_lines, warning_lines = _decode_test(path)
    elapsed = time.perf_counter() - t0

    if rc != 0:
        issues.append(
            f"Decode non-zero exit ({rc}): {len(error_lines)} error line(s)"
        )
    elif error_lines:
        issues.append(
            f"Decode succeeded but {len(error_lines)} error line(s) in stderr"
        )

    if warning_lines:
        issues.append(f"{len(warning_lines)} warning line(s) during decode")

    # ── Verdict resolution ────────────────────────────────────────────────────
    # FAIL conditions (hard): no video, unreadable container, bad decode exit,
    #                          zero/negative duration.
    # WARN conditions (soft): decode errors in stderr, missing audio,
    #                          quality ratio slightly low, decode warnings.
    fail_triggers = {
        "File does not exist",
        "File is zero bytes",
        "ffprobe failed — container unreadable",
        "No video stream found",
        "Duration reported as zero or negative",
    }
    hard_fail = (
        rc != 0
        or any(i in fail_triggers for i in issues)
        or (source_duration > 0 and duration / source_duration < 0.50)
    )

    if hard_fail:
        verdict = Verdict.FAIL
    elif issues:
        verdict = Verdict.WARN
    else:
        verdict = Verdict.PASS

    return VerifyResult(
        path            = path,
        verdict         = verdict,
        duration        = duration,
        source_duration = source_duration,
        stream_info     = stream_info,
        error_lines     = error_lines,
        warning_lines   = warning_lines,
        elapsed         = elapsed,
        issues          = issues,
    )


# ── Reporting ─────────────────────────────────────────────────────────────────

def _fmt_duration(s: float) -> str:
    if s <= 0:
        return "—"
    h, rem = divmod(int(s), 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def _fmt_lines_preview(lines: list[str], color: str, indent: str = "         ") -> list[str]:
    """
    Format up to DECODE_LINES_PREVIEW raw lines for terminal display,
    appending a '(+N more)' tail when the list is longer.
    """
    out: list[str] = []
    for line in lines[:DECODE_LINES_PREVIEW]:
        out.append(f"{color}{indent}{line}{RESET}")
    remaining = len(lines) - DECODE_LINES_PREVIEW
    if remaining > 0:
        out.append(f"{color}{indent}(+{remaining} more — see verify_report.txt){RESET}")
    return out


def _render_result(r: VerifyResult, show_detail: bool = True) -> None:
    ratio_str = f"{r.quality_ratio:.1%}" if r.source_duration > 0 else "no source"
    si = r.stream_info
    codec_str = f"{si.video_codec} {si.width}×{si.height}" if si and si.has_video else "—"

    cprint(
        f"  {r.verdict.symbol} [{r.verdict.value}]  {r.path.name}",
        r.verdict.color
    )
    cprint(
        f"       duration: {_fmt_duration(r.duration)}  "
        f"ratio: {ratio_str}  "
        f"codec: {codec_str}  "
        f"decode: {r.elapsed:.1f}s  "
        f"errors: {r.decode_errors}  warnings: {r.decode_warnings}"
    )

    if not show_detail:
        return

    issue_color = YELLOW if r.verdict == Verdict.WARN else RED
    for issue in r.issues:
        cprint(f"       → {issue}", issue_color)

    # Surface actual error lines directly below the issues that reference them
    if r.error_lines:
        cprint(f"       ffmpeg errors:", issue_color)
        with PRINT_LOCK:
            for line in _fmt_lines_preview(r.error_lines, issue_color):
                print(line, flush=True)

    if r.warning_lines:
        cprint(f"       ffmpeg warnings:", YELLOW)
        with PRINT_LOCK:
            for line in _fmt_lines_preview(r.warning_lines, YELLOW):
                print(line, flush=True)


def _write_report(results: list[VerifyResult], report_path: Path) -> None:
    lines = ["verify_fixed.py — Report", "=" * 60, ""]

    pass_count = sum(1 for r in results if r.verdict == Verdict.PASS)
    warn_count = sum(1 for r in results if r.verdict == Verdict.WARN)
    fail_count = sum(1 for r in results if r.verdict == Verdict.FAIL)

    lines.append(f"Total: {len(results)}  PASS: {pass_count}  WARN: {warn_count}  FAIL: {fail_count}")
    lines.append("")

    for r in results:
        ratio_str = f"{r.quality_ratio:.1%}" if r.source_duration > 0 else "no source"
        si = r.stream_info
        codec_str = f"{si.video_codec} {si.width}×{si.height}" if si and si.has_video else "—"
        lines.append(
            f"[{r.verdict.value}]  {r.path.name}\n"
            f"  duration : {_fmt_duration(r.duration)} (source: {_fmt_duration(r.source_duration)})\n"
            f"  ratio    : {ratio_str}\n"
            f"  codec    : {codec_str}\n"
            f"  decode   : {r.elapsed:.1f}s  errors: {r.decode_errors}  warnings: {r.decode_warnings}"
        )
        for issue in r.issues:
            lines.append(f"  → {issue}")

        # Full error lines — no truncation in the file, unlike the terminal preview
        if r.error_lines:
            lines.append("  ffmpeg errors:")
            for el in r.error_lines:
                lines.append(f"    {el}")

        if r.warning_lines:
            lines.append("  ffmpeg warnings:")
            for wl in r.warning_lines:
                lines.append(f"    {wl}")

        lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    cprint(f"\nReport written to: {report_path}", CYAN)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    _preflight()

    parser = argparse.ArgumentParser(
        description="Post-repair integrity verifier for MP4 files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--fixed", type=Path, default=Path("fixed"),
        help="Directory of repaired files to verify (default: ./fixed)",
    )
    parser.add_argument(
        "--source", type=Path, default=None,
        help="Directory of originals for duration comparison (default: ./)",
    )
    parser.add_argument(
        "--report", type=Path, default=None,
        help="Write text report to this path (default: fixed/verify_report.txt)",
    )
    parser.add_argument(
        "--strict", action="store_true",
        help="Treat WARN as FAIL for exit-code purposes (useful in CI)",
    )
    parser.add_argument(
        "--workers", type=int, default=None,
        help="Parallel workers (default: cpu_count // 2, min 2)",
    )
    args = parser.parse_args()

    fixed_dir: Path  = args.fixed
    source_dir: Optional[Path] = args.source

    # Default source dir: parent of fixed/ (assumes fix_parallel_mp4.py conventions)
    if source_dir is None:
        candidate = fixed_dir.parent
        if any(candidate.glob("*.mp4")):
            source_dir = candidate

    if not fixed_dir.exists():
        sys.exit(f"[ERROR] Fixed directory not found: {fixed_dir}")

    files = sorted(fixed_dir.glob("*.mp4"))
    if not files:
        sys.exit(f"[ERROR] No MP4 files found in {fixed_dir}")

    cpu = os.cpu_count() or 2
    workers = args.workers or max(2, cpu // 2)

    cprint(f"\n{BOLD}verify_fixed.py{RESET}")
    cprint(f"  fixed   : {fixed_dir.resolve()}")
    cprint(f"  source  : {source_dir.resolve() if source_dir else '(not provided)'}")
    cprint(f"  files   : {len(files)}")
    cprint(f"  workers : {workers}")
    cprint(f"  strict  : {args.strict}")
    cprint("")

    results: list[VerifyResult] = []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        fmap = {ex.submit(verify_file, f, source_dir): f for f in files}
        for future in as_completed(fmap):
            f = fmap[future]
            try:
                result = future.result()
            except Exception as e:
                result = VerifyResult(
                    f, Verdict.FAIL, 0.0, 0.0, None, [], [], 0.0,
                    issues=[f"Verifier crashed: {e}"]
                )
            results.append(result)
            _render_result(result)

    # Sort for deterministic report output regardless of completion order
    results.sort(key=lambda r: r.path.name)

    pass_count = sum(1 for r in results if r.verdict == Verdict.PASS)
    warn_count = sum(1 for r in results if r.verdict == Verdict.WARN)
    fail_count = sum(1 for r in results if r.verdict == Verdict.FAIL)

    cprint(f"\n{'='*50}")
    cprint(f"  PASS: {pass_count}  WARN: {warn_count}  FAIL: {fail_count}  Total: {len(results)}")
    cprint(f"{'='*50}\n")

    report_path = args.report or (fixed_dir / "verify_report.txt")
    _write_report(results, report_path)

    any_failure = fail_count > 0 or (args.strict and warn_count > 0)
    sys.exit(1 if any_failure else 0)


if __name__ == "__main__":
    main()

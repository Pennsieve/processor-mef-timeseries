"""A failed MEF streamer must fail the run, and say so itself.

The expensive failure here is a misleading one. When a non-zero exit was
ignored, a JVM crash mid-conversion left an empty staging directory and the run
died later with "No valid channel manifests" — which reads as a discovery or
path problem and sends you looking in the wrong place. The real error was in
the log twenty lines earlier.
"""

import struct
import subprocess
from pathlib import Path

import pytest

from processor.mef_streamer import END, stage_from_stream


def _frames(*payloads: bytes) -> bytes:
    """Encode frames as the streamer does: [type:1][len:4 LE][payload]."""
    return b"".join(payloads)


def _end_frame() -> bytes:
    return bytes([END]) + struct.pack("<I", 0)


def _fake_streamer(tmp_path: Path, exit_code: int, emit_end: bool = True) -> list[str]:
    """A python one-liner standing in for the jar: emits frames, then exits."""
    script = tmp_path / "fake_streamer.py"
    body = "import sys\n"
    if emit_end:
        body += (
            "sys.stdout.buffer.write(bytes([5]) + (0).to_bytes(4, 'little'))\n"
            "sys.stdout.buffer.flush()\n"
        )
    body += f"sys.exit({exit_code})\n"
    script.write_text(body)
    return ["python3", str(script)]


def test_a_clean_exit_is_not_an_error(tmp_path):
    stage_from_stream(_fake_streamer(tmp_path, 0), tmp_path / "staging")


def test_a_crashed_streamer_fails_the_run(tmp_path):
    """Exit code 1 is what a JVM OutOfMemoryError looks like from here."""
    with pytest.raises(RuntimeError, match="exit code 1"):
        stage_from_stream(_fake_streamer(tmp_path, 1), tmp_path / "staging")


def test_the_error_points_at_the_streamer_not_at_staging(tmp_path):
    with pytest.raises(RuntimeError, match="MEF streamer failed"):
        stage_from_stream(_fake_streamer(tmp_path, 137), tmp_path / "staging")


def test_a_streamer_killed_by_a_signal_fails_the_run(tmp_path):
    """subprocess reports a signal as a negative code; still a failure."""
    script = tmp_path / "killed.py"
    script.write_text(
        "import os, signal, sys\n"
        "sys.stdout.buffer.write(bytes([5]) + (0).to_bytes(4, 'little'))\n"
        "sys.stdout.buffer.flush()\n"
        "os.kill(os.getpid(), signal.SIGKILL)\n"
    )
    with pytest.raises(RuntimeError, match="MEF streamer failed"):
        stage_from_stream(["python3", str(script)], tmp_path / "staging")


def test_a_frame_loop_failure_is_not_masked_by_the_exit_check(tmp_path, monkeypatch):
    """The original exception names the real cause; this must not replace it.

    Raising from the finally would swallow it, which is the whole reason the
    check sits after the try/finally rather than inside it.
    """
    import processor.mef_streamer as mef_streamer

    def exploding_frames(_stream, timeout_sec: int = 300):
        raise TimeoutError("No data received for 300s")
        yield  # pragma: no cover - generator marker

    monkeypatch.setattr(mef_streamer, "_iter_frames", exploding_frames)

    with pytest.raises(TimeoutError, match="No data received"):
        stage_from_stream(_fake_streamer(tmp_path, 1), tmp_path / "staging")


def test_wait_returns_none_when_the_process_has_to_be_terminated(tmp_path):
    from processor.mef_streamer import _wait_for_process

    proc = subprocess.Popen(
        ["python3", "-c", "import time; time.sleep(30)"],
        stdout=subprocess.PIPE,
    )
    try:
        assert _wait_for_process(proc, timeout=1) is None
    finally:
        proc.kill()
        proc.wait()


def test_wait_returns_the_exit_code(tmp_path):
    from processor.mef_streamer import _wait_for_process

    proc = subprocess.Popen(["python3", "-c", "raise SystemExit(3)"])
    assert _wait_for_process(proc, timeout=10) == 3

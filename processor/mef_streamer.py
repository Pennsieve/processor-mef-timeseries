"""
Stream MEF data from Java subprocess and stage as binary/JSON files.

The mefstreamer.jar emits a binary protocol:
  - Frame header: 1 byte type + 4 bytes little-endian length
  - Frame types: CHANNEL_META (1), SEGMENT_START (2), SAMPLES_INT32 (3),
                 SEGMENT_END (4), END (5)

Output per channel:
  - <channel>_seg000.bin, _seg001.bin, ... : int32 little-endian samples
  - <channel>.json : metadata with segment boundaries
"""
import json
import logging
import re
import select
import struct
import subprocess
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Frame types
CHANNEL_META = 1
SEGMENT_START = 2
SAMPLES_INT32 = 3
SEGMENT_END = 4
END = 5

_HEADER_SIZE = 5
_LOG_INTERVAL_BYTES = 64 * 1024 * 1024


def _sanitize_filename(name: str) -> str:
    sanitized = re.sub(r"[^\w\-.]+", "_", name).strip("_")
    return sanitized or "channel"


def _iter_frames(stream, timeout_sec: int = 300):
    """Yield (frame_type, payload) tuples from the Java process stdout."""
    fd = stream.fileno()
    while True:
        readable, _, _ = select.select([fd], [], [], timeout_sec)
        if not readable:
            raise TimeoutError(f"No data received for {timeout_sec}s")

        header = stream.read(_HEADER_SIZE)
        if len(header) < _HEADER_SIZE:
            return

        frame_type = header[0]
        (length,) = struct.unpack("<I", header[1:5])
        payload = stream.read(length)

        if len(payload) < length:
            return
        yield frame_type, payload


def stage_from_stream(java_cmd: list[str], output_dir: Path) -> None:
    """
    Run MEF streamer and write staged channel files.

    Args:
        java_cmd: Command to launch mefstreamer.jar
        output_dir: Directory for staged .bin and .json files
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    log.info("Staging to %s", output_dir)

    proc = subprocess.Popen(
        java_cmd, stdout=subprocess.PIPE, stderr=None, bufsize=1024 * 1024
    )
    log.info("Started MEF streamer (PID %d): %s", proc.pid, " ".join(java_cmd))

    state = _ChannelState(output_dir)

    try:
        for frame_type, payload in _iter_frames(proc.stdout):
            if frame_type == CHANNEL_META:
                state.flush()
                meta = json.loads(payload.decode("utf-8"))
                state.begin_channel(meta)

            elif frame_type == SEGMENT_START:
                start_us, rate_hz = struct.unpack("<qd", payload)
                state.begin_segment(start_us, rate_hz)

            elif frame_type == SAMPLES_INT32:
                state.write_samples(payload)

            elif frame_type == SEGMENT_END:
                end_us, n_samples = struct.unpack("<qq", payload)
                state.end_segment(end_us, n_samples)

            elif frame_type == END:
                log.info("Stream complete")
                break
    finally:
        state.flush()
        _wait_for_process(proc)


def _wait_for_process(proc: subprocess.Popen, timeout: int = 300) -> None:
    try:
        rc = proc.wait(timeout=timeout)
        log.info("MEF streamer exited (code %d)", rc)
    except subprocess.TimeoutExpired:
        log.warning("Terminating hung process")
        proc.terminate()


class _ChannelState:
    """Accumulates state while streaming a single channel."""

    def __init__(self, output_dir: Path):
        self._output_dir = output_dir
        self._reset()

    def _reset(self):
        self._meta: dict[str, Any] = {}
        self._name = ""
        self._base_filename = ""
        self._rate_hz = 0.0
        self._voltage_conversion_factor = 1.0
        self._start_us: int | None = None
        self._end_us = -(2**63)
        self._segment_idx = -1
        self._segment_file = None
        self._segment_samples = 0
        self._segments: list[dict] = []
        self._bytes_written = 0
        self._last_log_bytes = 0
        self._t0 = 0.0

    def begin_channel(self, meta: dict):
        self._meta = meta
        self._name = meta.get("name", "channel")
        self._base_filename = _sanitize_filename(self._name)
        self._rate_hz = float(meta.get("rate_hz", 0.0))

        # Microvolts per A/D count, from the MEF header. Samples are raw counts
        # and mean nothing physically until scaled by this. A jar predating the
        # field omits it; 1.0 keeps the counts unscaled rather than inventing a
        # factor, and the warning makes the situation visible.
        if "voltage_conversion_factor" in meta:
            self._voltage_conversion_factor = float(meta["voltage_conversion_factor"])
        else:
            self._voltage_conversion_factor = 1.0
            log.warning(
                "Channel %s: no voltage_conversion_factor in CHANNEL_META; "
                "assuming 1.0 uV/count (mefstreamer.jar may be out of date)",
                self._name,
            )

        log.info(
            "Channel: %s (%.2f Hz, %g uV/count)",
            self._name, self._rate_hz, self._voltage_conversion_factor,
        )

    def begin_segment(self, start_us: int, rate_hz: float):
        self._rate_hz = rate_hz
        if self._start_us is None:
            self._start_us = start_us

        self._segment_idx += 1
        filename = f"{self._base_filename}_seg{self._segment_idx:03d}.bin"
        self._segment_file = open(
            self._output_dir / filename, "wb", buffering=8 * 1024 * 1024
        )
        self._segment_samples = 0
        self._bytes_written = 0
        self._last_log_bytes = 0
        self._t0 = time.monotonic()

        self._segments.append({
            "index": self._segment_idx,
            "start_us": start_us,
            "end_us": start_us,
            "n_samples": 0,
            "data_path": filename,
        })

    def write_samples(self, data: bytes):
        if self._segment_file is None:
            raise RuntimeError("Received samples without active segment")

        self._segment_file.write(data)
        self._segment_samples += len(data) // 4
        self._bytes_written += len(data)

        if self._bytes_written - self._last_log_bytes >= _LOG_INTERVAL_BYTES:
            elapsed = max(time.monotonic() - self._t0, 1e-9)
            mb = self._bytes_written / (1024 * 1024)
            log.info("  Written %.1f MB (%.1f MB/s)", mb, mb / elapsed)
            self._last_log_bytes = self._bytes_written

    def end_segment(self, end_us: int, expected_samples: int):
        if self._segment_file is None:
            return

        self._segment_file.close()
        self._segment_file = None

        if expected_samples != self._segment_samples:
            log.warning(
                "Sample count mismatch: expected %d, got %d",
                expected_samples,
                self._segment_samples,
            )

        self._segments[-1]["end_us"] = end_us
        self._segments[-1]["n_samples"] = self._segment_samples
        self._end_us = max(self._end_us, end_us)

    def flush(self):
        if self._segment_file is not None:
            self._segment_file.close()
            self._segment_file = None

        if not self._name:
            self._reset()
            return

        manifest = {
            "name": self._name,
            "type": self._meta.get("type", "Unknown"),
            "description": self._meta.get("description", ""),
            "unit": "counts",
            "voltage_conversion_factor": self._voltage_conversion_factor,
            "rate_hz": self._rate_hz,
            "low_cut_hz": self._meta.get("low_cut_hz", -1.0),
            "high_cut_hz": self._meta.get("high_cut_hz", -1.0),
            "absolute_start_us": self._start_us or 0,
            "absolute_end_us": self._end_us,
            "segments": self._segments,
        }

        json_path = self._output_dir / f"{self._base_filename}.json"
        json_path.write_text(json.dumps(manifest, indent=2))
        log.info("Wrote %s (%d segments)", json_path.name, len(self._segments))

        self._reset()

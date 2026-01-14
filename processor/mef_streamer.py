# processor/mef_streamer.py
"""
MEF streaming from Java subprocess.
Reads binary frames from mefstreamer.jar and stages channel data as .bin/.json files.
"""
import json
import logging
import re
import select
import struct
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List

log = logging.getLogger(__name__)

# Frame types from MEFStreamer Java app
CHANNEL_META = 1
SEGMENT_START = 2
SAMPLES_INT32 = 3
SEGMENT_END = 4
END = 5

HEADER_SIZE = 5  # 1 byte type + 4 bytes length


def _safe_name(name: str) -> str:
    """Sanitize channel name for use as filename."""
    base = re.sub(r"[^\w\-.]+", "_", name).strip("_")
    return base or "channel"


def _read_frames(stream, timeout: int = 30):
    """
    Generator yielding (frame_type, payload) from Java byte stream.

    Frame structure: 1 byte type, 4 bytes length (little-endian), then payload.
    """
    fd = stream.fileno()
    while True:
        readable, _, _ = select.select([fd], [], [], timeout)
        if not readable:
            raise TimeoutError(f"No data from Java for {timeout}s")

        header = stream.read(HEADER_SIZE)
        if not header or len(header) < HEADER_SIZE:
            return

        frame_type = header[0]
        (length,) = struct.unpack("<I", header[1:5])
        payload = stream.read(length)

        if len(payload) < length:
            return
        yield frame_type, payload


def stage_from_stream(java_cmd: List[str], staged_dir: Path) -> None:
    """
    Launch Java MEF streamer and write staged files.

    Creates for each channel:
      - <name>_seg000.bin, <name>_seg001.bin, ... (int32 LE sample data)
      - <name>.json (channel metadata with segment info)
    """
    staged_dir.mkdir(parents=True, exist_ok=True)
    log.info("Staging MEF data -> %s", staged_dir)
    log.info("Starting: %s", " ".join(java_cmd))

    proc = subprocess.Popen(
        java_cmd,
        stdout=subprocess.PIPE,
        stderr=None,
        bufsize=1024 * 1024,
    )
    log.info("Java PID %s", proc.pid)
    assert proc.stdout is not None

    # Channel state
    ch_meta: Dict[str, Any] = {}
    ch_name = ""
    ch_base = ""
    ch_rate_hz = 0.0
    observed_start_us: int | None = None
    observed_end_us = -(2**63)
    seg_index = -1
    seg_file = None
    seg_samples = 0
    segments: List[Dict[str, Any]] = []

    # Progress tracking
    bytes_written = 0
    last_log_bytes = 0
    t0 = time.monotonic()
    LOG_INTERVAL = 64 * 1024 * 1024  # 64MB

    def close_segment(end_us: int | None = None, expected_samples: int | None = None):
        nonlocal seg_file, seg_samples, observed_end_us, bytes_written, last_log_bytes
        if seg_file is None:
            return

        seg_file.close()
        seg_file = None

        if expected_samples is not None and expected_samples != seg_samples:
            log.warning("Sample mismatch: expected=%d actual=%d", expected_samples, seg_samples)

        segments[-1]["n_samples"] = seg_samples
        if end_us is not None:
            segments[-1]["end_us"] = int(end_us)
            observed_end_us = max(observed_end_us, int(end_us))

        seg_samples = 0
        bytes_written = 0
        last_log_bytes = 0

    def flush_channel():
        nonlocal ch_meta, ch_name, ch_base, ch_rate_hz, segments
        nonlocal observed_start_us, observed_end_us, seg_index

        if not ch_name:
            return
        if seg_file is not None:
            close_segment()

        data = {
            "name": ch_name,
            "type": ch_meta.get("type", "Unknown"),
            "description": ch_meta.get("description", "Unknown signal type"),
            "unit": "counts",
            "low_cut_hz": ch_meta.get("low_cut_hz", -1.0),
            "high_cut_hz": ch_meta.get("high_cut_hz", -1.0),
            "rate_hz": ch_rate_hz,
            "absolute_start_us": int(observed_start_us) if observed_start_us else 0,
            "absolute_end_us": int(observed_end_us),
            "segments": segments,
        }
        json_path = staged_dir / f"{ch_base}.json"
        json_path.write_text(json.dumps(data, indent=2))
        log.info("Wrote %s (%d segments)", json_path.name, len(segments))

        # Reset
        ch_meta = {}
        ch_name = ""
        ch_base = ""
        ch_rate_hz = 0.0
        observed_start_us = None
        observed_end_us = -(2**63)
        seg_index = -1
        segments = []

    try:
        for ftype, payload in _read_frames(proc.stdout, timeout=300):
            if ftype == CHANNEL_META:
                flush_channel()
                meta = json.loads(payload.decode("utf-8"))
                ch_meta = meta
                ch_name = meta.get("name", "channel")
                ch_base = _safe_name(ch_name)
                ch_rate_hz = float(meta.get("rate_hz", 0.0))
                log.info("Channel: %s @ %.2f Hz", ch_name, ch_rate_hz)

            elif ftype == SEGMENT_START:
                start_us, rate_hz = struct.unpack("<qd", payload)
                ch_rate_hz = float(rate_hz)
                if observed_start_us is None:
                    observed_start_us = int(start_us)

                seg_index += 1
                seg_fname = f"{ch_base}_seg{seg_index:03d}.bin"
                seg_file = open(staged_dir / seg_fname, "wb", buffering=8 * 1024 * 1024)
                seg_samples = 0
                t0 = time.monotonic()

                segments.append({
                    "index": seg_index,
                    "start_us": int(start_us),
                    "end_us": int(start_us),
                    "n_samples": 0,
                    "data_path": seg_fname,
                })
                log.info("Segment %d started", seg_index)

            elif ftype == SAMPLES_INT32:
                if seg_file is None:
                    raise RuntimeError("SAMPLES_INT32 without open segment")
                seg_file.write(payload)
                seg_samples += len(payload) // 4
                bytes_written += len(payload)

                if bytes_written - last_log_bytes >= LOG_INTERVAL:
                    mb = bytes_written / (1024 * 1024)
                    rate = mb / max(time.monotonic() - t0, 1e-6)
                    log.info("  %.1f MB (%.1f MB/s)", mb, rate)
                    last_log_bytes = bytes_written

            elif ftype == SEGMENT_END:
                end_us, n_samples = struct.unpack("<qq", payload)
                segments[-1]["end_us"] = int(end_us)
                observed_end_us = max(observed_end_us, int(end_us))
                close_segment(end_us=end_us, expected_samples=int(n_samples))
                log.info("Segment %d complete", seg_index)

            elif ftype == END:
                log.info("Stream complete")
                break

    finally:
        flush_channel()
        try:
            rc = proc.wait(timeout=300)
            log.info("Java exited with code %s", rc)
        except Exception:
            log.warning("Terminating Java process")
            proc.terminate()

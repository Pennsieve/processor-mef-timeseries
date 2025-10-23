import json
import logging
import os
import re
import select
import struct
import subprocess
import sys
import requests
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import numpy as np

from config import Config
from importer import import_timeseries
from writer import TimeSeriesChunkWriter
from processor.single_channel_reader import SingleChannelReader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("processor")

# MEFStreamer will output these frame types
CHANNEL_META, SEGMENT_START, SAMPLES_INT32, SEGMENT_END, END = 1, 2, 3, 4, 5

def _iter_channel_jsons(staged_dir: Path) -> list[Path]:
    """
    Return only JSON files that look like channel manifests.
    A valid channel JSON must:
      - Be a dict
      - Contain a "segments" key that is a list
      - Contain at least one segment whose "data_path" ends with ".bin"
    """
    return_payload: list[Path] = []

    for json_file in sorted(staged_dir.glob("*.json")):
        try:
            with json_file.open() as f:
                payload = json.load(f)

            if not isinstance(payload, dict):
                log.debug("Skipping JSON (not a dict): %s", json_file.name)
                continue

            segments = payload.get("segments")
            if not isinstance(segments, list):
                log.debug("Skipping JSON (no valid 'segments' list): %s", json_file.name)
                continue

            has_bin_segment = False
            for seg in segments:
                if isinstance(seg, dict) and "data_path" in seg:
                    if str(seg["data_path"]).endswith(".bin"):
                        has_bin_segment = True
                        break

            if has_bin_segment:
                return_payload.append(json_file)
            else:
                log.debug("Skipping JSON without .bin refs: %s", json_file.name)

        except Exception as e:
            log.warning("Skipping JSON %s due to error: %s", json_file.name, e)

    return return_payload


def _safe_channel_name(name: str) -> str:
    base = re.sub(r"[^\w\-.]+", "_", name).strip("_")
    return base or "channel"


def _read_frames(stream, timeout: int = 30):
    """
    Generator that yields (frame_type, payload) tuples from java byte stream.

    Each frame has the following structure:
      - Header: 1 byte for frame type, 4 bytes for payload length (little-endian uint32)
      - Payload: exactly `length` bytes following the header
    Yields:
        tuple[int, bytes]: The frame type and the raw payload bytes.
    """
    file_descriptor = stream.fileno()
    while True:
        is_readable, _, _ = select.select([file_descriptor], [], [], timeout)
        if not is_readable:
            raise TimeoutError(f"No data from Java for {timeout}s")

        header_bytes = stream.read(config.HEADER_SIZE)
        if not header_bytes or len(header_bytes) < config.HEADER_SIZE:
            return

        frame_type = header_bytes[0]
        (length,) = struct.unpack("<I", header_bytes[1:5]) # <I is little-endian uint32
        payload = stream.read(length)

        if len(payload) < length:
            return
        yield frame_type, payload


def stage_from_stream(java_cmd: List[str], staged_dir: Path) -> List[Path]:
    """
    Launch Java app and stage frames into:
      - <name>_seg%03d.bin (int32 LE counts)
      - <name>.json
    Returns list of JSON paths.
    """
    staged_dir.mkdir(parents=True, exist_ok=True)
    log.info("Staging from Java stream -> %s", staged_dir)

    # Inherit stderr so Java logs appear in container logs and won't block.
    log.info("Starting Java process: %s", " ".join(java_cmd))
    proc = subprocess.Popen(
        java_cmd,
        stdout=subprocess.PIPE,
        stderr=None,
        bufsize=0,
    )
    log.info("Java process started with PID %s", proc.pid)
    assert proc.stdout is not None

    json_paths: List[Path] = []

    # Per-channel state
    ch_meta: Dict[str, Any] = {}
    ch_name: str = ""
    ch_base: str = ""
    ch_rate_hz: float = 0.0
    observed_start_us: int | None = None
    observed_end_us: int = -2**63
    seg_index: int = -1
    seg_file = None  # type: ignore
    seg_samples_written: int = 0
    segments: List[Dict[str, Any]] = []

    bytes_this_seg = 0
    last_log_bytes = 0
    t0 = time.monotonic()
    BYTES_LOG_CHUNK = 64 * 1024 * 1024  # 64MB

    log.info("Starting to read frames from Java process")

    def close_segment(end_us: int | None = None, n_samples_from_frame: int | None = None):
        nonlocal seg_file, seg_samples_written, segments, observed_end_us, bytes_this_seg, last_log_bytes
        log.info("Closing segment: %s", (getattr(seg_file, "name", None) or "None"))
        if seg_file is None:
            return
        seg_file.flush()
        seg_file.close()
        seg_file = None

        n_samples = seg_samples_written
        if n_samples_from_frame is not None and n_samples_from_frame != n_samples:
            log.warning("Segment sample mismatch: frame=%d written=%d", n_samples_from_frame, n_samples)

        seg = segments[-1]
        seg["n_samples"] = n_samples
        if end_us is not None:
            seg["end_us"] = int(end_us)
            observed_end_us = max(observed_end_us, int(end_us))

        seg_samples_written = 0
        bytes_this_seg = 0
        last_log_bytes = 0

    def flush_channel():
        nonlocal ch_meta, ch_name, ch_base, ch_rate_hz, segments, observed_start_us, observed_end_us, seg_index
        log.info("Flushing channel: %s", ch_name or "(none)")
        if not ch_name:
            log.warning("No channel name set; skipping flush")
            return
        if seg_file is not None:
            log.warning("Flushing channel with open segment; closing it")
            close_segment()

        data = {
            "name": ch_name,
            "type": ch_meta.get("type", "Unknown"),
            "description": ch_meta.get("description", "Unknown signal type"),
            "unit": "counts",
            "low_cut_hz": ch_meta.get("low_cut_hz", -1.0),
            "high_cut_hz": ch_meta.get("high_cut_hz", -1.0),
            "rate_hz": ch_rate_hz,
            "absolute_start_us": int(observed_start_us) if observed_start_us is not None else 0,
            "absolute_end_us": int(observed_end_us),
            "segments": segments,
        }
        json_path = staged_dir / f"{ch_base}.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        json_paths.append(json_path)
        log.info("Wrote channel JSON: %s (segments=%d)", json_path, len(segments))

        # reset
        ch_meta = {}
        ch_name = ""
        ch_base = ""
        ch_rate_hz = 0.0
        observed_start_us = None
        observed_end_us = -2**63
        seg_index = -1
        segments = []

    # Frame loop
    try:
        for ftype, payload in _read_frames(proc.stdout, timeout=300):
            if ftype == CHANNEL_META:
                # finalize previous channel if any
                flush_channel()

                meta = json.loads(payload.decode("utf-8"))
                ch_meta = meta
                ch_name = meta.get("name", "channel")
                ch_base = _safe_channel_name(ch_name)
                ch_rate_hz = float(meta.get("rate_hz", 0.0))
                observed_start_us = None
                observed_end_us = -2**63
                segments = []
                seg_index = -1
                log.info("META: %s @ %.6f Hz", ch_name, ch_rate_hz)

            elif ftype == SEGMENT_START:
                start_us, rate_hz = struct.unpack("<qd", payload)
                ch_rate_hz = float(rate_hz)  # prefer segment rate
                if observed_start_us is None:
                    observed_start_us = int(start_us)

                seg_index += 1
                seg_fname = f"{ch_base}_seg{seg_index:03d}.bin"
                seg_path = staged_dir / seg_fname
                seg_file = open(seg_path, "wb")
                seg_samples_written = 0
                bytes_this_seg = 0
                last_log_bytes = 0
                t0 = time.monotonic()

                segments.append({
                    "index": seg_index,
                    "start_us": int(start_us),
                    "end_us": int(start_us),  # temp
                    "n_samples": 0,           # temp
                    "data_path": seg_fname,
                })
                log.info("SEGMENT_START: %s seg%03d start_us=%d -> %s",
                         ch_name, seg_index, int(start_us), seg_path)

            elif ftype == SAMPLES_INT32:
                if seg_file is None:
                    raise RuntimeError("Received SAMPLES_INT32 without an open segment")
                seg_file.write(payload)
                seg_samples_written += len(payload) // 4
                bytes_this_seg += len(payload)

                if bytes_this_seg - last_log_bytes >= BYTES_LOG_CHUNK:
                    dt = time.monotonic() - t0
                    mb = bytes_this_seg / (1024 * 1024)
                    rate = mb / max(dt, 1e-6)
                    log.info("Streaming %s seg%03d: wrote %.1f MB (%.1f MB/s)",
                             ch_name, seg_index, mb, rate)
                    last_log_bytes = bytes_this_seg

            elif ftype == SEGMENT_END:
                if seg_file is None:
                    log.warning("SEGMENT_END received but no open segment")
                    continue
                end_us, n_samples = struct.unpack("<qq", payload)
                # update JSON segment fields
                segments[-1]["end_us"] = int(end_us)
                observed_end_us = max(observed_end_us, int(end_us))
                log.info("SEGMENT_END: %s seg%03d end_us=%d n=%d",
                         ch_name, seg_index, int(end_us), int(n_samples))
                close_segment(end_us=end_us, n_samples_from_frame=int(n_samples))

            elif ftype == END:
                log.info("END frame received")
                break

            else:
                log.warning("Unknown frame type: %d (len=%d)", ftype, len(payload))

    finally:
        # finalize last channel if not already flushed
        flush_channel()
        # if Java died early, log its return code
        try:
            rc = proc.wait(timeout=300)
            log.info("Java process exited with code %s", rc)
        except Exception:
            log.warning("Java process still running; sending SIGTERM")
            proc.terminate()

    return json_paths


def get_start_time(json_path: Path) -> int:
    with json_path.open() as f:
        m = json.load(f)
    return min(int(s["start_us"]) for s in m["segments"]) if m["segments"] else 0

def getIntegrationId():
    integration_id = os.getenv("INTEGRATION_ID", None)
    if not integration_id:
        raise RuntimeError("INTEGRATION_ID environment variable is not set")
    return integration_id

def get_integration(api_host: str, integration_id: str, session_token: str) -> bytes:
    """
    Fetch an integration from the API and return its raw response body (bytes).
    Raises an exception if the request fails.
    """
    url = f"{api_host}/integrations/{integration_id}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {session_token}"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()  
    return response.json()


def update_package_properties(api_host: str, node_id: str, api_key: str) -> int:
    """
    Updates a package's properties on the Pennsieve API.

    Args:
        api_host (str): The API host (e.g. "api.pennsieve.io")
        node_id (str): The package (node) ID
        api_key (str): The user's API key for authentication

    Returns:
        int: The HTTP status code from the response
    """
    url = f"{api_host}/packages/{node_id}?updateStorage=true&api_key={api_key}"

    payload = {
        "properties": [
            {
                "key": "subtype",
                "value": "Pennsieve Timeseries",
                "dataType": "string",
                "category": "Viewer",
                "fixed": False,
                "hidden": False
            },
            {
                "key": "icon",
                "value": "Timeseries",
                "dataType": "string",
                "category": "Pennsieve",
                "fixed": False,
                "hidden": False
            }
        ]
    }

    headers = {
        "accept": "*/*",
        "content-type": "application/json"
    }

    response = requests.put(url, json=payload, headers=headers)
    return response.status_code



if __name__ == "__main__":
    config = Config()

    BYTES_PER_MB = 2**20
    BYTES_PER_SAMPLE = 8  # float64 for writer output
    chunk_size_samples = int(getattr(config, "CHUNK_SIZE_MB", 8) * BYTES_PER_MB / BYTES_PER_SAMPLE)

    INPUT_DIR = Path(getattr(config, "INPUT_DIR", "/data/input")).resolve()
    OUTPUT_DIR = Path(getattr(config, "OUTPUT_DIR", "/data/output")).resolve()
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Listing input dir before running Java:")
    log.info(f"INPUT_DIR={INPUT_DIR}, OUTPUT_DIR={OUTPUT_DIR}")
    subprocess.run(["ls", "-lh", INPUT_DIR])

    log.info(getattr(config, "STREAM_FROM_JAR", True))
    if getattr(config, "STREAM_FROM_JAR", True):
        java_cmd = getattr(config, "JAVA_CMD", None)
        if not java_cmd:
            raise RuntimeError("STREAM_FROM_JAR=True but JAVA_CMD not set in Config.")
        # If JAVA_CMD is a string, split it
        if isinstance(java_cmd, str):
            import shlex
            java_cmd = shlex.split(java_cmd)

        staged = stage_from_stream(java_cmd, INPUT_DIR)
        log.info("Staged %d channels", len(staged))

    # Discover staged JSONs (from streaming or pre-staged)
    chan_jsons = _iter_channel_jsons(INPUT_DIR)
    if not chan_jsons:
        raise RuntimeError(f"No channel .json files in {INPUT_DIR}. "
                           f"Either enable STREAM_FROM_JAR with JAVA_CMD, or pre-stage your channels.")

    session_start_us = min((get_start_time(p) for p in chan_jsons if p.exists()), default=0)
    session_start_time = datetime.fromtimestamp(session_start_us / 1e6, tz=timezone.utc) if session_start_us else datetime.now(timezone.utc)
    log.info("Session start (UTC): %s", session_start_time.isoformat())

    # Use staged_dtype='auto' so counts (<i4) vs floats (<f8) are inferred from JSON "unit"
    for index, json_path in enumerate(chan_jsons):
        reader = SingleChannelReader(str(json_path), staged_dtype="auto", global_index=index)
        writer = TimeSeriesChunkWriter(session_start_time, str(OUTPUT_DIR), chunk_size_samples)
        log.info("Writing channel index %05d (%s)", index, json_path.name)
        writer.write_electrical_series(reader)

    if getattr(config, "IMPORTER_ENABLED", False):
        import_timeseries(
            config.API_HOST,
            config.API_HOST2,
            config.API_KEY,
            config.API_SECRET,
            config.WORKFLOW_INSTANCE_ID,
            str(OUTPUT_DIR),
        )

    # Set attributes on collection
    session_token = os.getenv("SESSION_TOKEN", None)
    integration_id = config.WORKFLOW_INSTANCE_ID
    integration_payload = get_integration(config.API_HOST, integration_id, session_token)
    package_id = integration_payload.get("packageId", None)

    if package_id:
        status_code = update_package_properties(config.API_HOST, package_id, config.API_KEY)
        if status_code == 200:
            log.info(f"Successfully updated package {package_id} properties")
        else:
            log.error(f"Failed to update package {package_id} properties, status code: {status_code}")
    else:
        log.error("No packageId found in integration payload; cannot update package properties")


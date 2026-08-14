"""
Memory-efficient reader for staged MEF channel data.

Loads only JSON metadata on init; binary sample data is read on-demand
via memory-mapped files during NWB construction.
"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

log = logging.getLogger(__name__)


@dataclass
class Segment:
    start_us: int
    end_us: int
    n_samples: int
    path: str


@dataclass
class Channel:
    name: str
    rate_hz: float
    unit: str
    segments: list[Segment]
    total_samples: int
    # Microvolts per A/D count, from the MEF header. Samples are stored as raw
    # counts; this is what turns them into a physical measurement.
    voltage_conversion_factor: float = 1.0


class MultiChannelReader:
    """
    Provides unified access to multi-channel MEF data for NWB conversion.

    All channels are assumed to be temporally aligned with matching sample rates.
    """

    @classmethod
    def from_staged_dir(cls, directory: Path) -> "MultiChannelReader":
        """Load all valid channel manifests from a staging directory."""
        paths = []
        for path in sorted(directory.glob("*.json")):
            try:
                with path.open() as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    continue
                segments = data.get("segments", [])
                if any(str(s.get("data_path", "")).endswith(".bin") for s in segments):
                    paths.append(path)
            except Exception as e:
                log.warning("Skipping %s: %s", path.name, e)

        if not paths:
            raise ValueError(f"No valid channel manifests in {directory}")
        return cls(paths)

    def __init__(self, manifest_paths: list[Path]):
        if not manifest_paths:
            raise ValueError("No manifest paths provided")

        self._channels = [
            self._parse_manifest(p)
            for p in sorted(manifest_paths, key=lambda p: p.stem)
        ]
        log.info("Loaded %d channels", len(self._channels))

        self._validate_channels()
        self._session_start_us, self._session_start_time = self._compute_session_start()
        self._has_gaps = self._check_for_gaps()

    def _parse_manifest(self, path: Path) -> Channel:
        with path.open() as f:
            data = json.load(f)

        segments = []
        total = 0
        for s in sorted(data["segments"], key=lambda x: x["start_us"]):
            seg = Segment(
                start_us=int(s["start_us"]),
                end_us=int(s["end_us"]),
                n_samples=int(s["n_samples"]),
                path=str((path.parent / s["data_path"]).resolve()),
            )
            segments.append(seg)
            total += seg.n_samples

        vcf = data.get("voltage_conversion_factor")
        if vcf is None:
            vcf = 1.0
            log.warning(
                "Channel %s: manifest has no voltage_conversion_factor; "
                "assuming 1.0 uV/count (re-stage with a current mefstreamer.jar)",
                data["name"],
            )

        return Channel(
            name=data["name"],
            rate_hz=float(data["rate_hz"]),
            unit=data.get("unit", "counts"),
            segments=segments,
            total_samples=total,
            voltage_conversion_factor=float(vcf),
        )

    def _validate_channels(self):
        ref = self._channels[0]
        for ch in self._channels[1:]:
            if abs(ch.total_samples - ref.total_samples) > 100:
                log.warning(
                    "Sample count mismatch: %s has %d (expected %d)",
                    ch.name, ch.total_samples, ref.total_samples,
                )
            if ref.rate_hz and abs(1 - ch.rate_hz / ref.rate_hz) > 0.02:
                log.warning(
                    "Rate mismatch: %s has %.2f Hz (expected %.2f Hz)",
                    ch.name, ch.rate_hz, ref.rate_hz,
                )

    def _compute_session_start(self) -> tuple[int, datetime]:
        start_us = min(ch.segments[0].start_us for ch in self._channels)
        start_time = datetime.fromtimestamp(start_us / 1e6, tz=timezone.utc)
        log.info("Session start: %s", start_time.isoformat())
        return start_us, start_time

    def _check_for_gaps(self) -> bool:
        ref = self._channels[0]
        if len(ref.segments) < 2:
            return False

        threshold_us = 2 * (1_000_000 / ref.rate_hz)
        for i in range(1, len(ref.segments)):
            gap = ref.segments[i].start_us - ref.segments[i - 1].end_us
            if gap > threshold_us:
                log.info("Gap detected: %.1f ms between segments %d-%d",
                         gap / 1000, i - 1, i)
                return True
        return False

    @property
    def num_channels(self) -> int:
        return len(self._channels)

    @property
    def num_samples(self) -> int:
        return min(ch.total_samples for ch in self._channels)

    @property
    def sampling_rate(self) -> float:
        return self._channels[0].rate_hz

    @property
    def session_start_time(self) -> datetime:
        return self._session_start_time

    @property
    def channel_names(self) -> list[str]:
        return [ch.name for ch in self._channels]

    @property
    def voltage_conversion_factors(self) -> list[float]:
        """Microvolts per count, in the same order as channel_names."""
        return [ch.voltage_conversion_factor for ch in self._channels]

    def has_gaps(self) -> bool:
        return self._has_gaps

    def get_timestamps_seconds(self) -> np.ndarray:
        """Compute timestamps relative to session start for all samples."""
        ref = self._channels[0]
        period_us = 1_000_000 / ref.rate_hz

        arrays = []
        for seg in ref.segments:
            if seg.n_samples > 0:
                t = seg.start_us + np.arange(seg.n_samples, dtype=np.float64) * period_us
                arrays.append(t)

        if not arrays:
            return np.array([], dtype=np.float64)

        timestamps_us = np.concatenate(arrays)[: self.num_samples]
        return (timestamps_us - self._session_start_us) / 1e6

    def read_all_channels(self, start: int, end: int) -> np.ndarray:
        """Read sample range [start, end) from all channels."""
        data = np.empty((end - start, self.num_channels), dtype=np.float64)
        for i, ch in enumerate(self._channels):
            data[:, i] = self._read_channel(ch, start, end)
        return data

    def _read_channel(self, channel: Channel, start: int, end: int) -> np.ndarray:
        out = np.full(end - start, np.nan, dtype=np.float64)
        dtype = "<i4" if channel.unit.lower() == "counts" else "<f8"

        cumulative = 0
        for seg in channel.segments:
            seg_start = cumulative
            seg_end = cumulative + seg.n_samples
            cumulative = seg_end

            if seg_end <= start or seg_start >= end:
                continue

            read_start = max(start, seg_start)
            read_end = min(end, seg_end)

            local_start = read_start - seg_start
            local_end = read_end - seg_start
            out_start = read_start - start
            out_end = read_end - start

            mmap = np.memmap(seg.path, dtype=dtype, mode="r", shape=(seg.n_samples,))
            out[out_start:out_end] = mmap[local_start:local_end].astype(np.float64)
            del mmap

        return out

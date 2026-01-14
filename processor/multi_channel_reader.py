# processor/multi_channel_reader.py
"""
Multi-channel reader that aggregates staged channels for NWB construction.
Memory-efficient: loads metadata upfront, reads binary data on-demand.
"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import numpy as np

log = logging.getLogger(__name__)


@dataclass
class SegmentInfo:
    """Metadata for a binary segment file."""
    start_us: int
    end_us: int
    n_samples: int
    data_path: str


@dataclass
class ChannelMetadata:
    """Channel metadata loaded from JSON."""
    name: str
    rate_hz: float
    unit: str
    segments: List[SegmentInfo]
    total_samples: int


class MultiChannelReader:
    """
    Aggregates staged channels for NWB construction.

    Memory-efficient: only loads JSON metadata upfront,
    reads binary data in chunks during write phase.
    """

    def __init__(self, channel_json_paths: List[Path]):
        if not channel_json_paths:
            raise ValueError("No channel JSON paths provided")

        self._channels: List[ChannelMetadata] = []
        for json_path in sorted(channel_json_paths, key=lambda p: p.stem):
            self._channels.append(self._load_metadata(json_path))

        log.info("Loaded metadata for %d channels", len(self._channels))

        self._validate_alignment()
        self._compute_session_start()
        self._has_gaps = self._detect_gaps()

    def _load_metadata(self, json_path: Path) -> ChannelMetadata:
        """Load channel metadata from JSON."""
        with json_path.open() as f:
            data = json.load(f)

        segments = []
        total_samples = 0
        for seg in sorted(data["segments"], key=lambda s: int(s["start_us"])):
            seg_info = SegmentInfo(
                start_us=int(seg["start_us"]),
                end_us=int(seg["end_us"]),
                n_samples=int(seg["n_samples"]),
                data_path=str((json_path.parent / seg["data_path"]).resolve()),
            )
            segments.append(seg_info)
            total_samples += seg_info.n_samples

        return ChannelMetadata(
            name=data["name"],
            rate_hz=float(data["rate_hz"]),
            unit=str(data.get("unit", "counts")),
            segments=segments,
            total_samples=total_samples,
        )

    def _validate_alignment(self) -> None:
        """Warn if channels have misaligned sample counts or rates."""
        ref = self._channels[0]
        for ch in self._channels[1:]:
            if abs(ch.total_samples - ref.total_samples) > 100:
                log.warning(
                    "Channel '%s' has %d samples, expected ~%d",
                    ch.name, ch.total_samples, ref.total_samples
                )
            if abs(1 - (ch.rate_hz / ref.rate_hz)) > 0.02:
                log.warning(
                    "Channel '%s' has rate %.2f Hz, expected ~%.2f Hz",
                    ch.name, ch.rate_hz, ref.rate_hz
                )

    def _compute_session_start(self) -> None:
        """Compute session start from earliest segment timestamp."""
        min_start_us = min(
            ch.segments[0].start_us for ch in self._channels if ch.segments
        )
        self._session_start_us = min_start_us
        self._session_start_time = datetime.fromtimestamp(
            min_start_us / 1e6, tz=timezone.utc
        )
        log.info("Session start: %s", self._session_start_time.isoformat())

    def _detect_gaps(self) -> bool:
        """Check if there are discontinuities between segments."""
        ref = self._channels[0]
        if len(ref.segments) <= 1:
            return False

        gap_threshold_us = (1_000_000.0 / ref.rate_hz) * 2  # 2x sample period
        for i in range(1, len(ref.segments)):
            gap = ref.segments[i].start_us - ref.segments[i - 1].end_us
            if gap > gap_threshold_us:
                log.info("Gap detected: %d us between segments %d and %d", gap, i - 1, i)
                return True
        return False

    @property
    def num_channels(self) -> int:
        return len(self._channels)

    @property
    def num_samples(self) -> int:
        """Minimum sample count across all channels."""
        return min(ch.total_samples for ch in self._channels)

    @property
    def sampling_rate(self) -> float:
        return self._channels[0].rate_hz

    @property
    def session_start_time(self) -> datetime:
        return self._session_start_time

    @property
    def channel_names(self) -> List[str]:
        return [ch.name for ch in self._channels]

    def has_gaps(self) -> bool:
        return self._has_gaps

    def get_timestamps_seconds(self) -> np.ndarray:
        """Get timestamps relative to session start, in seconds."""
        ref = self._channels[0]
        period_us = 1_000_000.0 / ref.rate_hz

        timestamps_list = []
        for seg in ref.segments:
            if seg.n_samples <= 0:
                continue
            indices = np.arange(seg.n_samples, dtype=np.float64)
            timestamps_list.append(seg.start_us + indices * period_us)

        if not timestamps_list:
            return np.array([], dtype=np.float64)

        all_ts = np.concatenate(timestamps_list)
        return (all_ts[:self.num_samples] - self._session_start_us) / 1e6

    def read_all_channels(self, start: int, end: int) -> np.ndarray:
        """Read samples [start:end) from all channels. Returns (samples, channels)."""
        result = np.empty((end - start, self.num_channels), dtype=np.float64)
        for ch_idx, ch in enumerate(self._channels):
            result[:, ch_idx] = self._read_range(ch, start, end)
        return result

    def _read_range(self, ch: ChannelMetadata, start: int, end: int) -> np.ndarray:
        """Read sample range from a single channel's segment files."""
        out = np.full(end - start, np.nan, dtype=np.float64)
        dtype = "<i4" if ch.unit.lower() == "counts" else "<f8"

        # Build cumulative sample offsets
        offsets = [0]
        for seg in ch.segments:
            offsets.append(offsets[-1] + seg.n_samples)

        # Read from overlapping segments
        for i, seg in enumerate(ch.segments):
            seg_start, seg_end = offsets[i], offsets[i + 1]
            if seg_end <= start or seg_start >= end:
                continue

            # Compute overlap
            read_start = max(start, seg_start)
            read_end = min(end, seg_end)

            # Map to local segment indices and output indices
            local_start = read_start - seg_start
            local_end = read_end - seg_start
            out_start = read_start - start
            out_end = read_end - start

            mm = np.memmap(seg.data_path, dtype=dtype, mode="r", shape=(seg.n_samples,))
            out[out_start:out_end] = mm[local_start:local_end].astype(np.float64)
            del mm

        return out
